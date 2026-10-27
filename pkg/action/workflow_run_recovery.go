package action

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/go-github/v84/github"
	"github.com/promhippie/github_exporter/pkg/config"
	"github.com/promhippie/github_exporter/pkg/exporter"
	"github.com/promhippie/github_exporter/pkg/store"
	"github.com/ryanuber/go-glob"
)

type workflowRunRecovery struct {
	config config.Config
	db     store.Store
	logger *slog.Logger
	client *github.Client
	health *exporter.WorkflowRunHealth
}

func runWorkflowRunRecovery(ctx context.Context, cfg *config.Config, db store.Store, logger *slog.Logger, client *github.Client, health *exporter.WorkflowRunHealth) error {
	if !cfg.Target.WorkflowRuns.Recovery.Enabled {
		return nil
	}

	interval := cfg.Target.WorkflowRuns.Recovery.Interval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	recovery := workflowRunRecovery{
		config: *cfg,
		db:     db,
		logger: logger.With("component", "workflow_run_recovery"),
		client: client,
		health: health,
	}

	for {
		recovery.runCycle(ctx)
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func (r *workflowRunRecovery) runCycle(ctx context.Context) {
	r.health.Set(exporter.WorkflowRunHealthRecoveryPermissionError, false)
	r.health.Set(exporter.WorkflowRunHealthRecoveryRateLimit, false)
	r.health.Set(exporter.WorkflowRunHealthRecoveryPaginationTrunc, false)
	r.health.Set(exporter.WorkflowRunHealthRecoveryError, false)
	r.health.SetDatabaseError("recovery", false)

	callbackURL := r.config.Target.WorkflowRuns.Recovery.CallbackURL
	if callbackURL == "" {
		r.recordRecoveryError(errors.New("callback URL is required when recovery is enabled"))
		return
	}

	repos, err := r.targetRepositories(ctx)
	if err != nil {
		r.recordRecoveryError(err)
		return
	}
	if len(repos) == 0 {
		r.recordRecoveryError(errors.New("no target repositories are configured for recovery"))
		return
	}

	for _, repo := range repos {
		if ctx.Err() != nil {
			return
		}
		r.recoverRepository(ctx, repo, callbackURL)
	}
}

func (r *workflowRunRecovery) targetRepositories(ctx context.Context) ([]*github.Repository, error) {
	patterns := r.config.Target.Repos
	if len(patterns) == 0 {
		return nil, errors.New("recovery requires explicit repository targets")
	}

	collected := make(map[string]*github.Repository)
	for _, pattern := range patterns {
		parts := strings.Split(pattern, "/")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid recovery repository target %q", pattern)
		}

		owner, name := parts[0], parts[1]
		if !strings.Contains(pattern, "*") {
			requestContext, cancel := r.requestContext(ctx)
			repo, response, err := r.client.Repositories.Get(requestContext, owner, name)
			cancel()
			closeGitHubResponse(response)
			if err != nil {
				r.recordAPIError("fetch target repository", err)
				continue
			}
			collected[repo.GetFullName()] = repo
			continue
		}

		options := &github.SearchOptions{ListOptions: github.ListOptions{PerPage: 100}}
		for {
			requestContext, cancel := r.requestContext(ctx)
			result, response, err := r.client.Search.Repositories(requestContext, "user:"+owner, options)
			cancel()
			if err != nil {
				closeGitHubResponse(response)
				r.recordAPIError("search target repositories", err)
				break
			}
			if result.GetIncompleteResults() {
				r.health.Set(exporter.WorkflowRunHealthRecoveryPaginationTrunc, true)
			}
			for _, repo := range result.Repositories {
				if glob.Glob(pattern, repo.GetFullName()) {
					collected[repo.GetFullName()] = repo
				}
			}
			nextPage := response.NextPage
			closeGitHubResponse(response)
			if nextPage == 0 {
				break
			}
			options.Page = nextPage
		}
	}

	repos := make([]*github.Repository, 0, len(collected))
	for _, repo := range collected {
		repos = append(repos, repo)
	}
	return repos, nil
}

func (r *workflowRunRecovery) recoverRepository(ctx context.Context, repo *github.Repository, callbackURL string) {
	owner, name := repo.GetOwner().GetLogin(), repo.GetName()
	options := &github.ListOptions{PerPage: 100}
	for {
		requestContext, cancel := r.requestContext(ctx)
		hooks, response, err := r.client.Repositories.ListHooks(requestContext, owner, name, options)
		cancel()
		if err != nil {
			closeGitHubResponse(response)
			r.recordAPIError("list repository hooks", err)
			return
		}
		nextPage := response.NextPage
		closeGitHubResponse(response)
		for _, hook := range hooks {
			if !hook.GetActive() || !containsEvent(hook.Events, "workflow_run") || hook.GetConfig() == nil || hook.GetConfig().GetURL() != callbackURL {
				continue
			}
			r.recoverHook(ctx, owner, name, hook.GetID())
		}
		if nextPage == 0 {
			return
		}
		options.Page = nextPage
	}
}

func (r *workflowRunRecovery) recoverHook(ctx context.Context, owner, repo string, hookID int64) {
	checkpoint, err := r.db.GetWorkflowRunRecoveryCheckpoint(owner, repo, hookID)
	if err != nil {
		r.recordDatabaseError("read recovery checkpoint", err)
		return
	}
	if checkpoint.ScanStartedAt.IsZero() {
		checkpoint.ScanStartedAt = time.Now().UTC()
		checkpoint.ScanCutoff = checkpoint.LastCompletedAt
		checkpoint.Cursor = ""
	}

	maxPages := r.config.Target.WorkflowRuns.Recovery.MaxPages
	if maxPages <= 0 {
		maxPages = 10
	}
	pages := 0
	for pages < maxPages {
		if ctx.Err() != nil {
			return
		}
		options := &github.ListCursorOptions{PerPage: 100, Cursor: checkpoint.Cursor}
		requestContext, cancel := r.requestContext(ctx)
		deliveries, response, err := r.client.Repositories.ListHookDeliveries(requestContext, owner, repo, hookID, options)
		cancel()
		if err != nil {
			closeGitHubResponse(response)
			if saveErr := r.db.SaveWorkflowRunRecoveryCheckpoint(checkpoint); saveErr != nil {
				r.recordDatabaseError("save recovery checkpoint", saveErr)
			}
			r.recordAPIError("list hook deliveries", err)
			return
		}
		pages++
		nextCursor := response.Cursor
		closeGitHubResponse(response)

		cutoffReached := false
		for _, delivery := range deliveries {
			deliveredAt := delivery.GetDeliveredAt().Time
			if !checkpoint.ScanCutoff.IsZero() && !deliveredAt.IsZero() && deliveredAt.Before(checkpoint.ScanCutoff) {
				cutoffReached = true
				break
			}
			if delivery.GetEvent() != "workflow_run" || delivery.GetStatus() == "OK" {
				continue
			}
			r.redeliverFailedDelivery(ctx, owner, repo, hookID, delivery)
		}

		if cutoffReached || nextCursor == "" {
			checkpoint.Cursor = ""
			checkpoint.LastCompletedAt = checkpoint.ScanStartedAt
			checkpoint.ScanStartedAt = time.Time{}
			checkpoint.ScanCutoff = time.Time{}
			checkpoint.UpdatedAt = time.Now().UTC()
			if err := r.db.SaveWorkflowRunRecoveryCheckpoint(checkpoint); err != nil {
				r.recordDatabaseError("complete recovery checkpoint", err)
			}
			return
		}

		checkpoint.Cursor = nextCursor
		checkpoint.UpdatedAt = time.Now().UTC()
		if err := r.db.SaveWorkflowRunRecoveryCheckpoint(checkpoint); err != nil {
			r.recordDatabaseError("save recovery checkpoint", err)
			return
		}
	}

	if checkpoint.Cursor != "" {
		r.health.Set(exporter.WorkflowRunHealthRecoveryPaginationTrunc, true)
	}
}

func (r *workflowRunRecovery) redeliverFailedDelivery(ctx context.Context, owner, repo string, hookID int64, delivery *github.HookDelivery) {
	guid := delivery.GetGUID()
	if guid == "" {
		r.recordRecoveryError(errors.New("failed hook delivery has no GUID"))
		return
	}
	reserved, err := r.db.ReserveWorkflowRunRecoveryDelivery(owner, repo, hookID, guid, delivery.GetID(), time.Now().UTC())
	if err != nil {
		r.recordDatabaseError("reserve delivery redelivery", err)
		return
	}
	if !reserved {
		return
	}

	requestContext, cancel := r.requestContext(ctx)
	_, response, redeliveryErr := r.client.Repositories.RedeliverHookDelivery(requestContext, owner, repo, hookID, delivery.GetID())
	cancel()
	closeGitHubResponse(response)
	status := "accepted"
	if redeliveryErr != nil {
		var accepted *github.AcceptedError
		if !errors.As(redeliveryErr, &accepted) {
			status = "failed"
		}
	}
	if err := r.db.MarkWorkflowRunRecoveryDelivery(owner, repo, hookID, guid, delivery.GetID(), time.Now().UTC(), status); err != nil {
		r.recordDatabaseError("record delivery redelivery", err)
		return
	}
	if redeliveryErr != nil {
		var accepted *github.AcceptedError
		if errors.As(redeliveryErr, &accepted) {
			return
		}
		r.recordAPIError("redeliver hook delivery", redeliveryErr)
		return
	}
	r.logger.Info("Redelivered failed workflow_run webhook delivery",
		"owner", owner,
		"repo", repo,
		"hook_id", hookID,
		"delivery_id", delivery.GetID(),
	)
}

func (r *workflowRunRecovery) recordDatabaseError(operation string, err error) {
	r.health.SetDatabaseError("recovery", true)
	r.recordRecoveryError(fmt.Errorf("%s: %w", operation, err))
}

func (r *workflowRunRecovery) recordRecoveryError(err error) {
	r.health.Set(exporter.WorkflowRunHealthRecoveryError, true)
	r.logger.Error("Workflow run delivery recovery failed", "error", err)
}

func (r *workflowRunRecovery) recordAPIError(operation string, err error) {
	var rateLimit *github.RateLimitError
	var abuseLimit *github.AbuseRateLimitError
	if errors.As(err, &rateLimit) || errors.As(err, &abuseLimit) {
		r.health.Set(exporter.WorkflowRunHealthRecoveryRateLimit, true)
		r.logger.Error("GitHub API rate limit blocked workflow run delivery recovery", "operation", operation, "error", err)
		return
	}

	var apiError *github.ErrorResponse
	if errors.As(err, &apiError) && apiError.Response != nil && (apiError.Response.StatusCode == http.StatusForbidden || apiError.Response.StatusCode == http.StatusUnauthorized) {
		r.health.Set(exporter.WorkflowRunHealthRecoveryPermissionError, true)
		r.logger.Error("GitHub API permission blocked workflow run delivery recovery", "operation", operation, "error", err)
		return
	}
	r.recordRecoveryError(fmt.Errorf("%s: %w", operation, err))
}

func containsEvent(events []string, event string) bool {
	for _, candidate := range events {
		if candidate == event {
			return true
		}
	}
	return false
}

func closeGitHubResponse(response *github.Response) {
	if response != nil && response.Body != nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
}

func (r *workflowRunRecovery) requestContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := r.config.Target.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}
