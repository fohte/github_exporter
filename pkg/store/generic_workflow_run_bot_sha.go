package store

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/go-github/v84/github"
	"github.com/jmoiron/sqlx"
)

var workflowRunBotSHAMu sync.Mutex

// storeWorkflowRunBotSHAEvent keeps the earliest run for each repository, branch, and SHA.
func storeWorkflowRunBotSHAEvent(handle *sqlx.DB, event *github.WorkflowRunEvent) error {
	if event == nil || event.GetRepo() == nil || event.GetRepo().GetOwner() == nil {
		return nil
	}

	run := event.GetWorkflowRun()
	if run == nil {
		return nil
	}
	owner := event.GetRepo().GetOwner().GetLogin()
	repo := event.GetRepo().GetName()
	if owner == "" || repo == "" {
		return nil
	}

	workflowRunBotSHAMu.Lock()
	defer workflowRunBotSHAMu.Unlock()

	tx, err := handle.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin workflow run bot SHA transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	repoKey := workflowRunHash(owner, repo)
	var existingRepo int
	err = tx.Get(&existingRepo, tx.Rebind("SELECT 1 FROM workflow_run_bot_sha_repos WHERE repo_key = ?"), repoKey)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.Exec(
			tx.Rebind("INSERT INTO workflow_run_bot_sha_repos (repo_key, owner, repo) VALUES (?, ?, ?)"),
			repoKey,
			owner,
			repo,
		); err != nil {
			return fmt.Errorf("failed to register workflow run bot SHA repository: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to check workflow run bot SHA repository: %w", err)
	}

	createdAt := run.GetCreatedAt().Time
	branch := run.GetHeadBranch()
	sha := run.GetHeadSHA()
	eventName := run.GetEvent()
	if eventName == "schedule" || branch == "" || sha == "" || createdAt.IsZero() {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit workflow run bot SHA repository: %w", err)
		}
		return nil
	}

	actor := run.GetTriggeringActor()
	isBot := actor.GetType() == "Bot" || strings.HasSuffix(actor.GetLogin(), "[bot]")
	defaultBranch := event.GetRepo().GetDefaultBranch()
	shaKey := workflowRunHash(owner, repo, branch, sha)
	candidate := WorkflowRunBotSHA{
		Owner:         owner,
		Repo:          repo,
		Branch:        branch,
		SHA:           sha,
		DefaultBranch: defaultBranch,
		IsBot:         isBot,
		CreatedAtNS:   createdAt.UnixNano(),
	}

	var existing WorkflowRunBotSHA
	err = tx.Get(&existing, tx.Rebind(`SELECT owner, repo, branch, sha, default_branch, is_bot, created_at_ns
		FROM workflow_run_bot_shas WHERE sha_key = ?`), shaKey)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.Exec(tx.Rebind(`INSERT INTO workflow_run_bot_shas (
			sha_key, owner, repo, branch, sha, default_branch, is_bot, created_at_ns
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`),
			shaKey, candidate.Owner, candidate.Repo, candidate.Branch, candidate.SHA,
			candidate.DefaultBranch, candidate.IsBot, candidate.CreatedAtNS)
		if err != nil {
			return fmt.Errorf("failed to insert workflow run bot SHA: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to read workflow run bot SHA: %w", err)
	} else if earlierWorkflowRunBotSHA(candidate, existing) {
		_, err = tx.Exec(tx.Rebind(`UPDATE workflow_run_bot_shas SET
			default_branch = ?, is_bot = ?, created_at_ns = ? WHERE sha_key = ?`),
			candidate.DefaultBranch, candidate.IsBot, candidate.CreatedAtNS, shaKey)
		if err != nil {
			return fmt.Errorf("failed to update earliest workflow run bot SHA: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit workflow run bot SHA: %w", err)
	}
	return nil
}

func earlierWorkflowRunBotSHA(candidate, existing WorkflowRunBotSHA) bool {
	if candidate.CreatedAtNS != existing.CreatedAtNS {
		return candidate.CreatedAtNS < existing.CreatedAtNS
	}
	return !candidate.IsBot && existing.IsBot
}

func getWorkflowRunBotSHASnapshot(handle *sqlx.DB, now time.Time) (*WorkflowRunBotSHASnapshot, error) {
	snapshot := &WorkflowRunBotSHASnapshot{
		Repositories: make([]WorkflowRunBotSHASeries, 0),
		Events:       make([]WorkflowRunBotSHA, 0),
	}
	if err := handle.Select(&snapshot.Repositories, "SELECT owner, repo FROM workflow_run_bot_sha_repos ORDER BY owner, repo"); err != nil {
		return nil, fmt.Errorf("failed to read workflow run bot SHA repositories: %w", err)
	}
	if err := handle.Select(
		&snapshot.Events,
		handle.Rebind(`SELECT owner, repo, branch, sha, default_branch, is_bot, created_at_ns
			FROM workflow_run_bot_shas WHERE created_at_ns > ? AND created_at_ns <= ?`),
		now.Add(-6*time.Hour).UnixNano(),
		now.UnixNano(),
	); err != nil {
		return nil, fmt.Errorf("failed to read recent workflow run bot SHAs: %w", err)
	}
	return snapshot, nil
}
