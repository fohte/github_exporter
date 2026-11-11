package exporter

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/promhippie/github_exporter/pkg/store"
)

type botSHASnapshotStore struct {
	StaticStore
	snapshot *store.WorkflowRunBotSHASnapshot
	err      error
	pruneErr error
}

func (s botSHASnapshotStore) GetWorkflowRunBotSHASnapshot(time.Time) (*store.WorkflowRunBotSHASnapshot, error) {
	return s.snapshot, s.err
}

func (s botSHASnapshotStore) PruneWorkflowRunBotSHAs(time.Duration) error {
	return s.pruneErr
}

func TestWorkflowRunBotSHAMaxCountsDistinctSHAsAndAppliesHumanResets(t *testing.T) {
	now := time.Now().UTC()
	event := func(branch, sha string, age time.Duration, isBot bool) store.WorkflowRunBotSHA {
		return store.WorkflowRunBotSHA{
			Owner:         "example-org",
			Repo:          "sample-repo",
			Branch:        branch,
			SHA:           sha,
			DefaultBranch: "main",
			IsBot:         isBot,
			CreatedAtNS:   now.Add(-age).UnixNano(),
		}
	}
	collector := newWorkflowRunAttemptTestCollector(botSHASnapshotStore{snapshot: &store.WorkflowRunBotSHASnapshot{
		Repositories: []store.WorkflowRunBotSHASeries{{Owner: "example-org", Repo: "sample-repo"}},
		Events: []store.WorkflowRunBotSHA{
			event("topic/loop", "sha-a", 20*time.Minute, true),
			event("topic/loop", "sha-b", 10*time.Minute, true),
		},
	}})
	var logs bytes.Buffer
	collector.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	maxima := collectBotSHAMaxima(t, collector)
	if maxima["30m"] != 2 || maxima["6h"] != 2 {
		t.Fatalf("maxima before reset = %#v, want 2 in both windows", maxima)
	}
	if !strings.Contains(logs.String(), "topic/loop") {
		t.Fatalf("maximum branch was not logged: %s", logs.String())
	}

	snapshot := collector.db.(botSHASnapshotStore).snapshot
	snapshot.Events = append(snapshot.Events, event("topic/loop", "sha-human", 5*time.Minute, false))
	maxima = collectBotSHAMaxima(t, collector)
	if maxima["30m"] != 0 || maxima["6h"] != 0 {
		t.Fatalf("maxima after branch human reset = %#v, want zero", maxima)
	}

	snapshot.Events = append(snapshot.Events, event("topic/loop", "sha-c", 4*time.Minute, true))
	maxima = collectBotSHAMaxima(t, collector)
	if maxima["30m"] != 1 || maxima["6h"] != 1 {
		t.Fatalf("maxima after new bot SHA = %#v, want 1", maxima)
	}

	snapshot.Events = append(snapshot.Events,
		event("topic/other", "sha-other", 3*time.Minute, true),
		event("main", "sha-default-human", 2*time.Minute, false),
	)
	maxima = collectBotSHAMaxima(t, collector)
	if maxima["30m"] != 0 || maxima["6h"] != 0 {
		t.Fatalf("maxima after default branch human reset = %#v, want zero", maxima)
	}
}

func collectBotSHAMaxima(t *testing.T, collector *WorkflowRunCollector) map[string]float64 {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	collector.Collect(ch)
	close(ch)
	maxima := make(map[string]float64)
	for metric := range ch {
		var sample dto.Metric
		if err := metric.Write(&sample); err != nil {
			t.Fatal(err)
		}
		labels := make(map[string]string)
		for _, label := range sample.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		if labels["repo"] == "example-org/sample-repo" && labels["window"] != "" && labels["workflow"] == "" && labels["event"] == "" {
			maxima[labels["window"]] = sample.GetGauge().GetValue()
		}
	}
	return maxima
}

func TestWorkflowRunBotSHAMaxSortsHumansBeforeTiedBots(t *testing.T) {
	now := time.Now().UTC()
	createdAt := now.Add(-time.Minute).UnixNano()
	snapshot := &store.WorkflowRunBotSHASnapshot{
		Repositories: []store.WorkflowRunBotSHASeries{{Owner: "example-org", Repo: "sample-repo"}},
		Events: []store.WorkflowRunBotSHA{
			{Owner: "example-org", Repo: "sample-repo", Branch: "topic/bot", SHA: "sha-bot", DefaultBranch: "main", IsBot: true, CreatedAtNS: createdAt},
			{Owner: "example-org", Repo: "sample-repo", Branch: "main", SHA: "sha-human", DefaultBranch: "main", IsBot: false, CreatedAtNS: createdAt},
		},
	}
	collector := newWorkflowRunAttemptTestCollector(botSHASnapshotStore{snapshot: snapshot})
	maxima := collectBotSHAMaxima(t, collector)
	if maxima["30m"] != 1 || maxima["6h"] != 1 {
		t.Fatalf("maxima with tied human and bot = %#v, want human reset before bot", maxima)
	}
}

func TestWorkflowRunBotSHASnapshotFailureSetsDatabaseHealthDuringScrape(t *testing.T) {
	collector := newWorkflowRunAttemptTestCollector(botSHASnapshotStore{err: errors.New("test database error")})
	ch := make(chan prometheus.Metric, 64)
	collector.Collect(ch)
	close(ch)

	databaseError := 0.0
	for metric := range ch {
		var sample dto.Metric
		if err := metric.Write(&sample); err != nil {
			t.Fatal(err)
		}
		for _, label := range sample.GetLabel() {
			if label.GetName() == "condition" && label.GetValue() == WorkflowRunHealthDatabaseError {
				databaseError = sample.GetGauge().GetValue()
			}
		}
	}
	if databaseError != 1 {
		t.Fatalf("database_error = %v, want 1 after bot SHA snapshot failure", databaseError)
	}
}

func TestWorkflowRunBotSHAPrunFailureSetsDatabaseHealthAndOmitsMetric(t *testing.T) {
	now := time.Now().UTC()
	collector := newWorkflowRunAttemptTestCollector(botSHASnapshotStore{
		snapshot: &store.WorkflowRunBotSHASnapshot{
			Repositories: []store.WorkflowRunBotSHASeries{{Owner: "example-org", Repo: "sample-repo"}},
			Events: []store.WorkflowRunBotSHA{{
				Owner: "example-org", Repo: "sample-repo", Branch: "topic/loop", SHA: "sha-example",
				DefaultBranch: "main", IsBot: true, CreatedAtNS: now.Add(-time.Minute).UnixNano(),
			}},
		},
		pruneErr: errors.New("test prune error"),
	})
	metrics := collectWorkflowRunAttemptTestMetrics(collector)
	maxima := 0
	databaseError := 0.0
	for _, metric := range metrics {
		var sample dto.Metric
		if err := metric.Write(&sample); err != nil {
			t.Fatal(err)
		}
		for _, label := range sample.GetLabel() {
			if label.GetName() == "window" && label.GetValue() != "" {
				maxima++
			}
			if label.GetName() == "condition" && label.GetValue() == WorkflowRunHealthDatabaseError {
				databaseError = sample.GetGauge().GetValue()
			}
		}
	}
	if maxima != 0 || databaseError != 1 {
		t.Fatalf("prune failure emitted %d maxima and database_error=%v", maxima, databaseError)
	}
}
