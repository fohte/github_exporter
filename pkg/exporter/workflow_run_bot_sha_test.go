package exporter

import (
	"bytes"
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
}

func (s botSHASnapshotStore) GetWorkflowRunBotSHASnapshot(time.Time) (*store.WorkflowRunBotSHASnapshot, error) {
	return s.snapshot, nil
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

	maxima := collectBotSHAMaxima(collector, now)
	if maxima["30m"] != 2 || maxima["6h"] != 2 {
		t.Fatalf("maxima before reset = %#v, want 2 in both windows", maxima)
	}
	if !strings.Contains(logs.String(), "topic/loop") {
		t.Fatalf("maximum branch was not logged: %s", logs.String())
	}

	snapshot := collector.db.(botSHASnapshotStore).snapshot
	snapshot.Events = append(snapshot.Events, event("topic/loop", "sha-human", 5*time.Minute, false))
	maxima = collectBotSHAMaxima(collector, now)
	if maxima["30m"] != 0 || maxima["6h"] != 0 {
		t.Fatalf("maxima after branch human reset = %#v, want zero", maxima)
	}

	snapshot.Events = append(snapshot.Events, event("topic/loop", "sha-c", 4*time.Minute, true))
	maxima = collectBotSHAMaxima(collector, now)
	if maxima["30m"] != 1 || maxima["6h"] != 1 {
		t.Fatalf("maxima after new bot SHA = %#v, want 1", maxima)
	}

	snapshot.Events = append(snapshot.Events,
		event("topic/other", "sha-other", 3*time.Minute, true),
		event("main", "sha-default-human", 2*time.Minute, false),
	)
	maxima = collectBotSHAMaxima(collector, now)
	if maxima["30m"] != 0 || maxima["6h"] != 0 {
		t.Fatalf("maxima after default branch human reset = %#v, want zero", maxima)
	}
}

func collectBotSHAMaxima(collector *WorkflowRunCollector, now time.Time) map[string]float64 {
	ch := make(chan prometheus.Metric, 2)
	collector.collectWorkflowRunBotSHAMax(ch, now)
	close(ch)
	maxima := make(map[string]float64)
	for metric := range ch {
		var sample dto.Metric
		if err := metric.Write(&sample); err != nil {
			panic(err)
		}
		for _, label := range sample.GetLabel() {
			if label.GetName() == "window" {
				maxima[label.GetValue()] = sample.GetGauge().GetValue()
			}
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
	maxima := collectBotSHAMaxima(collector, now)
	if maxima["30m"] != 1 || maxima["6h"] != 1 {
		t.Fatalf("maxima with tied human and bot = %#v, want human reset before bot", maxima)
	}
}
