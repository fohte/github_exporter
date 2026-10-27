package exporter

import (
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/go-github/v84/github"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/promhippie/github_exporter/pkg/config"
	"github.com/promhippie/github_exporter/pkg/store"
)

type attemptSnapshotStore struct {
	StaticStore
	snapshot *store.WorkflowRunAttemptSnapshot
	err      error
}

func (s attemptSnapshotStore) GetWorkflowRunAttemptSnapshot(time.Time) (*store.WorkflowRunAttemptSnapshot, error) {
	return s.snapshot, s.err
}

func TestWorkflowRunAttemptCollectorEmitsMaximaAndKnownZeroSeries(t *testing.T) {
	now := time.Now().UTC()
	snapshotStore := attemptSnapshotStore{snapshot: &store.WorkflowRunAttemptSnapshot{
		InitializedAt: now.Add(-7 * time.Hour),
		Series: []store.WorkflowRunAttemptSeriesKey{
			{Owner: "example-org", Repo: "sample-repo", WorkflowID: 9, Event: "push"},
			{Owner: "example-org", Repo: "sample-repo", WorkflowID: 10, Event: "schedule"},
		},
		BranchCounts: []store.WorkflowRunAttemptBranchCount{
			{Owner: "example-org", Repo: "sample-repo", WorkflowID: 9, Event: "push", Branch: "topic/alpha", Attempts30m: 4, Attempts6h: 10},
			{Owner: "example-org", Repo: "sample-repo", WorkflowID: 9, Event: "push", Branch: "topic/beta", Attempts30m: 4, Attempts6h: 8},
		},
	}}
	collector := newWorkflowRunAttemptTestCollector(snapshotStore)

	metrics := collectWorkflowRunAttemptTestMetrics(collector)
	maxima := make(map[string]float64)
	health := make(map[string]float64)
	for _, metric := range metrics {
		var sample dto.Metric
		if err := metric.Write(&sample); err != nil {
			t.Fatal(err)
		}
		labels := make(map[string]string)
		for _, label := range sample.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		if window, ok := labels["window"]; ok {
			maxima[labels["event"]+"/"+window] = sample.GetGauge().GetValue()
		}
		if condition, ok := labels["condition"]; ok {
			health[condition] = sample.GetGauge().GetValue()
		}
	}

	if len(maxima) != 4 {
		t.Fatalf("attempt maxima = %#v, want four window values", maxima)
	}
	if maxima["push/30m"] != 4 || maxima["push/6h"] != 10 || maxima["schedule/30m"] != 0 || maxima["schedule/6h"] != 0 {
		t.Fatalf("attempt maxima = %#v", maxima)
	}
	if health[WorkflowRunHealthHistoryReady] != 1 || health[WorkflowRunHealthDatabaseError] != 0 {
		t.Fatalf("attempt health = %#v", health)
	}
}

func TestWorkflowRunAttemptCollectorHidesPartialHistory(t *testing.T) {
	now := time.Now().UTC()
	collector := newWorkflowRunAttemptTestCollector(attemptSnapshotStore{snapshot: &store.WorkflowRunAttemptSnapshot{
		InitializedAt: now.Add(-time.Hour),
		Series:        []store.WorkflowRunAttemptSeriesKey{{Owner: "example-org", Repo: "sample-repo", WorkflowID: 9, Event: "push"}},
	}})

	metrics := collectWorkflowRunAttemptTestMetrics(collector)
	maxima := 0
	ready := -1.0
	for _, metric := range metrics {
		var sample dto.Metric
		if err := metric.Write(&sample); err != nil {
			t.Fatal(err)
		}
		for _, label := range sample.GetLabel() {
			if label.GetName() == "window" {
				maxima++
			}
			if label.GetName() == "condition" && label.GetValue() == WorkflowRunHealthHistoryReady {
				ready = sample.GetGauge().GetValue()
			}
		}
	}
	if maxima != 0 || ready != 0 {
		t.Fatalf("partial history emitted %d maxima and history_ready=%v", maxima, ready)
	}
}

func TestWorkflowRunAttemptCollectorReportsDatabaseFailureWithoutZeroMaxima(t *testing.T) {
	collector := newWorkflowRunAttemptTestCollector(attemptSnapshotStore{err: errors.New("test database error")})
	metrics := collectWorkflowRunAttemptTestMetrics(collector)
	maxima := 0
	databaseError := 0.0
	for _, metric := range metrics {
		var sample dto.Metric
		if err := metric.Write(&sample); err != nil {
			t.Fatal(err)
		}
		for _, label := range sample.GetLabel() {
			if label.GetName() == "window" {
				maxima++
			}
			if label.GetName() == "condition" && label.GetValue() == WorkflowRunHealthDatabaseError {
				databaseError = sample.GetGauge().GetValue()
			}
		}
	}
	if maxima != 0 || databaseError != 1 {
		t.Fatalf("database error emitted %d maxima and database_error=%v", maxima, databaseError)
	}
}

func newWorkflowRunAttemptTestCollector(db store.Store) *WorkflowRunCollector {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	failures := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_attempt_failures_total", Help: "Test failures"}, []string{"type"})
	duration := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "test_attempt_duration_seconds", Help: "Test duration"}, []string{"type"})
	cfg := config.Target{WorkflowRuns: config.WorkflowRuns{AttemptRetention: 24 * time.Hour, Labels: config.RunLabels()}}
	return NewWorkflowRunCollector(logger, &github.Client{}, db, failures, duration, cfg, NewWorkflowRunHealth(false))
}

func collectWorkflowRunAttemptTestMetrics(collector *WorkflowRunCollector) []prometheus.Metric {
	ch := make(chan prometheus.Metric, 64)
	collector.Collect(ch)
	close(ch)
	metrics := make([]prometheus.Metric, 0)
	for metric := range ch {
		metrics = append(metrics, metric)
	}
	return metrics
}
