package exporter

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"strings"
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

func TestWorkflowRunAttemptCollectorEmitsOnlyWindowsCoveredByHistory(t *testing.T) {
	tests := []struct {
		history time.Duration
		windows string
		ready   float64
	}{
		{history: 29 * time.Minute, windows: "", ready: 0},
		{history: 31 * time.Minute, windows: "30m", ready: 0},
		{history: 6*time.Hour - time.Minute, windows: "30m", ready: 0},
		{history: 6*time.Hour + time.Minute, windows: "30m,6h", ready: 1},
	}
	for _, tt := range tests {
		t.Run(tt.history.String(), func(t *testing.T) {
			collector := newWorkflowRunAttemptTestCollector(attemptSnapshotStore{snapshot: &store.WorkflowRunAttemptSnapshot{
				InitializedAt: time.Now().Add(-tt.history),
				Series:        []store.WorkflowRunAttemptSeriesKey{{Owner: "example-org", Repo: "sample-repo", WorkflowID: 9, Event: "push"}},
			}})

			windows := make([]string, 0)
			ready := -1.0
			for _, metric := range collectWorkflowRunAttemptTestMetrics(collector) {
				var sample dto.Metric
				if err := metric.Write(&sample); err != nil {
					t.Fatal(err)
				}
				for _, label := range sample.GetLabel() {
					if label.GetName() == "window" {
						windows = append(windows, label.GetValue())
					}
					if label.GetName() == "condition" && label.GetValue() == WorkflowRunHealthHistoryReady {
						ready = sample.GetGauge().GetValue()
					}
				}
			}
			if strings.Join(windows, ",") != tt.windows || ready != tt.ready {
				t.Fatalf("windows = %v and history_6h_ready = %v, want %q and %v", windows, ready, tt.windows, tt.ready)
			}
		})
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

func TestWorkflowRunAttemptCollectorKeepsWebhookDatabaseErrorAfterSuccessfulScrape(t *testing.T) {
	collector := newWorkflowRunAttemptTestCollector(attemptSnapshotStore{snapshot: &store.WorkflowRunAttemptSnapshot{}})
	databaseError := func() float64 {
		collectWorkflowRunAttemptTestMetrics(collector)
		for _, state := range collector.health.Snapshot() {
			if state.Condition == WorkflowRunHealthDatabaseError {
				return state.Value
			}
		}
		return -1
	}

	collector.health.SetDatabaseError("webhook", true)
	if got := databaseError(); got != 1 {
		t.Fatalf("database_error after webhook failure and successful scrape = %v, want 1", got)
	}

	collector.health.SetDatabaseError("webhook", false)
	if got := databaseError(); got != 0 {
		t.Fatalf("database_error after webhook recovery = %v, want 0", got)
	}
}

func TestWorkflowRunAttemptCollectorLogsMaximumBranchesWhenTheyChange(t *testing.T) {
	branch := func(name string, attempts int64) store.WorkflowRunAttemptBranchCount {
		return store.WorkflowRunAttemptBranchCount{Owner: "example-org", Repo: "sample-repo", WorkflowID: 9, Event: "push", Branch: name, Attempts30m: attempts, Attempts6h: attempts}
	}
	collector := newWorkflowRunAttemptTestCollector(nil)
	var logs bytes.Buffer
	collector.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo}))

	steps := []struct {
		name     string
		branches []store.WorkflowRunAttemptBranchCount
		logged   int
	}{
		{name: "single run per branch", branches: []store.WorkflowRunAttemptBranchCount{branch("topic/single", 1)}, logged: 0},
		{name: "new maximum", branches: []store.WorkflowRunAttemptBranchCount{branch("topic/looping", 3)}, logged: 2},
		{name: "unchanged", branches: []store.WorkflowRunAttemptBranchCount{branch("topic/looping", 3)}, logged: 0},
		{name: "tied branch", branches: []store.WorkflowRunAttemptBranchCount{branch("topic/looping", 3), branch("topic/tied", 3)}, logged: 2},
	}
	for _, step := range steps {
		logs.Reset()
		collector.db = attemptSnapshotStore{snapshot: &store.WorkflowRunAttemptSnapshot{
			InitializedAt: time.Now().Add(-7 * time.Hour),
			Series:        []store.WorkflowRunAttemptSeriesKey{{Owner: "example-org", Repo: "sample-repo", WorkflowID: 9, Event: "push"}},
			BranchCounts:  step.branches,
		}}
		collectWorkflowRunAttemptTestMetrics(collector)
		if got := strings.Count(logs.String(), "Workflow run attempts per branch changed"); got != step.logged {
			t.Fatalf("%s: logged %d times, want %d:\n%s", step.name, got, step.logged, logs.String())
		}
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
