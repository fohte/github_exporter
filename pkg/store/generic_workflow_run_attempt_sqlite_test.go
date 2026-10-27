//go:build sqlite

package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-github/v84/github"
	"github.com/jmoiron/sqlx"
)

func TestWorkflowRunAttemptLedgerDeduplicatesAndUsesStartedAtWindows(t *testing.T) {
	db := newWorkflowRunAttemptTestStore(t, filepath.Join(t.TempDir(), "attempts.sqlite"))
	now := time.Now().UTC()

	requested := workflowRunAttemptTestEvent("requested", 100, 1, "push", "topic/example", now.Add(-time.Minute))
	if err := db.StoreWorkflowRunAttemptEvent(requested); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.GetWorkflowRunAttemptSnapshot(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Series) != 0 {
		t.Fatalf("requested event registered a series: %#v", snapshot.Series)
	}

	for _, action := range []string{"completed", "in_progress", "completed"} {
		if err := db.StoreWorkflowRunAttemptEvent(workflowRunAttemptTestEvent(action, 100, 1, "push", "topic/example", now.Add(-time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.StoreWorkflowRunAttemptEvent(workflowRunAttemptTestEvent("completed", 100, 2, "push", "topic/example", now.Add(-10*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if err := db.StoreWorkflowRunAttemptEvent(workflowRunAttemptTestEvent("in_progress", 101, 1, "push", "topic/example", now.Add(-30*time.Minute))); err != nil {
		t.Fatal(err)
	}
	if err := db.StoreWorkflowRunAttemptEvent(workflowRunAttemptTestEvent("completed", 102, 1, "push", "topic/example", now.Add(-30*time.Minute+time.Nanosecond))); err != nil {
		t.Fatal(err)
	}
	if err := db.StoreWorkflowRunAttemptEvent(workflowRunAttemptTestEvent("completed", 105, 1, "push", "topic/example", now.Add(-6*time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := db.StoreWorkflowRunAttemptEvent(workflowRunAttemptTestEvent("completed", 106, 1, "push", "topic/example", now.Add(-6*time.Hour+time.Nanosecond))); err != nil {
		t.Fatal(err)
	}
	if err := db.StoreWorkflowRunAttemptEvent(workflowRunAttemptTestEvent("completed", 103, 1, "push", "topic/example", now.Add(-7*time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := db.StoreWorkflowRunAttemptEvent(workflowRunAttemptTestEvent("completed", 104, 1, "schedule", "topic/empty", now.Add(-7*time.Hour))); err != nil {
		t.Fatal(err)
	}

	snapshot, err = db.GetWorkflowRunAttemptSnapshot(now)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Series) != 2 {
		t.Fatalf("registered series = %d, want 2", len(snapshot.Series))
	}
	var pushCount *WorkflowRunAttemptBranchCount
	var scheduleCount *WorkflowRunAttemptBranchCount
	for i := range snapshot.BranchCounts {
		row := &snapshot.BranchCounts[i]
		switch row.Event {
		case "push":
			pushCount = row
		case "schedule":
			scheduleCount = row
		}
	}
	if pushCount == nil || pushCount.Attempts30m != 3 || pushCount.Attempts6h != 5 {
		t.Fatalf("push branch counts = %#v, want 3 in 30m and 5 in 6h", pushCount)
	}
	if scheduleCount != nil {
		t.Fatalf("out-of-window schedule attempt was counted: %#v", scheduleCount)
	}
}

func TestWorkflowRunAttemptLedgerSurvivesRestartAndRejectsShortRetention(t *testing.T) {
	database := filepath.Join(t.TempDir(), "attempts.sqlite")
	startedAt := time.Now().UTC().Add(-time.Minute)
	db := newWorkflowRunAttemptTestStore(t, database)
	if err := db.StoreWorkflowRunAttemptEvent(workflowRunAttemptTestEvent("in_progress", 200, 1, "push", "topic/persisted", startedAt)); err != nil {
		t.Fatal(err)
	}
	first, err := db.GetWorkflowRunAttemptSnapshot(time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = newWorkflowRunAttemptTestStore(t, database)
	defer func() { _ = db.Close() }()
	second, err := db.GetWorkflowRunAttemptSnapshot(time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !second.InitializedAt.Equal(first.InitializedAt) {
		t.Fatalf("initialized at after restart = %s, want %s", second.InitializedAt, first.InitializedAt)
	}
	if len(second.Series) != 1 || len(second.BranchCounts) != 1 || second.BranchCounts[0].Attempts30m != 1 {
		t.Fatalf("snapshot after restart = %#v, want persisted attempt", second)
	}
	if err := db.PruneWorkflowRunAttempts(5 * time.Hour); err == nil {
		t.Fatal("retention shorter than the 6h query window was accepted")
	}
}

func TestWorkflowRunAttemptSeriesLimitIsGlobalAndReported(t *testing.T) {
	db := newWorkflowRunAttemptTestStore(t, filepath.Join(t.TempDir(), "attempts.sqlite"))
	defer func() { _ = db.Close() }()
	startedAt := time.Now().UTC()

	for workflowID := int64(1); workflowID <= workflowRunAttemptSeriesLimit+1; workflowID++ {
		event := workflowRunAttemptTestEvent("in_progress", workflowID, 1, "push", "topic/cap", startedAt)
		event.WorkflowRun.WorkflowID = testPointer(workflowID)
		if err := db.StoreWorkflowRunAttemptEvent(event); err != nil {
			t.Fatalf("store workflow %d: %v", workflowID, err)
		}
	}

	snapshot, err := db.GetWorkflowRunAttemptSnapshot(startedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Series) != workflowRunAttemptSeriesLimit {
		t.Fatalf("registered series = %d, want %d", len(snapshot.Series), workflowRunAttemptSeriesLimit)
	}
	if !snapshot.SeriesLimitExceeded {
		t.Fatal("series limit overflow was not reported")
	}
}

func newWorkflowRunAttemptTestStore(t *testing.T, database string) *sqliteStore {
	t.Helper()
	handle, err := sqlx.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	db := &sqliteStore{handle: handle}
	if err := db.Migrate(); err != nil {
		_ = handle.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	return db
}

func workflowRunAttemptTestEvent(action string, runID int64, attempt int, event, branch string, startedAt time.Time) *github.WorkflowRunEvent {
	return &github.WorkflowRunEvent{
		Action: testPointer(action),
		Repo: &github.Repository{
			Name: testPointer("sample-repo"),
			Owner: &github.User{
				Login: testPointer("example-org"),
			},
		},
		WorkflowRun: &github.WorkflowRun{
			ID:           testPointer(runID),
			WorkflowID:   testPointer(int64(9)),
			RunAttempt:   testPointer(attempt),
			Event:        testPointer(event),
			HeadBranch:   testPointer(branch),
			RunStartedAt: &github.Timestamp{Time: startedAt},
		},
	}
}

func testPointer[T any](value T) *T {
	return &value
}
