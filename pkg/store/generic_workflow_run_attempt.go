package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/go-github/v84/github"
	"github.com/jmoiron/sqlx"
)

const workflowRunAttemptSeriesLimit = 500

var workflowRunAttemptMu sync.Mutex

// storeWorkflowRunAttemptEvent keeps one started attempt per run and attempt ID.
func storeWorkflowRunAttemptEvent(handle *sqlx.DB, event *github.WorkflowRunEvent) error {
	if event == nil || event.GetAction() == "requested" {
		return nil
	}

	if event.GetAction() != "in_progress" && event.GetAction() != "completed" {
		return nil
	}

	run := event.GetWorkflowRun()
	startedAt := run.GetRunStartedAt().Time
	owner := event.GetRepo().GetOwner().GetLogin()
	repo := event.GetRepo().GetName()
	workflowID := run.GetWorkflowID()
	eventName := run.GetEvent()
	branch := run.GetHeadBranch()
	runID := run.GetID()
	runAttempt := run.GetRunAttempt()

	if startedAt.IsZero() || owner == "" || repo == "" || workflowID == 0 || eventName == "" || branch == "" || runID == 0 || runAttempt < 1 {
		return nil
	}

	workflowRunAttemptMu.Lock()
	defer workflowRunAttemptMu.Unlock()

	tx, err := handle.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin workflow run attempt transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensureWorkflowRunAttemptMeta(tx, time.Now()); err != nil {
		return err
	}

	attemptKey := workflowRunHash(
		owner,
		repo,
		strconv.FormatInt(runID, 10),
		strconv.Itoa(runAttempt),
	)
	var existing int
	err = tx.Get(
		&existing,
		tx.Rebind("SELECT 1 FROM workflow_run_attempts WHERE attempt_key = ?"),
		attemptKey,
	)
	if err == nil {
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check workflow run attempt: %w", err)
	}

	seriesKey := workflowRunHash(
		owner,
		repo,
		strconv.FormatInt(workflowID, 10),
		eventName,
	)
	registered, err := registerWorkflowRunAttemptSeries(tx, seriesKey, owner, repo, workflowID, eventName, time.Now())
	if err != nil {
		return err
	}
	if !registered {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit workflow run attempt series limit: %w", err)
		}

		return nil
	}

	_, err = tx.Exec(
		tx.Rebind(`INSERT INTO workflow_run_attempts (
			attempt_key, owner, repo, workflow_id, event, branch, run_id, run_attempt, started_at_ns
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`),
		attemptKey,
		owner,
		repo,
		workflowID,
		eventName,
		branch,
		runID,
		runAttempt,
		startedAt.UnixNano(),
	)
	if err != nil {
		return fmt.Errorf("failed to insert workflow run attempt: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit workflow run attempt: %w", err)
	}

	return nil
}

func registerWorkflowRunAttemptSeries(tx *sqlx.Tx, key, owner, repo string, workflowID int64, event string, now time.Time) (bool, error) {
	var exists int
	err := tx.Get(
		&exists,
		tx.Rebind("SELECT 1 FROM workflow_run_attempt_series WHERE series_key = ?"),
		key,
	)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("failed to check workflow run attempt series: %w", err)
	}

	var count int
	if err := tx.Get(&count, "SELECT COUNT(*) FROM workflow_run_attempt_series"); err != nil {
		return false, fmt.Errorf("failed to count workflow run attempt series: %w", err)
	}
	if count >= workflowRunAttemptSeriesLimit {
		if _, err := tx.Exec("UPDATE workflow_run_attempt_meta SET series_limit_exceeded = 1 WHERE id = 1"); err != nil {
			return false, fmt.Errorf("failed to record workflow run attempt series limit: %w", err)
		}

		return false, nil
	}

	_, err = tx.Exec(
		tx.Rebind(`INSERT INTO workflow_run_attempt_series (
			series_key, owner, repo, workflow_id, event, created_at_ns
		) VALUES (?, ?, ?, ?, ?, ?)`),
		key,
		owner,
		repo,
		workflowID,
		event,
		now.UnixNano(),
	)
	if err != nil {
		return false, fmt.Errorf("failed to register workflow run attempt series: %w", err)
	}

	return true, nil
}

// getWorkflowRunAttemptSnapshot returns registered series and branch counts.
func getWorkflowRunAttemptSnapshot(handle *sqlx.DB, now time.Time) (*WorkflowRunAttemptSnapshot, error) {
	workflowRunAttemptMu.Lock()
	defer workflowRunAttemptMu.Unlock()

	tx, err := handle.Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to begin workflow run attempt snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := ensureWorkflowRunAttemptMeta(tx, now); err != nil {
		return nil, err
	}

	meta := struct {
		InitializedAtNS int64 `db:"initialized_at_ns"`
		Exceeded        int   `db:"series_limit_exceeded"`
	}{}
	if err := tx.Get(
		&meta,
		tx.Rebind("SELECT initialized_at_ns, series_limit_exceeded FROM workflow_run_attempt_meta WHERE id = ?"),
		1,
	); err != nil {
		return nil, fmt.Errorf("failed to read workflow run attempt metadata: %w", err)
	}
	snapshot := &WorkflowRunAttemptSnapshot{
		InitializedAt:       time.Unix(0, meta.InitializedAtNS),
		SeriesLimitExceeded: meta.Exceeded != 0,
		Series:              make([]WorkflowRunAttemptSeriesKey, 0),
		BranchCounts:        make([]WorkflowRunAttemptBranchCount, 0),
	}

	if err := tx.Select(
		&snapshot.Series,
		tx.Rebind("SELECT owner, repo, workflow_id, event, created_at_ns FROM workflow_run_attempt_series ORDER BY created_at_ns, series_key LIMIT ?"),
		workflowRunAttemptSeriesLimit+1,
	); err != nil {
		return nil, fmt.Errorf("failed to read workflow run attempt series: %w", err)
	}
	if len(snapshot.Series) > workflowRunAttemptSeriesLimit {
		snapshot.Series = snapshot.Series[:workflowRunAttemptSeriesLimit]
		snapshot.SeriesLimitExceeded = true
	}

	cutoff30m := now.Add(-30 * time.Minute).UnixNano()
	cutoff6h := now.Add(-6 * time.Hour).UnixNano()
	if err := tx.Select(
		&snapshot.BranchCounts,
		tx.Rebind(`SELECT owner, repo, workflow_id, event, branch,
			SUM(CASE WHEN started_at_ns > ? THEN 1 ELSE 0 END) AS attempts_30m,
			COUNT(*) AS attempts_6h
		FROM workflow_run_attempts
		WHERE started_at_ns > ? AND started_at_ns <= ?
		GROUP BY owner, repo, workflow_id, event, branch`),
		cutoff30m,
		cutoff6h,
		now.UnixNano(),
	); err != nil {
		return nil, fmt.Errorf("failed to count workflow run attempts: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit workflow run attempt snapshot: %w", err)
	}

	return snapshot, nil
}

func ensureWorkflowRunAttemptMeta(tx *sqlx.Tx, now time.Time) error {
	var exists int
	err := tx.Get(
		&exists,
		tx.Rebind("SELECT 1 FROM workflow_run_attempt_meta WHERE id = ?"),
		1,
	)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check workflow run attempt metadata: %w", err)
	}

	if _, err := tx.Exec(
		tx.Rebind("INSERT INTO workflow_run_attempt_meta (id, initialized_at_ns, series_limit_exceeded) VALUES (?, ?, ?)"),
		1,
		now.UnixNano(),
		0,
	); err != nil {
		return fmt.Errorf("failed to initialize workflow run attempt metadata: %w", err)
	}

	return nil
}

func pruneWorkflowRunAttempts(handle *sqlx.DB, retention time.Duration) error {
	if retention < 6*time.Hour {
		return fmt.Errorf("workflow run attempt retention must be at least 6h: %s", retention)
	}

	cutoff := time.Now().Add(-retention).UnixNano()
	if _, err := handle.Exec(
		handle.Rebind("DELETE FROM workflow_run_attempts WHERE started_at_ns < ?"),
		cutoff,
	); err != nil {
		return fmt.Errorf("failed to prune workflow run attempts: %w", err)
	}

	return nil
}

func getWorkflowRunRecoveryCheckpoint(handle *sqlx.DB, owner, repo string, hookID int64) (WorkflowRunRecoveryCheckpoint, error) {
	checkpoint := WorkflowRunRecoveryCheckpoint{Owner: owner, Repo: repo, HookID: hookID}
	key := workflowRunHash(owner, repo, strconv.FormatInt(hookID, 10))
	var record struct {
		Cursor            string `db:"scan_cursor"`
		ScanStartedAtNS   int64  `db:"scan_started_at_ns"`
		ScanCutoffNS      int64  `db:"scan_cutoff_ns"`
		LastCompletedAtNS int64  `db:"last_completed_at_ns"`
		UpdatedAtNS       int64  `db:"updated_at_ns"`
	}
	if err := handle.Get(
		&record,
		handle.Rebind(`SELECT scan_cursor, scan_started_at_ns, scan_cutoff_ns, last_completed_at_ns, updated_at_ns
			FROM workflow_run_recovery_checkpoints WHERE checkpoint_key = ?`),
		key,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return checkpoint, nil
		}

		return checkpoint, fmt.Errorf("failed to read workflow run recovery checkpoint: %w", err)
	}

	checkpoint.Cursor = record.Cursor
	checkpoint.ScanStartedAt = unixNanoOrZero(record.ScanStartedAtNS)
	checkpoint.ScanCutoff = unixNanoOrZero(record.ScanCutoffNS)
	checkpoint.LastCompletedAt = unixNanoOrZero(record.LastCompletedAtNS)
	checkpoint.UpdatedAt = unixNanoOrZero(record.UpdatedAtNS)

	return checkpoint, nil
}

func saveWorkflowRunRecoveryCheckpoint(handle *sqlx.DB, checkpoint WorkflowRunRecoveryCheckpoint) error {
	workflowRunAttemptMu.Lock()
	defer workflowRunAttemptMu.Unlock()

	key := workflowRunHash(checkpoint.Owner, checkpoint.Repo, strconv.FormatInt(checkpoint.HookID, 10))
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now()
	}
	values := []any{
		checkpoint.Cursor,
		unixNano(checkpoint.ScanStartedAt),
		unixNano(checkpoint.ScanCutoff),
		unixNano(checkpoint.LastCompletedAt),
		updatedAt.UnixNano(),
		key,
	}

	tx, err := handle.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin workflow run recovery checkpoint transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var exists int
	err = tx.Get(&exists, tx.Rebind("SELECT 1 FROM workflow_run_recovery_checkpoints WHERE checkpoint_key = ?"), key)
	if err == nil {
		if _, err := tx.Exec(
			tx.Rebind(`UPDATE workflow_run_recovery_checkpoints
				SET scan_cursor = ?, scan_started_at_ns = ?, scan_cutoff_ns = ?, last_completed_at_ns = ?, updated_at_ns = ?
				WHERE checkpoint_key = ?`),
			values...,
		); err != nil {
			return fmt.Errorf("failed to update workflow run recovery checkpoint: %w", err)
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.Exec(
			tx.Rebind(`INSERT INTO workflow_run_recovery_checkpoints (
				checkpoint_key, owner, repo, hook_id, scan_cursor, scan_started_at_ns, scan_cutoff_ns, last_completed_at_ns, updated_at_ns
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			key,
			checkpoint.Owner,
			checkpoint.Repo,
			checkpoint.HookID,
			checkpoint.Cursor,
			unixNano(checkpoint.ScanStartedAt),
			unixNano(checkpoint.ScanCutoff),
			unixNano(checkpoint.LastCompletedAt),
			updatedAt.UnixNano(),
		)
		if err != nil {
			return fmt.Errorf("failed to insert workflow run recovery checkpoint: %w", err)
		}
	} else {
		return fmt.Errorf("failed to check workflow run recovery checkpoint: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit workflow run recovery checkpoint: %w", err)
	}

	return nil
}

func reserveWorkflowRunRecoveryDelivery(handle *sqlx.DB, owner, repo string, hookID int64, guid string, deliveryID int64, attemptedAt time.Time) (bool, error) {
	if guid == "" {
		return false, errors.New("delivery GUID is empty")
	}

	workflowRunAttemptMu.Lock()
	defer workflowRunAttemptMu.Unlock()

	key := workflowRunHash(owner, repo, strconv.FormatInt(hookID, 10), guid)
	var exists int
	err := handle.Get(
		&exists,
		handle.Rebind("SELECT 1 FROM workflow_run_recovery_attempts WHERE delivery_key = ?"),
		key,
	)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("failed to check workflow run recovery attempt: %w", err)
	}

	_, err = handle.Exec(
		handle.Rebind(`INSERT INTO workflow_run_recovery_attempts (
			delivery_key, owner, repo, hook_id, guid, delivery_id, attempted_at_ns, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`),
		key,
		owner,
		repo,
		hookID,
		guid,
		deliveryID,
		attemptedAt.UnixNano(),
		"pending",
	)
	if err != nil {
		return false, fmt.Errorf("failed to reserve workflow run recovery attempt: %w", err)
	}

	return true, nil
}

func markWorkflowRunRecoveryDelivery(handle *sqlx.DB, owner, repo string, hookID int64, guid string, deliveryID int64, at time.Time, status string) error {
	if guid == "" {
		return errors.New("delivery GUID is empty")
	}

	workflowRunAttemptMu.Lock()
	defer workflowRunAttemptMu.Unlock()

	key := workflowRunHash(owner, repo, strconv.FormatInt(hookID, 10), guid)
	var exists int
	err := handle.Get(
		&exists,
		handle.Rebind("SELECT 1 FROM workflow_run_recovery_attempts WHERE delivery_key = ?"),
		key,
	)
	if err == nil {
		if _, err := handle.Exec(
			handle.Rebind("UPDATE workflow_run_recovery_attempts SET delivery_id = ?, status = ? WHERE delivery_key = ?"),
			deliveryID,
			status,
			key,
		); err != nil {
			return fmt.Errorf("failed to update workflow run recovery attempt: %w", err)
		}

		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to check workflow run recovery attempt: %w", err)
	}

	if _, err := handle.Exec(
		handle.Rebind(`INSERT INTO workflow_run_recovery_attempts (
			delivery_key, owner, repo, hook_id, guid, delivery_id, attempted_at_ns, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`),
		key,
		owner,
		repo,
		hookID,
		guid,
		deliveryID,
		unixNano(at),
		status,
	); err != nil {
		return fmt.Errorf("failed to record workflow run recovery delivery: %w", err)
	}

	return nil
}

func workflowRunHash(parts ...string) string {
	hash := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(hash[:])
}

func unixNano(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}

	return value.UnixNano()
}

func unixNanoOrZero(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}

	return time.Unix(0, value)
}
