//go:build sqlite

package store

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/GuiaBolso/darwin"
	"github.com/google/go-github/v84/github"
	"github.com/jmoiron/sqlx"
	"github.com/promhippie/github_exporter/pkg/migration/dialect"

	// Import SQLite driver for database/sql
	_ "modernc.org/sqlite"
)

var (
	sqliteMigrations = []darwin.Migration{
		{
			Version:     1,
			Description: "Creating table workflow_runs",
			Script: `CREATE TABLE workflow_runs (
				owner TEXT NOT NULL,
				repo TEXT NOT NULL,
				workflow_id INTEGER NOT NULL,
				number INTEGER NOT NULL,
				attempt INTEGER,
				event TEXT,
				name TEXT,
				title TEXT,
				status TEXT,
				branch TEXT,
				sha TEXT,
				identifier BIGINT,
				created_at BIGINT,
				updated_at BIGINT,
				started_at BIGINT,
				PRIMARY KEY(owner, repo, workflow_id, number)
			);`,
		},
		{
			Version:     2,
			Description: "Adding actor column to workflow_runs table",
			Script:      `ALTER TABLE workflow_runs ADD COLUMN actor TEXT;`,
		},
		{
			Version:     3,
			Description: "Creating table workflow_jobs",
			Script: `CREATE TABLE workflow_jobs (
				owner TEXT NOT NULL,
				repo TEXT NOT NULL,
				name TEXT,
				status TEXT,
				branch TEXT,
				sha TEXT,
				conclusion TEXT,
				labels TEXT,
				identifier INTEGER,
				run_id INTEGER NOT NULL,
				run_attempt INTEGER NOT NULL,
				created_at BIGINT,
				started_at BIGINT,
				completed_at BIGINT,
				runner_id INTEGER,
				runner_name TEXT,
				runner_group_id INTEGER,
				runner_group_name TEXT,
				workflow_name TEXT,
				PRIMARY KEY(owner, repo, identifier)
			);`,
		},
		{
			Version:     4,
			Description: "Creating workflow run attempts table",
			Script: `CREATE TABLE workflow_run_attempts (
				attempt_key TEXT NOT NULL PRIMARY KEY,
				owner TEXT NOT NULL,
				repo TEXT NOT NULL,
				workflow_id BIGINT NOT NULL,
				event TEXT NOT NULL,
				branch TEXT NOT NULL,
				run_id BIGINT NOT NULL,
				run_attempt INTEGER NOT NULL,
				started_at_ns BIGINT NOT NULL
			);`,
		},
		{
			Version:     5,
			Description: "Creating workflow run attempt series table",
			Script: `CREATE TABLE workflow_run_attempt_series (
				series_key TEXT NOT NULL PRIMARY KEY,
				owner TEXT NOT NULL,
				repo TEXT NOT NULL,
				workflow_id BIGINT NOT NULL,
				event TEXT NOT NULL,
				created_at_ns BIGINT NOT NULL
			);`,
		},
		{
			Version:     6,
			Description: "Creating workflow run attempt metadata table",
			Script: `CREATE TABLE workflow_run_attempt_meta (
				id INTEGER NOT NULL PRIMARY KEY,
				initialized_at_ns BIGINT NOT NULL,
				series_limit_exceeded INTEGER NOT NULL
			);`,
		},
		{
			Version:     7,
			Description: "Creating workflow run recovery checkpoints table",
			Script: `CREATE TABLE workflow_run_recovery_checkpoints (
				checkpoint_key TEXT NOT NULL PRIMARY KEY,
				owner TEXT NOT NULL,
				repo TEXT NOT NULL,
				hook_id BIGINT NOT NULL,
				scan_cursor TEXT NOT NULL,
				scan_started_at_ns BIGINT NOT NULL,
				scan_cutoff_ns BIGINT NOT NULL,
				last_completed_at_ns BIGINT NOT NULL,
				updated_at_ns BIGINT NOT NULL
			);`,
		},
		{
			Version:     8,
			Description: "Creating workflow run recovery attempts table",
			Script: `CREATE TABLE workflow_run_recovery_attempts (
				delivery_key TEXT NOT NULL PRIMARY KEY,
				owner TEXT NOT NULL,
				repo TEXT NOT NULL,
				hook_id BIGINT NOT NULL,
				guid TEXT NOT NULL,
				delivery_id BIGINT NOT NULL,
				attempted_at_ns BIGINT NOT NULL,
				status TEXT NOT NULL
			);`,
		},
		{
			Version:     9,
			Description: "Indexing workflow run attempt start times",
			Script:      `CREATE INDEX workflow_run_attempts_started_at ON workflow_run_attempts (started_at_ns);`,
		},
	}
)

// sqliteStore implements the Store interface for SQLite.
type sqliteStore struct {
	logger   *slog.Logger
	driver   string
	database string
	meta     url.Values
	handle   *sqlx.DB
}

func init() {
	register("sqlite", NewSqliteStore)
	register("sqlite3", NewSqliteStore)
}

// Open simply opens the database connection.
func (s *sqliteStore) Open() (res bool, err error) {
	if dir := path.Dir(s.database); dir != "." {
		if err := os.MkdirAll(dir, 0770); err != nil {
			return false, fmt.Errorf("failed to create database dir: %w", err)
		}
	}

	s.handle, err = sqlx.Open(
		s.driver,
		s.dsn(),
	)

	if err != nil {
		return false, err
	}

	return true, nil
}

// Close simply closes the database connection.
func (s *sqliteStore) Close() error {
	return s.handle.Close()
}

// Ping just tests the database connection.
func (s *sqliteStore) Ping() (bool, error) {
	if err := s.handle.Ping(); err != nil {
		return false, err
	}

	return true, nil
}

// Migrate executes required db migrations.
func (s *sqliteStore) Migrate() error {
	driver := darwin.New(
		darwin.NewGenericDriver(
			s.handle.DB,
			dialect.SqliteDialect{},
		),
		sqliteMigrations,
		nil,
	)

	if err := driver.Migrate(); err != nil {
		fmt.Printf("%+v\n", err)
		return fmt.Errorf("failed to exec migrations: %w", err)
	}

	return nil
}

// StoreWorkflowRunEvent implements the Store interface.
func (s *sqliteStore) StoreWorkflowRunEvent(event *github.WorkflowRunEvent) error {
	return storeWorkflowRunEvent(s.handle, event)
}

// GetWorkflowRuns implements the Store interface.
func (s *sqliteStore) GetWorkflowRuns(window time.Duration) ([]*WorkflowRun, error) {
	return getWorkflowRuns(s.handle, window)
}

// PruneWorkflowRuns implements the Store interface.
func (s *sqliteStore) PruneWorkflowRuns(timeframe time.Duration) error {
	return pruneWorkflowRuns(s.handle, timeframe)
}

func (s *sqliteStore) StoreWorkflowRunAttemptEvent(event *github.WorkflowRunEvent) error {
	return storeWorkflowRunAttemptEvent(s.handle, event)
}

func (s *sqliteStore) GetWorkflowRunAttemptSnapshot(now time.Time) (*WorkflowRunAttemptSnapshot, error) {
	return getWorkflowRunAttemptSnapshot(s.handle, now)
}

func (s *sqliteStore) PruneWorkflowRunAttempts(retention time.Duration) error {
	return pruneWorkflowRunAttempts(s.handle, retention)
}

func (s *sqliteStore) GetWorkflowRunRecoveryCheckpoint(owner, repo string, hookID int64) (WorkflowRunRecoveryCheckpoint, error) {
	return getWorkflowRunRecoveryCheckpoint(s.handle, owner, repo, hookID)
}

func (s *sqliteStore) SaveWorkflowRunRecoveryCheckpoint(checkpoint WorkflowRunRecoveryCheckpoint) error {
	return saveWorkflowRunRecoveryCheckpoint(s.handle, checkpoint)
}

func (s *sqliteStore) ReserveWorkflowRunRecoveryDelivery(owner, repo string, hookID int64, guid string, deliveryID int64, attemptedAt time.Time) (bool, error) {
	return reserveWorkflowRunRecoveryDelivery(s.handle, owner, repo, hookID, guid, deliveryID, attemptedAt)
}

func (s *sqliteStore) MarkWorkflowRunRecoveryDelivery(owner, repo string, hookID int64, guid string, deliveryID int64, at time.Time, status string) error {
	return markWorkflowRunRecoveryDelivery(s.handle, owner, repo, hookID, guid, deliveryID, at, status)
}

// StoreWorkflowJobEvent implements the Store interface.
func (s *sqliteStore) StoreWorkflowJobEvent(event *github.WorkflowJobEvent) error {
	return storeWorkflowJobEvent(s.handle, event)
}

// GetWorkflowJobs implements the Store interface.
func (s *sqliteStore) GetWorkflowJobs(window time.Duration) ([]*WorkflowJob, error) {
	return getWorkflowJobs(s.handle, window)
}

// PruneWorkflowJobs implements the Store interface.
func (s *sqliteStore) PruneWorkflowJobs(timeframe time.Duration) error {
	return pruneWorkflowJobs(s.handle, timeframe)
}

func (s *sqliteStore) dsn() string {
	if len(s.meta) > 0 {
		return fmt.Sprintf(
			"%s?%s",
			s.database,
			s.meta.Encode(),
		)
	}

	return s.database
}

// NewSqliteStore initializes a new SQLite store.
func NewSqliteStore(dsn string, logger *slog.Logger) (Store, error) {
	parsed, err := url.Parse(dsn)

	if err != nil {
		return nil, fmt.Errorf("failed to parse dsn: %w", err)
	}

	client := &sqliteStore{
		logger:   logger,
		driver:   "sqlite",
		database: path.Join(parsed.Host, parsed.Path),
		meta:     parsed.Query(),
	}

	client.meta.Add("_pragma", "journal_mode(WAL)")
	client.meta.Add("_pragma", "busy_timeout(5000)")
	client.meta.Add("_pragma", "foreign_keys(1)")

	return client, nil
}
