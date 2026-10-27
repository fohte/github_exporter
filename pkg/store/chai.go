//go:build chai

package store

import (
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"time"

	"github.com/GuiaBolso/darwin"
	"github.com/google/go-github/v84/github"
	"github.com/jmoiron/sqlx"
	"github.com/promhippie/github_exporter/pkg/migration/dialect"

	// Import Chai driver for database/sql
	_ "github.com/chaisql/chai"
)

var (
	chaiMigrations = []darwin.Migration{
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
				identifier INTEGER,
				created_at INTEGER,
				updated_at INTEGER,
				started_at INTEGER,
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
				created_at INTEGER,
				started_at INTEGER,
				completed_at INTEGER,
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
				cursor TEXT NOT NULL,
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

func init() {
	register("chai", NewChaiStore)
	register("genji", NewChaiStore)
}

// chaiStore implements the Store interface for Chai.
type chaiStore struct {
	logger   *slog.Logger
	driver   string
	database string
	meta     url.Values
	handle   *sqlx.DB
}

// Open simply opens the database connection.
func (s *chaiStore) Open() (res bool, err error) {
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
func (s *chaiStore) Close() error {
	return s.handle.Close()
}

// Ping just tests the database connection.
func (s *chaiStore) Ping() (bool, error) {
	if err := s.handle.Ping(); err != nil {
		return false, err
	}

	return true, nil
}

// Migrate executes required db migrations.
func (s *chaiStore) Migrate() error {
	driver := darwin.New(
		darwin.NewGenericDriver(
			s.handle.DB,
			dialect.ChaiDialect{},
		),
		chaiMigrations,
		nil,
	)

	if err := driver.Migrate(); err != nil {
		return fmt.Errorf("failed to exec migrations: %w", err)
	}

	return nil
}

// StoreWorkflowRunEvent implements the Store interface.
func (s *chaiStore) StoreWorkflowRunEvent(event *github.WorkflowRunEvent) error {
	return storeWorkflowRunEvent(s.handle, event)
}

// GetWorkflowRuns implements the Store interface.
func (s *chaiStore) GetWorkflowRuns(window time.Duration) ([]*WorkflowRun, error) {
	return getWorkflowRuns(s.handle, window)
}

// PruneWorkflowRuns implements the Store interface.
func (s *chaiStore) PruneWorkflowRuns(timeframe time.Duration) error {
	return pruneWorkflowRuns(s.handle, timeframe)
}

func (s *chaiStore) StoreWorkflowRunAttemptEvent(event *github.WorkflowRunEvent) error {
	return storeWorkflowRunAttemptEvent(s.handle, event)
}

func (s *chaiStore) GetWorkflowRunAttemptSnapshot(now time.Time) (*WorkflowRunAttemptSnapshot, error) {
	return getWorkflowRunAttemptSnapshot(s.handle, now)
}

func (s *chaiStore) PruneWorkflowRunAttempts(retention time.Duration) error {
	return pruneWorkflowRunAttempts(s.handle, retention)
}

func (s *chaiStore) GetWorkflowRunRecoveryCheckpoint(owner, repo string, hookID int64) (WorkflowRunRecoveryCheckpoint, error) {
	return getWorkflowRunRecoveryCheckpoint(s.handle, owner, repo, hookID)
}

func (s *chaiStore) SaveWorkflowRunRecoveryCheckpoint(checkpoint WorkflowRunRecoveryCheckpoint) error {
	return saveWorkflowRunRecoveryCheckpoint(s.handle, checkpoint)
}

func (s *chaiStore) ReserveWorkflowRunRecoveryDelivery(owner, repo string, hookID int64, guid string, deliveryID int64, attemptedAt time.Time) (bool, error) {
	return reserveWorkflowRunRecoveryDelivery(s.handle, owner, repo, hookID, guid, deliveryID, attemptedAt)
}

func (s *chaiStore) MarkWorkflowRunRecoveryDelivery(owner, repo string, hookID int64, guid string, deliveryID int64, at time.Time, status string) error {
	return markWorkflowRunRecoveryDelivery(s.handle, owner, repo, hookID, guid, deliveryID, at, status)
}

// StoreWorkflowJobEvent implements the Store interface.
func (s *chaiStore) StoreWorkflowJobEvent(event *github.WorkflowJobEvent) error {
	return storeWorkflowJobEvent(s.handle, event)
}

// GetWorkflowJobs implements the Store interface.
func (s *chaiStore) GetWorkflowJobs(window time.Duration) ([]*WorkflowJob, error) {
	return getWorkflowJobs(s.handle, window)
}

// PruneWorkflowJobs implements the Store interface.
func (s *chaiStore) PruneWorkflowJobs(timeframe time.Duration) error {
	return pruneWorkflowJobs(s.handle, timeframe)
}

func (s *chaiStore) dsn() string {
	if len(s.meta) > 0 {
		return fmt.Sprintf(
			"%s?%s",
			s.database,
			s.meta.Encode(),
		)
	}

	return s.database
}

// NewChaiStore initializes a new MySQL store.
func NewChaiStore(dsn string, logger *slog.Logger) (Store, error) {
	parsed, err := url.Parse(dsn)

	if err != nil {
		return nil, fmt.Errorf("failed to parse dsn: %w", err)
	}

	client := &chaiStore{
		logger:   logger,
		driver:   "chai",
		database: path.Join(parsed.Host, parsed.Path),
		meta:     parsed.Query(),
	}

	return client, nil
}
