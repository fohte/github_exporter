//go:build sqlite

package action

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/go-github/v84/github"
	"github.com/promhippie/github_exporter/pkg/config"
	"github.com/promhippie/github_exporter/pkg/exporter"
	"github.com/promhippie/github_exporter/pkg/store"
)

func TestWorkflowRunRecoveryPaginatesMatchingHookAndRedeliversOnce(t *testing.T) {
	var (
		mu               sync.Mutex
		redelivered      = make(map[string]int)
		deliveryListCall int
	)
	var server *httptest.Server
	callbackURL := ""
	deliveredAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/example-org/sample-repo":
			writeJSON(t, w, map[string]any{
				"id":        1,
				"name":      "sample-repo",
				"full_name": "example-org/sample-repo",
				"owner":     map[string]string{"login": "example-org"},
			})
		case "/repos/example-org/sample-repo/hooks":
			writeJSON(t, w, []map[string]any{
				{"id": 11, "active": true, "events": []string{"workflow_run"}, "config": map[string]string{"url": callbackURL}},
				{"id": 12, "active": true, "events": []string{"workflow_run"}, "config": map[string]string{"url": "https://unrelated.example.invalid/hook"}},
				{"id": 13, "active": false, "events": []string{"workflow_run"}, "config": map[string]string{"url": callbackURL}},
				{"id": 14, "active": true, "events": []string{"push"}, "config": map[string]string{"url": callbackURL}},
			})
		case "/repos/example-org/sample-repo/hooks/11/deliveries":
			mu.Lock()
			deliveryListCall++
			mu.Unlock()
			if r.URL.Query().Get("cursor") == "next-cursor" {
				writeJSON(t, w, []map[string]any{
					{"id": 202, "guid": "delivery-second", "delivered_at": deliveredAt, "status": "failed", "event": "workflow_run"},
				})
				return
			}
			w.Header().Set("Link", "<"+server.URL+"/repos/example-org/sample-repo/hooks/11/deliveries?cursor=next-cursor>; rel=\"next\"")
			writeJSON(t, w, []map[string]any{
				{"id": 201, "guid": "delivery-first", "delivered_at": deliveredAt, "status": "failed", "event": "workflow_run"},
				{"id": 203, "guid": "delivery-push", "delivered_at": deliveredAt, "status": "failed", "event": "push"},
				{"id": 204, "guid": "delivery-ok", "delivered_at": deliveredAt, "status": "OK", "event": "workflow_run"},
			})
		case "/repos/example-org/sample-repo/hooks/11/deliveries/201/attempts",
			"/repos/example-org/sample-repo/hooks/11/deliveries/202/attempts":
			mu.Lock()
			redelivered[r.URL.Path]++
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	callbackURL = server.URL + "/webhook"

	db := newRecoveryTestStore(t)
	client := newRecoveryTestClient(t, server)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	health := exporter.NewWorkflowRunHealth(true)
	cfg := config.Config{
		Target: config.Target{
			Repos:   []string{"example-org/sample-repo"},
			Timeout: time.Second,
			WorkflowRuns: config.WorkflowRuns{Recovery: config.WorkflowRunRecovery{
				Enabled:     true,
				Interval:    time.Minute,
				CallbackURL: callbackURL,
				MaxPages:    1,
			}},
		},
	}
	recovery := workflowRunRecovery{config: cfg, db: db, logger: logger, client: client, health: health}

	recovery.runCycle(context.Background())
	checkpoint, err := db.GetWorkflowRunRecoveryCheckpoint("example-org", "sample-repo", 11)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Cursor != "next-cursor" || checkpoint.ScanStartedAt.IsZero() {
		t.Fatalf("checkpoint after first page = %#v, want resumable cursor", checkpoint)
	}
	if healthValue(health, exporter.WorkflowRunHealthRecoveryPaginationTrunc) != 1 {
		t.Fatal("page limit was not reported as truncated")
	}

	recovery.runCycle(context.Background())
	checkpoint, err = db.GetWorkflowRunRecoveryCheckpoint("example-org", "sample-repo", 11)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Cursor != "" || !checkpoint.ScanStartedAt.IsZero() || checkpoint.LastCompletedAt.IsZero() {
		t.Fatalf("checkpoint during active scan = %#v, want completed cursor scan", checkpoint)
	}
	if healthValue(health, exporter.WorkflowRunHealthRecoveryPaginationTrunc) != 0 {
		t.Fatal("completed pagination remained marked truncated")
	}

	mu.Lock()
	if len(redelivered) != 2 || redelivered["/repos/example-org/sample-repo/hooks/11/deliveries/201/attempts"] != 1 || redelivered["/repos/example-org/sample-repo/hooks/11/deliveries/202/attempts"] != 1 {
		t.Fatalf("redelivery calls = %#v, want one call for each matching failed delivery", redelivered)
	}
	if deliveryListCall != 2 {
		t.Fatalf("delivery list calls = %d, want 2", deliveryListCall)
	}
	mu.Unlock()

	reserved, err := db.ReserveWorkflowRunRecoveryDelivery("example-org", "sample-repo", 11, "delivery-first", 201, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if reserved {
		t.Fatal("completed delivery was reserved for another redelivery")
	}

	recovery.runCycle(context.Background())
	mu.Lock()
	defer mu.Unlock()
	if len(redelivered) != 2 {
		t.Fatalf("redelivery calls after repeated scan = %#v, want no duplicate calls", redelivered)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Errorf("encode test response: %v", err)
	}
}

func healthValue(health *exporter.WorkflowRunHealth, condition string) float64 {
	for _, state := range health.Snapshot() {
		if state.Condition == condition {
			return state.Value
		}
	}
	return -1
}

func TestWorkflowRunRecoveryStopsBeforeReservingWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var (
		mu          sync.Mutex
		redelivered []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/example-org/sample-repo/hooks/11/deliveries":
			writeJSON(t, w, []map[string]any{
				{"id": 301, "guid": "delivery-canceling", "status": "failed", "event": "workflow_run"},
				{"id": 302, "guid": "delivery-after-cancel", "status": "failed", "event": "workflow_run"},
			})
		case "/repos/example-org/sample-repo/hooks/11/deliveries/301/attempts",
			"/repos/example-org/sample-repo/hooks/11/deliveries/302/attempts":
			mu.Lock()
			redelivered = append(redelivered, r.URL.Path)
			mu.Unlock()
			cancel()
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	db := newRecoveryTestStore(t)
	recovery := workflowRunRecovery{
		config: config.Config{Target: config.Target{Timeout: time.Second}},
		db:     db,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		client: newRecoveryTestClient(t, server),
		health: exporter.NewWorkflowRunHealth(true),
	}

	recovery.recoverHook(ctx, "example-org", "sample-repo", 11)

	mu.Lock()
	defer mu.Unlock()
	if len(redelivered) != 1 {
		t.Fatalf("redelivery calls = %#v, want only the call that canceled recovery", redelivered)
	}
	reserved, err := db.ReserveWorkflowRunRecoveryDelivery("example-org", "sample-repo", 11, "delivery-after-cancel", 302, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !reserved {
		t.Fatal("delivery after cancellation was reserved and can no longer be redelivered")
	}
	checkpoint, err := db.GetWorkflowRunRecoveryCheckpoint("example-org", "sample-repo", 11)
	if err != nil {
		t.Fatal(err)
	}
	if !checkpoint.LastCompletedAt.IsZero() {
		t.Fatalf("checkpoint after cancellation = %#v, want the interrupted page to be scanned again", checkpoint)
	}
}

func TestWorkflowRunRecoveryClassifiesGitHubAPIErrors(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		headers   map[string]string
		body      string
		condition string
	}{
		{
			name:      "permission denied",
			status:    http.StatusForbidden,
			body:      `{"message":"Resource not accessible by integration"}`,
			condition: exporter.WorkflowRunHealthRecoveryPermissionError,
		},
		{
			name:      "primary rate limit",
			status:    http.StatusForbidden,
			headers:   map[string]string{"X-RateLimit-Remaining": "0"},
			body:      `{"message":"API rate limit exceeded"}`,
			condition: exporter.WorkflowRunHealthRecoveryRateLimit,
		},
		{
			name:      "secondary rate limit",
			status:    http.StatusTooManyRequests,
			body:      `{"message":"You have exceeded a secondary rate limit","documentation_url":"https://docs.github.com/rest/using-the-rest-api/rate-limits-for-the-rest-api#about-secondary-rate-limits"}`,
			condition: exporter.WorkflowRunHealthRecoveryRateLimit,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repos/example-org/sample-repo":
					writeJSON(t, w, map[string]any{
						"id":        1,
						"name":      "sample-repo",
						"full_name": "example-org/sample-repo",
						"owner":     map[string]string{"login": "example-org"},
					})
				case "/repos/example-org/sample-repo/hooks":
					for name, value := range tt.headers {
						w.Header().Set(name, value)
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tt.status)
					_, _ = io.WriteString(w, tt.body)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			health := exporter.NewWorkflowRunHealth(true)
			recovery := workflowRunRecovery{
				config: config.Config{Target: config.Target{
					Repos:        []string{"example-org/sample-repo"},
					Timeout:      time.Second,
					WorkflowRuns: config.WorkflowRuns{Recovery: config.WorkflowRunRecovery{CallbackURL: server.URL + "/webhook"}},
				}},
				logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
				client: newRecoveryTestClient(t, server),
				health: health,
			}

			recovery.runCycle(context.Background())

			for _, condition := range []string{
				exporter.WorkflowRunHealthRecoveryPermissionError,
				exporter.WorkflowRunHealthRecoveryRateLimit,
				exporter.WorkflowRunHealthRecoveryError,
			} {
				want := 0.0
				if condition == tt.condition {
					want = 1
				}
				if got := healthValue(health, condition); got != want {
					t.Errorf("%s = %v, want %v", condition, got, want)
				}
			}
		})
	}
}

func TestWorkflowRunRecoveryReportsRedeliveryThatFailedAgain(t *testing.T) {
	var redelivered int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/example-org/sample-repo/hooks/11/deliveries":
			writeJSON(t, w, []map[string]any{
				{"id": 402, "guid": "delivery-retried", "status": "failed", "event": "workflow_run", "redelivery": true},
			})
		case "/repos/example-org/sample-repo/hooks/11/deliveries/402/attempts":
			redelivered++
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	db := newRecoveryTestStore(t)
	if _, err := db.ReserveWorkflowRunRecoveryDelivery("example-org", "sample-repo", 11, "delivery-retried", 401, time.Now()); err != nil {
		t.Fatal(err)
	}
	health := exporter.NewWorkflowRunHealth(true)
	recovery := workflowRunRecovery{
		config: config.Config{Target: config.Target{Timeout: time.Second}},
		db:     db,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		client: newRecoveryTestClient(t, server),
		health: health,
	}

	recovery.recoverHook(context.Background(), "example-org", "sample-repo", 11)

	if redelivered != 0 {
		t.Fatalf("redelivery calls = %d, want no second redelivery of the same GUID", redelivered)
	}
	if got := healthValue(health, exporter.WorkflowRunHealthRecoveryError); got != 1 {
		t.Fatalf("recovery_error = %v, want 1 for a redelivery that failed again", got)
	}
}

func newRecoveryTestStore(t *testing.T) store.Store {
	t.Helper()
	db, err := store.NewSqliteStore("sqlite://"+filepath.Join(t.TempDir(), "recovery.sqlite"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Migrate(); err != nil {
		t.Fatal(err)
	}
	return db
}

func newRecoveryTestClient(t *testing.T, server *httptest.Server) *github.Client {
	t.Helper()
	client := github.NewClient(server.Client())
	baseURL, err := url.Parse(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	client.BaseURL = baseURL
	return client
}
