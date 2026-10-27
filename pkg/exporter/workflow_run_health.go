package exporter

import (
	"maps"
	"slices"
	"sync"
)

const (
	// WorkflowRunHealthHistoryReady is true when the six-hour history is available.
	WorkflowRunHealthHistoryReady = "history_ready"
	// WorkflowRunHealthDatabaseError is true when a database operation failed.
	WorkflowRunHealthDatabaseError = "database_error"
	// WorkflowRunHealthSeriesLimitExceeded is true after the persistent registry reaches its cap.
	WorkflowRunHealthSeriesLimitExceeded = "series_limit_exceeded"
	// WorkflowRunHealthRecoveryEnabled is true when delivery recovery is enabled.
	WorkflowRunHealthRecoveryEnabled = "recovery_enabled"
	// WorkflowRunHealthRecoveryPermissionError is true when GitHub denied recovery access.
	WorkflowRunHealthRecoveryPermissionError = "recovery_permission_error"
	// WorkflowRunHealthRecoveryRateLimit is true when GitHub rate-limited recovery requests.
	WorkflowRunHealthRecoveryRateLimit = "recovery_rate_limit"
	// WorkflowRunHealthRecoveryPaginationTrunc is true when a delivery scan hit its page limit or a repository search was incomplete.
	WorkflowRunHealthRecoveryPaginationTrunc = "recovery_pagination_truncated"
	// WorkflowRunHealthRecoveryError is true when recovery failed for another reason.
	WorkflowRunHealthRecoveryError = "recovery_error"
)

var workflowRunHealthConditions = []string{
	WorkflowRunHealthHistoryReady,
	WorkflowRunHealthDatabaseError,
	WorkflowRunHealthSeriesLimitExceeded,
	WorkflowRunHealthRecoveryEnabled,
	WorkflowRunHealthRecoveryPermissionError,
	WorkflowRunHealthRecoveryRateLimit,
	WorkflowRunHealthRecoveryPaginationTrunc,
	WorkflowRunHealthRecoveryError,
}

// WorkflowRunHealth stores bounded health states shared by the collector and recovery worker.
type WorkflowRunHealth struct {
	mu     sync.RWMutex
	states map[string]bool
	// databaseErrors is keyed by component so one component's success does
	// not hide another component's failure before the next scrape.
	databaseErrors map[string]bool
}

// NewWorkflowRunHealth initializes all fixed health states.
func NewWorkflowRunHealth(recoveryEnabled bool) *WorkflowRunHealth {
	states := make(map[string]bool, len(workflowRunHealthConditions))
	for _, condition := range workflowRunHealthConditions {
		states[condition] = false
	}
	states[WorkflowRunHealthRecoveryEnabled] = recoveryEnabled

	return &WorkflowRunHealth{states: states, databaseErrors: make(map[string]bool)}
}

// Set updates a known health state. Use SetDatabaseError for database_error.
func (h *WorkflowRunHealth) Set(condition string, value bool) {
	if h == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.states[condition]; ok {
		h.states[condition] = value
	}
}

// SetDatabaseError records whether the latest database operation of a component failed.
func (h *WorkflowRunHealth) SetDatabaseError(component string, failed bool) {
	if h == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	h.databaseErrors[component] = failed
}

// Snapshot returns the fixed condition values in stable order.
func (h *WorkflowRunHealth) Snapshot() []struct {
	Condition string
	Value     float64
} {
	if h == nil {
		h = NewWorkflowRunHealth(false)
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]struct {
		Condition string
		Value     float64
	}, 0, len(workflowRunHealthConditions))
	for _, condition := range workflowRunHealthConditions {
		active := h.states[condition]
		if condition == WorkflowRunHealthDatabaseError {
			active = slices.Contains(slices.Collect(maps.Values(h.databaseErrors)), true)
		}
		value := 0.0
		if active {
			value = 1
		}
		result = append(result, struct {
			Condition string
			Value     float64
		}{Condition: condition, Value: value})
	}

	return result
}
