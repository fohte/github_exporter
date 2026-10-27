package exporter

import "sync"

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
	// WorkflowRunHealthRecoveryPaginationTrunc is true when a delivery scan hit its page limit.
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
}

// NewWorkflowRunHealth initializes all fixed health states.
func NewWorkflowRunHealth(recoveryEnabled bool) *WorkflowRunHealth {
	states := make(map[string]bool, len(workflowRunHealthConditions))
	for _, condition := range workflowRunHealthConditions {
		states[condition] = false
	}
	states[WorkflowRunHealthRecoveryEnabled] = recoveryEnabled

	return &WorkflowRunHealth{states: states}
}

// Set updates a known health state.
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
		value := 0.0
		if h.states[condition] {
			value = 1
		}
		result = append(result, struct {
			Condition string
			Value     float64
		}{Condition: condition, Value: value})
	}

	return result
}
