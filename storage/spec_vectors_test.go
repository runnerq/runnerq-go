package storage

import (
	"testing"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/internal/spectest"
)

func TestSpecCheckpointID(t *testing.T) {
	type in struct {
		ActivityID string `json:"activity_id"`
		Kind       string `json:"kind"`
		Name       string `json:"name"`
	}
	for _, c := range spectest.Load[in, string](t, "vectors/checkpoint_id.json") {
		if got := CheckpointID(uuid.MustParse(c.Input.ActivityID), c.Input.Kind, c.Input.Name).String(); got != c.Output {
			t.Errorf("%s: got %s, want %s", c.Name, got, c.Output)
		}
	}
}

func TestSpecBusinessKey(t *testing.T) {
	type in struct {
		Key          string `json:"key"`
		ActivityType string `json:"activity_type"`
	}
	for _, c := range spectest.Load[in, string](t, "vectors/business_key.json") {
		if got := BusinessIdempotencyKey(c.Input.Key, c.Input.ActivityType); got != c.Output {
			t.Errorf("%s: got %s, want %s", c.Name, got, c.Output)
		}
	}
}

func TestSpecApplicationKey(t *testing.T) {
	type in struct {
		Stored       string `json:"stored"`
		ActivityType string `json:"activity_type"`
	}
	for _, c := range spectest.Load[in, string](t, "vectors/application_key.json") {
		if got := ApplicationIdempotencyKey(c.Input.Stored, c.Input.ActivityType); got != c.Output {
			t.Errorf("%s: got %q, want %q", c.Name, got, c.Output)
		}
	}
}

func TestSpecStepKey(t *testing.T) {
	type in struct {
		RootID   string `json:"root_id"`
		ParentID string `json:"parent_id"`
		Step     string `json:"step"`
	}
	for _, c := range spectest.Load[in, string](t, "vectors/step_key.json") {
		if got := StepIdempotencyKey(uuid.MustParse(c.Input.RootID), uuid.MustParse(c.Input.ParentID), c.Input.Step); got != c.Output {
			t.Errorf("%s: got %s, want %s", c.Name, got, c.Output)
		}
	}
}

func TestSpecAttemptsRemain(t *testing.T) {
	type in struct {
		RetryCount int `json:"retry_count"`
		MaxRetries int `json:"max_retries"`
	}
	for _, c := range spectest.Load[in, bool](t, "vectors/attempts_remain.json") {
		if got := AttemptsRemain(c.Input.RetryCount, c.Input.MaxRetries); got != c.Output {
			t.Errorf("%s: got %v, want %v", c.Name, got, c.Output)
		}
	}
}

func TestSpecRetryDelay(t *testing.T) {
	type in struct {
		RetryCount    int   `json:"retry_count"`
		RetryDelay    int64 `json:"retry_delay_seconds"`
		MaxRetryDelay int64 `json:"max_retry_delay_seconds"`
	}
	for _, c := range spectest.Load[in, int64](t, "vectors/retry_delay.json") {
		if got := RetryDelaySeconds(c.Input.RetryCount, c.Input.RetryDelay, c.Input.MaxRetryDelay); got != c.Output {
			t.Errorf("%s: got %d, want %d", c.Name, got, c.Output)
		}
	}
}
