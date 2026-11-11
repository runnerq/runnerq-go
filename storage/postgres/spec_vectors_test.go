package postgres

import (
	"slices"
	"testing"

	"github.com/runnerq/runnerq-go/internal/spectest"
)

func TestSpecCanonicalStatus(t *testing.T) {
	type in struct {
		Status string `json:"status"`
	}
	for _, c := range spectest.Load[in, string](t, "vectors/canonical_status.json") {
		if got := canonicalStatus(c.Input.Status); got != c.Output {
			t.Errorf("%s: got %s, want %s", c.Name, got, c.Output)
		}
	}
}

func TestSpecCanonicalEvent(t *testing.T) {
	type in struct {
		EventType string `json:"event_type"`
	}
	cases := spectest.Load[in, string](t, "vectors/canonical_event.json")
	for _, c := range cases {
		if got := canonicalEvent(c.Input.EventType); got != c.Output {
			t.Errorf("%s: got %s, want %s", c.Name, got, c.Output)
		}
	}
	// The vectors are the whole table: nothing maps here that they don't list.
	listed := map[string]bool{}
	for _, c := range cases {
		listed[c.Input.EventType] = true
	}
	for internal := range canonicalEvents {
		if !listed[internal] {
			t.Errorf("event %s is mapped but not in the spec", internal)
		}
	}
}

func TestSpecInternalEvents(t *testing.T) {
	type in struct {
		Type string `json:"type"`
	}
	for _, c := range spectest.Load[in, []string](t, "vectors/internal_events.json") {
		if got := internalEvents(c.Input.Type); !slices.Equal(got, c.Output) {
			t.Errorf("%s: got %v, want %v", c.Name, got, c.Output)
		}
	}
}
