package conductor

import (
	"encoding/json"
	"testing"

	"github.com/runnerq/runnerq-go/internal/spectest"
	"github.com/runnerq/runnerq-go/storage"
)

func TestSpecPlainJSON(t *testing.T) {
	type in struct {
		Serialization string          `json:"serialization"`
		Data          json.RawMessage `json:"data"`
	}
	for _, c := range spectest.Load[in, json.RawMessage](t, "serialization/vectors/plain_json.json") {
		got := plainJSON(&storage.ActivityResult{Serialization: c.Input.Serialization, Data: c.Input.Data})
		if !spectest.SameJSON(got, c.Output) {
			t.Errorf("%s: got %s, want %s", c.Name, got, c.Output)
		}
	}
}
