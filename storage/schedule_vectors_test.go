package storage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/runnerq/runnerq-go/internal/cron"
	"github.com/runnerq/runnerq-go/internal/spectest"
)

func TestSpecCronNextFire(t *testing.T) {
	type in struct {
		Cron     string `json:"cron"`
		Timezone string `json:"timezone"`
		From     string `json:"from"`
		Count    int    `json:"count"`
	}
	for _, c := range spectest.Load[in, json.RawMessage](t, "vectors/cron_next_fire.json") {
		var got any = "invalid"
		e, err := cron.Parse(c.Input.Cron)
		loc, zerr := cron.LoadZone(c.Input.Timezone)
		if err == nil && zerr == nil {
			from, _ := time.Parse(time.RFC3339Nano, c.Input.From)
			out := []string{}
			for range c.Input.Count {
				next, ok := e.Next(from, loc)
				if !ok {
					break
				}
				out = append(out, next.Format(time.RFC3339))
				from = next
			}
			got = out
		}
		if g, _ := json.Marshal(got); !jsonEqual(g, c.Output) {
			t.Errorf("%s: got %s, want %s", c.Name, g, c.Output)
		}
	}
}

func TestSpecScheduleFire(t *testing.T) {
	type in struct {
		Cron       string `json:"cron"`
		Timezone   string `json:"timezone"`
		NextFireAt string `json:"next_fire_at"`
		Now        string `json:"now"`
		CatchUp    string `json:"catch_up"`
		Limit      int    `json:"limit"`
	}
	type out struct {
		Fire       []string `json:"fire"`
		NextFireAt string   `json:"next_fire_at"`
	}
	for _, c := range spectest.Load[in, out](t, "vectors/schedule_fire.json") {
		due, _ := time.Parse(time.RFC3339Nano, c.Input.NextFireAt)
		now, _ := time.Parse(time.RFC3339Nano, c.Input.Now)
		s := &Schedule{Cron: c.Input.Cron, Timezone: c.Input.Timezone, NextFireAt: due, CatchUp: CatchUp(c.Input.CatchUp), CatchUpLimit: c.Input.Limit}
		fire, next, err := ScheduleFireTimes(s, now)
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		got := out{Fire: []string{}, NextFireAt: next.UTC().Format(time.RFC3339Nano)}
		for _, f := range fire {
			got.Fire = append(got.Fire, f.Format(time.RFC3339))
		}
		g, _ := json.Marshal(got)
		w, _ := json.Marshal(c.Output)
		if !jsonEqual(g, w) {
			t.Errorf("%s: got %s, want %s", c.Name, g, w)
		}
	}
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	ax, _ := json.Marshal(x)
	by, _ := json.Marshal(y)
	return string(ax) == string(by)
}
