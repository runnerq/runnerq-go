package storagetest

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/runnerq/runnerq-go/storage"
)

// ScheduleClock is an optional Harness capability: moving a schedule's next
// fire time, so the suite can make ticks due without waiting for them.
// Firing and catch-up tests are skipped without it.
type ScheduleClock interface {
	SetNextFireAt(b storage.Storage, name string, at time.Time) error
}

// Schedules (storage.ScheduleStorage, skipped when absent): upsert, list,
// pause, resume, delete, trigger, and firing each due tick once across
// concurrent handles, with every catch-up mode.
var scheduleTests = []conformanceTest{
	{"UpsertGetListDelete", testScheduleCRUD},
	{"UpsertKeepsStateUnlessCronChanges", testScheduleUpsertKeeps},
	{"PauseResumeTrigger", testSchedulePauseResume},
	{"DueTickFiresOnceAcrossHandles", testScheduleFiresOnce},
	{"CatchUpModes", testScheduleCatchUp},
}

func (s *suite) schedules() storage.ScheduleStorage {
	s.t.Helper()
	ss, ok := s.b.(storage.ScheduleStorage)
	if !ok {
		s.t.Skip("backend does not implement storage.ScheduleStorage")
	}
	return ss
}

func (s *suite) clock() ScheduleClock {
	s.t.Helper()
	c, ok := s.h.(ScheduleClock)
	if !ok {
		s.t.Skip("harness does not implement storagetest.ScheduleClock")
	}
	return c
}

func schedule(name, cron string) storage.Schedule {
	return storage.Schedule{
		Name: name, Cron: cron, ActivityType: "scheduled", Payload: json.RawMessage(`{"n":1}`),
		Options: storage.ScheduleOptions{Priority: storage.PriorityHigh, MaxRetries: 4, TimeoutSeconds: 30, RetryDelaySeconds: 1, Metadata: map[string]string{"k": "v"}},
	}
}

func (s *suite) getSchedule(name string) *storage.Schedule {
	s.t.Helper()
	got, err := s.schedules().GetSchedule(s.ctx, name)
	if err != nil {
		s.t.Fatalf("GetSchedule(%s): %v", name, err)
	}
	return got
}

func (s *suite) upsert(sc storage.Schedule) {
	s.t.Helper()
	if err := s.schedules().UpsertSchedule(s.ctx, sc); err != nil {
		s.t.Fatalf("UpsertSchedule(%s): %v", sc.Name, err)
	}
}

func (s *suite) fire(b storage.Storage) int {
	s.t.Helper()
	n, err := b.(storage.ScheduleStorage).FireDueSchedules(s.ctx, 100)
	if err != nil {
		s.t.Fatalf("FireDueSchedules: %v", err)
	}
	return n
}

func isKind(err error, kind storage.StorageErrorKind) bool {
	var se *storage.StorageError
	return errors.As(err, &se) && se.Kind == kind
}

func testScheduleCRUD(t *testing.T, h Harness) {
	s := newSuite(t, h)
	ss := s.schedules()
	s.upsert(schedule("b", "@hourly"))
	s.upsert(schedule("a", "0 9 * * 1-5"))
	got := s.getSchedule("a")
	if got.Timezone != "UTC" || got.CatchUp != storage.CatchUpOne || got.CatchUpLimit != storage.DefaultCatchUpLimit || got.Paused || got.LastFireAt != nil {
		t.Fatalf("defaults: %+v", got)
	}
	if !got.NextFireAt.After(time.Now().Add(-time.Second)) || got.Options.Priority != storage.PriorityHigh || got.Options.Metadata["k"] != "v" || string(got.Payload) != `{"n": 1}` && string(got.Payload) != `{"n":1}` {
		t.Fatalf("stored: %+v", got)
	}
	list, err := ss.ListSchedules(s.ctx)
	if err != nil || len(list) != 2 || list[0].Name != "a" || list[1].Name != "b" {
		t.Fatalf("ListSchedules = %v, %v", list, err)
	}
	for _, bad := range []storage.Schedule{schedule("x", "61 * * * *"), {Name: "x", Cron: "@daily", Timezone: "Mars/Olympus_Mons", ActivityType: "t"}, {Name: "x", Cron: "@daily"}, {Name: "x", Cron: "@daily", ActivityType: "t", CatchUp: "some"}} {
		if err := ss.UpsertSchedule(s.ctx, bad); !isKind(err, storage.ErrInvalidArgument) {
			t.Errorf("UpsertSchedule(%+v) = %v, want invalid argument", bad, err)
		}
	}
	if err := ss.DeleteSchedule(s.ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := ss.GetSchedule(s.ctx, "b"); !isKind(err, storage.ErrNotFound) {
		t.Fatalf("GetSchedule after delete = %v", err)
	}
	for name, err := range map[string]error{
		"delete":  ss.DeleteSchedule(s.ctx, "b"),
		"pause":   ss.PauseSchedule(s.ctx, "b"),
		"resume":  ss.ResumeSchedule(s.ctx, "b"),
		"trigger": func() error { _, err := ss.TriggerSchedule(s.ctx, "b"); return err }(),
	} {
		if !isKind(err, storage.ErrNotFound) {
			t.Errorf("%s of a deleted schedule = %v, want not found", name, err)
		}
	}
}

func testScheduleUpsertKeeps(t *testing.T, h Harness) {
	s := newSuite(t, h)
	c := s.clock()
	ss := s.schedules()
	s.upsert(schedule("s", "@hourly"))
	if err := ss.PauseSchedule(s.ctx, "s"); err != nil {
		t.Fatal(err)
	}
	past := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	if err := c.SetNextFireAt(s.b, "s", past); err != nil {
		t.Fatal(err)
	}
	changed := schedule("s", "@hourly")
	changed.Payload = json.RawMessage(`{"n":2}`)
	s.upsert(changed)
	got := s.getSchedule("s")
	if !got.Paused || !got.NextFireAt.Equal(past) {
		t.Fatalf("after upsert with the same cron: paused %v, next %v, want true, %v", got.Paused, got.NextFireAt, past)
	}
	changed.Cron = "30 * * * *"
	s.upsert(changed)
	got = s.getSchedule("s")
	if !got.Paused || !got.NextFireAt.After(time.Now()) || got.NextFireAt.Minute() != 30 {
		t.Fatalf("after changing the cron: paused %v, next %v", got.Paused, got.NextFireAt)
	}
}

func testSchedulePauseResume(t *testing.T, h Harness) {
	s := newSuite(t, h)
	c := s.clock()
	ss := s.schedules()
	s.upsert(schedule("s", "@hourly"))
	if err := ss.PauseSchedule(s.ctx, "s"); err != nil {
		t.Fatal(err)
	}
	if err := c.SetNextFireAt(s.b, "s", time.Now().Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n := s.fire(s.b); n != 0 {
		t.Fatalf("a paused schedule fired %d", n)
	}
	id, err := ss.TriggerSchedule(s.ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if snap := s.snapshot(id); snap.ActivityType != "scheduled" || snap.Priority != storage.PriorityHigh || snap.MaxRetries != 4 {
		t.Fatalf("triggered activity: %+v", snap)
	}
	if err := ss.ResumeSchedule(s.ctx, "s"); err != nil {
		t.Fatal(err)
	}
	got := s.getSchedule("s")
	if got.Paused || !got.NextFireAt.After(time.Now()) || got.LastFireAt != nil {
		t.Fatalf("after resume: %+v", got)
	}
	if n := s.fire(s.b); n != 0 {
		t.Fatalf("resume caught up %d ticks", n)
	}
}

func testScheduleFiresOnce(t *testing.T, h Harness) {
	s := newSuite(t, h)
	c := s.clock()
	s.upsert(schedule("s", "@hourly"))
	tick := time.Now().UTC().Truncate(time.Hour)
	if err := c.SetNextFireAt(s.b, "s", tick); err != nil {
		t.Fatal(err)
	}
	handles := []storage.Storage{s.b, s.another(), s.another()}
	var mu sync.Mutex
	total := 0
	var wg sync.WaitGroup
	for _, b := range handles {
		for range 3 {
			wg.Go(func() {
				n, err := b.(storage.ScheduleStorage).FireDueSchedules(s.ctx, 100)
				if err != nil {
					t.Error(err)
				}
				mu.Lock()
				total += n
				mu.Unlock()
			})
		}
	}
	wg.Wait()
	if total != 1 {
		t.Fatalf("one due tick fired %d activities", total)
	}
	key := storage.BusinessIdempotencyKey(storage.ScheduleIdempotencyKey("s", tick), "scheduled")
	id, err := s.b.LookupIdempotencyActivityID(s.ctx, key)
	if err != nil {
		t.Fatalf("no activity under %s: %v", key, err)
	}
	snap := s.snapshot(id)
	if snap.ScheduledAt != nil || snap.ParentActivityID != nil || snap.Priority != storage.PriorityHigh || snap.Metadata["k"] != "v" {
		t.Fatalf("fired activity: %+v", snap)
	}
	got := s.getSchedule("s")
	if got.LastFireAt == nil || !got.LastFireAt.Equal(tick) || !got.NextFireAt.Equal(tick.Add(time.Hour)) {
		t.Fatalf("after firing: last %v next %v", got.LastFireAt, got.NextFireAt)
	}
	// A lost advance: the same tick due again enqueues nothing.
	if err := c.SetNextFireAt(s.b, "s", tick); err != nil {
		t.Fatal(err)
	}
	if n := s.fire(s.b); n != 0 {
		t.Fatalf("a tick fired twice (%d)", n)
	}
}

func testScheduleCatchUp(t *testing.T, h Harness) {
	s := newSuite(t, h)
	c := s.clock()
	year := time.Date(time.Now().UTC().Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		mode  storage.CatchUp
		limit int
		want  int
	}{{storage.CatchUpSkip, 0, 0}, {storage.CatchUpOne, 0, 1}, {storage.CatchUpAll, 0, 6}, {storage.CatchUpAll, 2, 2}} {
		sc := schedule(string(tc.mode)+string(rune('0'+tc.limit)), "@yearly")
		sc.CatchUp, sc.CatchUpLimit = tc.mode, tc.limit
		s.upsert(sc)
		if err := c.SetNextFireAt(s.b, sc.Name, year.AddDate(-5, 0, 0)); err != nil {
			t.Fatal(err)
		}
		if n := s.fire(s.b); n != tc.want {
			t.Errorf("catch_up %s limit %d fired %d, want %d", tc.mode, tc.limit, n, tc.want)
		}
		got := s.getSchedule(sc.Name)
		if !got.NextFireAt.Equal(year.AddDate(1, 0, 0)) {
			t.Errorf("catch_up %s: next %v", tc.mode, got.NextFireAt)
		}
		if tc.want > 0 && (got.LastFireAt == nil || !got.LastFireAt.Equal(year)) {
			t.Errorf("catch_up %s: last %v, want %v", tc.mode, got.LastFireAt, year)
		}
	}
}
