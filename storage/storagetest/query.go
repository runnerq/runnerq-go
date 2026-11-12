package storagetest

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/runnerq/runnerq-go/storage"
)

// Query (storage.QueryStorage, skipped when absent): canonical statuses,
// filters, keyset paging, projections, counts, aggregates, events, steps and
// trees. Queries span every queue in the store, so each test scopes itself
// with a queue filter, which also proves that filter works.
var queryTests = []conformanceTest{
	{"CanonicalStatusesAndFields", testQueryCanonicalFields},
	{"FiltersCombine", testQueryFilters},
	{"IdempotencyKeysAreTheApplications", testQueryIdempotencyKeys},
	{"KeysetPagingIsStable", testQueryPaging},
	{"HeavyFieldsOnlyWhenIncluded", testQueryIncludes},
	{"RejectsWhatItCannotEvaluate", testQueryRejects},
	{"CountAndAggregate", testQueryCountAggregate},
	{"EventsAreCanonicalAndPaged", testQueryEvents},
	{"StepsAndTrees", testQueryStepsTrees},
}

func (s *suite) query() storage.QueryStorage {
	s.t.Helper()
	qs, ok := s.b.(storage.QueryStorage)
	if !ok {
		s.t.Skip("backend does not implement storage.QueryStorage")
	}
	return qs
}

func eq(field string, v any) storage.QueryFilter {
	return storage.QueryFilter{Field: field, Op: storage.OpEq, Value: v}
}

// scoped ANDs the suite's queue filter with terms.
func (s *suite) scoped(terms ...storage.QueryFilter) *storage.QueryFilter {
	return &storage.QueryFilter{And: append([]storage.QueryFilter{eq("queue", s.queue)}, terms...)}
}

func (s *suite) records(q storage.ActivityQuery) []storage.ActivityRecord {
	s.t.Helper()
	page, err := s.query().QueryActivities(s.ctx, q)
	if err != nil {
		s.t.Fatalf("query: %v", err)
	}
	return page.Items
}

func (s *suite) record(id uuid.UUID, inc storage.RecordInclude) storage.ActivityRecord {
	s.t.Helper()
	items := s.records(storage.ActivityQuery{Filter: s.scoped(eq("id", id.String())), Include: inc})
	if len(items) != 1 {
		s.t.Fatalf("activity %s: got %d records", id, len(items))
	}
	return items[0]
}

func recordIDs(items []storage.ActivityRecord) map[uuid.UUID]bool {
	out := make(map[uuid.UUID]bool, len(items))
	for _, r := range items {
		out[r.ID] = true
	}
	return out
}

func sameIDs(t *testing.T, what string, items []storage.ActivityRecord, want ...uuid.UUID) {
	t.Helper()
	got := recordIDs(items)
	if len(got) != len(want) {
		t.Fatalf("%s: got %d activities, want %d", what, len(got), len(want))
	}
	for _, id := range want {
		if !got[id] {
			t.Fatalf("%s: missing %s", what, id)
		}
	}
}

func wantStorageErr(t *testing.T, what string, err error, kind storage.StorageErrorKind, field string) {
	t.Helper()
	var se *storage.StorageError
	if !errors.As(err, &se) || se.Kind != kind || (field != "" && se.Field != field) {
		t.Fatalf("%s: got %v, want kind %d on field %q", what, err, kind, field)
	}
}

func testQueryCanonicalFields(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.query()
	pending := s.enqueue(activity(withType("pending")))
	scheduled := s.enqueue(activity(withScheduledAt(time.Now().UTC().Add(time.Hour))))
	running := s.enqueueClaimed("exec-1:worker-0:x", activity(withType("running")))
	waiting := s.enqueueClaimed("w", activity())
	if err := s.b.Yield(s.ctx, waiting.ID, time.Now().UTC().Add(time.Hour), "w", "sleep", "sleep:nap"); err != nil {
		t.Fatal(err)
	}
	retrying := s.enqueueClaimed("r", activity(withRetryDelay(3600, 0)))
	if _, err := s.b.AckFailure(s.ctx, retrying.ID, storage.NewRetryableFailure("later"), "r"); err != nil {
		t.Fatal(err)
	}
	completed := s.enqueueClaimed("c", activity())
	if err := s.b.AckSuccess(s.ctx, completed.ID, json.RawMessage(`{"ok":true}`), "c"); err != nil {
		t.Fatal(err)
	}
	failed := s.enqueueClaimed("f", activity())
	if _, err := s.b.AckFailure(s.ctx, failed.ID, storage.NewNonRetryableFailure("boom"), "f"); err != nil {
		t.Fatal(err)
	}

	for id, want := range map[uuid.UUID]string{
		pending.ID:   storage.RecordStatusPending,
		scheduled.ID: storage.RecordStatusScheduled,
		running.ID:   storage.RecordStatusRunning,
		waiting.ID:   storage.RecordStatusWaiting,
		retrying.ID:  storage.RecordStatusScheduled,
		completed.ID: storage.RecordStatusCompleted,
	} {
		if got := s.record(id, storage.RecordInclude{}).Status; got != want {
			t.Fatalf("activity %s: status %q, want %q", id, got, want)
		}
	}
	if st := s.record(failed.ID, storage.RecordInclude{}).Status; st != storage.RecordStatusFailed && st != storage.RecordStatusDeadLetter {
		t.Fatalf("non-retryable failure: status %q", st)
	}

	r := s.record(running.ID, storage.RecordInclude{})
	if r.ExecutorID != "exec-1" || r.LeaseExpiresAt == nil || r.StartedAt == nil || r.Attempt != 1 || r.MaxAttempts != 3 {
		t.Fatalf("running record %+v", r)
	}
	// MaxRetries is the total attempts allowed; 0 is unlimited.
	if unlimited := s.enqueue(activity(withMaxRetries(0))); s.record(unlimited.ID, storage.RecordInclude{}).MaxAttempts != 0 {
		t.Fatalf("unlimited activity reports max attempts %d", s.record(unlimited.ID, storage.RecordInclude{}).MaxAttempts)
	}
	if r.Type != "running" || r.Queue != s.queue || r.RootID != running.ID || r.ParentID != nil || r.Depth != 0 {
		t.Fatalf("identity fields %+v", r)
	}
	if rt := s.record(retrying.ID, storage.RecordInclude{}); rt.Attempt != 2 || rt.ExecutorID != "" {
		t.Fatalf("retrying record: attempt %d executor %q", rt.Attempt, rt.ExecutorID)
	}
	w := s.record(waiting.ID, storage.RecordInclude{})
	if w.Wait == nil || w.Wait.Kind != "sleep" || w.Wait.Name != "nap" || w.Wait.Until == nil {
		t.Fatalf("waiting record wait %+v", w.Wait)
	}
	if s.record(pending.ID, storage.RecordInclude{}).Wait != nil {
		t.Fatal("non-waiting activity has a wait")
	}
	c := s.record(completed.ID, storage.RecordInclude{})
	if c.CompletedAt == nil || c.UpdatedAt.Before(c.CreatedAt) {
		t.Fatalf("completed timestamps %+v", c)
	}
}

func testQueryFilters(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.query()
	running := s.enqueueClaimed("x", activity(withType("invoice")))
	parent := s.enqueue(activity(withType("order.create"), withPriority(storage.PriorityHigh), func(a *storage.QueuedActivity) {
		a.Metadata = map[string]string{"tenant": "acme"}
	}))
	child := s.enqueue(activity(withType("order.charge"), withParent(parent)))
	other := s.enqueue(activity(withType("invoice"), withKey("inv-1", storage.BehaviorReturnExisting)))

	all := func(terms ...storage.QueryFilter) []storage.ActivityRecord {
		return s.records(storage.ActivityQuery{Filter: s.scoped(terms...)})
	}
	sameIDs(t, "status in", all(storage.QueryFilter{Field: "status", Op: storage.OpIn, Value: []any{"pending"}}), parent.ID, child.ID, other.ID)
	sameIDs(t, "type prefix", all(storage.QueryFilter{Field: "type", Op: storage.OpPrefix, Value: "order."}), parent.ID, child.ID)
	sameIDs(t, "metadata", all(eq("metadata.tenant", "acme")), parent.ID)
	sameIDs(t, "roots only", all(storage.QueryFilter{Not: &storage.QueryFilter{Field: "parent_id", Op: storage.OpExists}},
		eq("status", "pending")), parent.ID, other.ID)
	sameIDs(t, "children of", all(eq("parent_id", parent.ID.String())), child.ID)
	sameIDs(t, "tree", all(eq("root_id", parent.ID.String())), parent.ID, child.ID)
	sameIDs(t, "idempotency key", all(eq("idempotency_key", "inv-1")), other.ID)
	sameIDs(t, "priority range", all(storage.QueryFilter{Field: "priority", Op: storage.OpGte, Value: float64(storage.PriorityHigh)}), parent.ID)
	sameIDs(t, "or", all(storage.QueryFilter{Or: []storage.QueryFilter{eq("type", "invoice"), eq("depth", float64(1))}},
		eq("status", "pending")), child.ID, other.ID)
	sameIDs(t, "ne", all(storage.QueryFilter{Field: "type", Op: storage.OpNe, Value: "invoice"}), parent.ID, child.ID)
	sameIDs(t, "nin", all(storage.QueryFilter{Field: "status", Op: storage.OpNin, Value: []any{"pending"}}), running.ID)
	sameIDs(t, "executor", all(eq("executor_id", "x")), running.ID)
	sameIDs(t, "created range", all(storage.QueryFilter{Field: "created_at", Op: storage.OpLt,
		Value: time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)}))
	sameIDs(t, "id that is not a backend id", all(eq("id", "not-a-uuid")))
}

// Records report, and filters match, the key the application set, however
// it is stored: a v2 business key, a legacy "<key>-<type>" key, or a key
// written directly. Keys the engine derives for a step's child are not the
// application's.
func testQueryIdempotencyKeys(t *testing.T, h Harness) {
	s := newSuite(t, h)
	qs := s.query()
	biz := s.enqueue(activity(withType("order.charge"), withKey(storage.BusinessIdempotencyKey("order-7", "order.charge"), storage.BehaviorReturnExisting)))
	legacy := s.enqueue(activity(withType("order.charge"), withKey("order-8-order.charge", storage.BehaviorReturnExisting)))
	raw := s.enqueue(activity(withType("invoice"), withKey("inv-9", storage.BehaviorReturnExisting)))
	step := s.enqueue(activity(withType("order.ship"), withKey(storage.StepKeyPrefix+"root:parent:ship", storage.BehaviorReturnExisting)))
	none := s.enqueue(activity(withType("invoice")))

	for id, want := range map[uuid.UUID]string{biz.ID: "order-7", legacy.ID: "order-8", raw.ID: "inv-9", step.ID: "", none.ID: ""} {
		if got := s.record(id, storage.RecordInclude{}).IdempotencyKey; got != want {
			t.Errorf("record %s: idempotency key %q, want %q", id, got, want)
		}
	}

	all := func(f storage.QueryFilter) []storage.ActivityRecord {
		return s.records(storage.ActivityQuery{Filter: s.scoped(f)})
	}
	key := func(op string, v any) storage.QueryFilter {
		return storage.QueryFilter{Field: "idempotency_key", Op: op, Value: v}
	}
	sameIDs(t, "eq business key", all(key(storage.OpEq, "order-7")), biz.ID)
	sameIDs(t, "eq legacy key", all(key(storage.OpEq, "order-8")), legacy.ID)
	sameIDs(t, "eq raw key", all(key(storage.OpEq, "inv-9")), raw.ID)
	sameIDs(t, "eq stored encoding", all(key(storage.OpEq, storage.BusinessIdempotencyKey("order-7", "order.charge"))))
	sameIDs(t, "in", all(key(storage.OpIn, []any{"order-7", "inv-9"})), biz.ID, raw.ID)
	sameIDs(t, "ne", all(key(storage.OpNe, "order-7")), legacy.ID, raw.ID, step.ID, none.ID)
	sameIDs(t, "nin", all(key(storage.OpNin, []any{"order-7", "order-8"})), raw.ID, step.ID, none.ID)
	sameIDs(t, "exists", all(key(storage.OpExists, true)), biz.ID, legacy.ID, raw.ID)
	sameIDs(t, "not exists", all(key(storage.OpExists, false)), step.ID, none.ID)

	_, err := qs.QueryActivities(s.ctx, storage.ActivityQuery{Filter: s.scoped(key(storage.OpPrefix, "order-"))})
	wantStorageErr(t, "prefix on an encoded key", err, storage.ErrUnsupported, "idempotency_key")
}

func testQueryPaging(t *testing.T, h Harness) {
	s := newSuite(t, h)
	qs := s.query()
	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	var want []uuid.UUID // newest first
	for i := range 7 {
		// Two activities share each timestamp so the id tiebreaker matters.
		a := s.enqueue(activity(withCreatedAt(base.Add(time.Duration(i/2) * time.Minute))))
		want = append([]uuid.UUID{a.ID}, want...)
	}

	walk := func(sort *storage.QuerySort) []storage.ActivityRecord {
		var out []storage.ActivityRecord
		cursor := ""
		for pages := 0; ; pages++ {
			page, err := qs.QueryActivities(s.ctx, storage.ActivityQuery{Filter: s.scoped(), Sort: sort, Limit: 3, Cursor: cursor})
			if err != nil {
				t.Fatalf("page %d: %v", pages, err)
			}
			out = append(out, page.Items...)
			if page.NextCursor == "" {
				return out
			}
			if pages > 5 {
				t.Fatal("paging never ends")
			}
			cursor = page.NextCursor
		}
	}

	desc := walk(nil)
	if len(desc) != 7 {
		t.Fatalf("paged %d activities, want 7", len(desc))
	}
	seen := map[uuid.UUID]bool{}
	for i := 1; i < len(desc); i++ {
		if desc[i].CreatedAt.After(desc[i-1].CreatedAt) {
			t.Fatalf("not newest first at %d", i)
		}
	}
	for _, r := range desc {
		if seen[r.ID] {
			t.Fatalf("activity %s returned twice", r.ID)
		}
		seen[r.ID] = true
	}

	asc := walk(&storage.QuerySort{Field: "created_at"})
	for i := range asc {
		if asc[i].ID != desc[len(desc)-1-i].ID {
			t.Fatalf("ascending order is not the reverse of descending at %d", i)
		}
	}

	page, err := qs.QueryActivities(s.ctx, storage.ActivityQuery{Filter: s.scoped(), Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	_, err = qs.QueryActivities(s.ctx, storage.ActivityQuery{Filter: s.scoped(), Limit: 3, Cursor: page.NextCursor,
		Sort: &storage.QuerySort{Field: "created_at"}})
	wantStorageErr(t, "cursor for another sort", err, storage.ErrInvalidArgument, "cursor")
	_, err = qs.QueryActivities(s.ctx, storage.ActivityQuery{Filter: s.scoped(), Cursor: "garbage"})
	wantStorageErr(t, "garbage cursor", err, storage.ErrInvalidArgument, "cursor")
}

func testQueryIncludes(t *testing.T, h Harness) {
	s := newSuite(t, h)
	s.query()
	done := s.enqueueClaimed("c", activity())
	if err := s.b.AckSuccess(s.ctx, done.ID, json.RawMessage(`{"n":42}`), "c"); err != nil {
		t.Fatal(err)
	}
	failed := s.enqueueClaimed("f", activity())
	if _, err := s.b.AckFailure(s.ctx, failed.ID, storage.NewNonRetryableFailure("kaput"), "f"); err != nil {
		t.Fatal(err)
	}

	bare := s.record(done.ID, storage.RecordInclude{})
	if bare.Payload != nil || bare.Result != nil || bare.LastError != nil {
		t.Fatalf("heavy fields without include: %+v", bare)
	}
	full := s.record(done.ID, storage.RecordInclude{Payload: true, Result: true, LastError: true})
	if string(full.Payload) == "" || full.Result == nil || full.Result.State != storage.ResultOk {
		t.Fatalf("included fields missing: payload %s result %+v", full.Payload, full.Result)
	}
	var res map[string]int
	if err := json.Unmarshal(full.Result.Data, &res); err != nil || res["n"] != 42 {
		t.Fatalf("result data %s", full.Result.Data)
	}
	f := s.record(failed.ID, storage.RecordInclude{LastError: true})
	if f.LastError == nil || f.LastError.Message == "" {
		t.Fatalf("last error not included: %+v", f.LastError)
	}
}

func testQueryRejects(t *testing.T, h Harness) {
	s := newSuite(t, h)
	qs := s.query()
	q := func(f storage.QueryFilter) error {
		_, err := qs.QueryActivities(s.ctx, storage.ActivityQuery{Filter: s.scoped(f)})
		return err
	}
	wantStorageErr(t, "unknown field", q(eq("colour", "red")), storage.ErrUnsupported, "colour")
	wantStorageErr(t, "unknown status", q(eq("status", "exploded")), storage.ErrInvalidArgument, "status")
	wantStorageErr(t, "wrong value type", q(eq("priority", "high")), storage.ErrInvalidArgument, "priority")
	wantStorageErr(t, "bad timestamp", q(storage.QueryFilter{Field: "created_at", Op: storage.OpGt, Value: "yesterday"}), storage.ErrInvalidArgument, "created_at")
	wantStorageErr(t, "unknown operator", q(storage.QueryFilter{Field: "type", Op: "sounds_like", Value: "x"}), storage.ErrUnsupported, "type")
	wantStorageErr(t, "range on a string", q(storage.QueryFilter{Field: "type", Op: storage.OpLt, Value: "x"}), storage.ErrUnsupported, "type")
	wantStorageErr(t, "in without array", q(storage.QueryFilter{Field: "type", Op: storage.OpIn, Value: "x"}), storage.ErrInvalidArgument, "type")
	wantStorageErr(t, "empty term", q(storage.QueryFilter{}), storage.ErrInvalidArgument, "filter")
	_, err := qs.QueryActivities(s.ctx, storage.ActivityQuery{Filter: s.scoped(), Sort: &storage.QuerySort{Field: "payload"}})
	wantStorageErr(t, "unsortable field", err, storage.ErrUnsupported, "payload")
	_, err = qs.AggregateActivities(s.ctx, storage.AggregateQuery{Filter: s.scoped(), GroupBy: []string{"payload"}, Count: true})
	wantStorageErr(t, "bad group by", err, storage.ErrUnsupported, "payload")
}

func testQueryCountAggregate(t *testing.T, h Harness) {
	s := newSuite(t, h)
	qs := s.query()
	for range 3 {
		s.enqueue(activity(withType("a")))
	}
	for range 2 {
		c := s.enqueueClaimed("c", activity(withType("b")))
		if err := s.b.AckSuccess(s.ctx, c.ID, nil, "c"); err != nil {
			t.Fatal(err)
		}
	}

	n, exact, err := qs.CountActivities(s.ctx, s.scoped(), 0)
	if err != nil || n != 5 || !exact {
		t.Fatalf("count: %d %v %v", n, exact, err)
	}
	n, exact, err = qs.CountActivities(s.ctx, s.scoped(), 2)
	if err != nil || n != 2 || exact {
		t.Fatalf("capped count: %d %v %v", n, exact, err)
	}

	agg, err := qs.AggregateActivities(s.ctx, storage.AggregateQuery{
		Filter: s.scoped(), GroupBy: []string{"type", "status"}, Count: true,
		Durations: []storage.DurationMetric{{Field: "run", Percentiles: []float64{50, 99}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, r := range agg.Rows {
		counts[r.Key["type"]+"/"+r.Key["status"]] = r.Count
		if r.Key["status"] == storage.RecordStatusCompleted {
			if _, ok := r.Durations["run"]["p99"]; !ok {
				t.Fatalf("completed group has no run p99: %+v", r.Durations)
			}
		}
	}
	if counts["a/pending"] != 3 || counts["b/completed"] != 2 || len(counts) != 2 {
		t.Fatalf("groups %v", counts)
	}

	hour := time.Now().UTC().Truncate(time.Hour)
	to := hour.Add(2 * time.Hour)
	buckets, err := qs.AggregateActivities(s.ctx, storage.AggregateQuery{
		Filter: s.scoped(), Count: true,
		Bucket: &storage.AggregateBucket{Field: "created_at", Interval: time.Hour, From: &hour, To: &to},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets.Rows) != 1 || buckets.Rows[0].Count != 5 || buckets.Rows[0].Bucket == nil || !buckets.Rows[0].Bucket.Equal(hour) {
		t.Fatalf("buckets %+v", buckets.Rows)
	}
}

func testQueryEvents(t *testing.T, h Harness) {
	s := newSuite(t, h)
	qs := s.query()
	parent := s.enqueueClaimed("exec-9:w", activity())
	if _, err := s.b.AckFailure(s.ctx, parent.ID, storage.NewRetryableFailure("boom"), "exec-9:w"); err != nil {
		t.Fatal(err)
	}
	child := s.enqueue(activity(withParent(parent)))
	if err := s.b.SignalActivity(s.ctx, child.ID, storage.CheckpointID(child.ID, "signal", "go"), "go", raw("x")); err != nil {
		t.Fatal(err)
	}

	page, err := qs.QueryEvents(s.ctx, storage.EventQuery{Filter: &storage.QueryFilter{Field: "activity_id", Op: storage.OpEq, Value: parent.ID.String()}})
	if err != nil {
		t.Fatal(err)
	}
	var types []string
	for _, e := range page.Items {
		types = append(types, e.Type)
	}
	// Submission, claims and success store no events: the failed attempt is
	// the parent's only one.
	if len(types) != 1 || types[0] != storage.RecordEventAttemptFailed {
		t.Fatalf("event types %v, want [%s]", types, storage.RecordEventAttemptFailed)
	}
	if page.Items[0].ExecutorID != "exec-9" || page.Items[0].Detail != nil {
		t.Fatalf("event fields %+v", page.Items[0])
	}

	tree, err := qs.QueryEvents(s.ctx, storage.EventQuery{Filter: &storage.QueryFilter{Field: "root_id", Op: storage.OpEq, Value: parent.ID.String()}, Desc: true})
	if err != nil || len(tree.Items) != 2 || tree.Items[0].ActivityID != child.ID {
		t.Fatalf("tree events: %+v %v", tree, err)
	}

	byType, err := qs.QueryEvents(s.ctx, storage.EventQuery{Filter: &storage.QueryFilter{And: []storage.QueryFilter{
		{Field: "root_id", Op: storage.OpEq, Value: parent.ID.String()},
		{Field: "type", Op: storage.OpEq, Value: storage.RecordEventAttemptFailed},
	}}, IncludeDetail: true})
	if err != nil || len(byType.Items) != 1 || byType.Items[0].Detail == nil {
		t.Fatalf("failed-attempt events: %+v %v", byType, err)
	}

	first, err := qs.QueryEvents(s.ctx, storage.EventQuery{Filter: &storage.QueryFilter{Field: "root_id", Op: storage.OpEq, Value: parent.ID.String()}, Limit: 1})
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first page: %+v %v", first, err)
	}
	rest, err := qs.QueryEvents(s.ctx, storage.EventQuery{Filter: &storage.QueryFilter{Field: "root_id", Op: storage.OpEq, Value: parent.ID.String()}, Cursor: first.NextCursor})
	if err != nil || len(rest.Items) != 1 || rest.Items[0].ID <= first.Items[0].ID {
		t.Fatalf("second page: %+v %v", rest, err)
	}
}

func testQueryStepsTrees(t *testing.T, h Harness) {
	s := newSuite(t, h)
	qs := s.query()
	root := s.enqueue(activity())
	child := s.enqueue(activity(withParent(root)))
	grandchild := s.enqueue(activity(withParent(child)))

	for i, step := range []string{"run:reserve", "sleep:backoff", "run:charge"} {
		id := uuid.New()
		if err := s.b.StoreResult(s.ctx, id, root.ID, storage.ActivityResult{Data: json.RawMessage(`{"i":` + string(rune('0'+i)) + `}`)}, step); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct created_at for a deterministic order
	}

	page, err := qs.ListStepEntries(s.ctx, root.ID, false, 2, "")
	if err != nil || len(page.Items) != 2 || page.NextCursor == "" {
		t.Fatalf("steps page: %+v %v", page, err)
	}
	if page.Items[0].Kind != "run" || page.Items[0].Name != "reserve" || page.Items[1].Kind != "sleep" || page.Items[0].Data != nil {
		t.Fatalf("steps %+v", page.Items)
	}
	rest, err := qs.ListStepEntries(s.ctx, root.ID, true, 2, page.NextCursor)
	if err != nil || len(rest.Items) != 1 || rest.Items[0].Name != "charge" || rest.Items[0].Data == nil || rest.NextCursor != "" {
		t.Fatalf("second steps page: %+v %v", rest, err)
	}

	tree, err := qs.GetActivityTree(s.ctx, grandchild.ID, storage.RecordInclude{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if tree.RootID != root.ID || len(tree.Items) != 3 || tree.Items[0].ID != root.ID || tree.Items[2].ID != grandchild.ID || tree.Truncated {
		t.Fatalf("tree %+v", tree)
	}
	cut, err := qs.GetActivityTree(s.ctx, root.ID, storage.RecordInclude{}, 2)
	if err != nil || len(cut.Items) != 2 || !cut.Truncated {
		t.Fatalf("truncated tree %+v %v", cut, err)
	}
	_, err = qs.GetActivityTree(s.ctx, uuid.New(), storage.RecordInclude{}, 0)
	wantStorageErr(t, "missing tree", err, storage.ErrNotFound, "")
}
