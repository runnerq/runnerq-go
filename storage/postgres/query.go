package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/runnerq/runnerq-go/storage"
)

// storage.QueryStorage is cross-queue. Filters compile to parameterized SQL;
// nothing from a query is spliced into SQL text except whitelisted expressions.

const (
	defaultQueryLimit = 50
	maxQueryLimit     = 1000
	defaultAggLimit   = 200
	maxAggLimit       = 1000
	maxFilterDepth    = 8
	maxFilterNodes    = 64
	maxInValues       = 1000
	maxTreeNodes      = 5000
)

type fieldKind int

const (
	kindString fieldKind = iota
	kindInt
	kindTime
	kindUUID
	kindStatus
	kindEventType
)

type queryField struct {
	expr     string
	kind     fieldKind
	nullable bool
}

// updatedAtSQL is the latest state change we have a timestamp for.
const updatedAtSQL = `GREATEST(a.created_at, a.started_at, a.completed_at, a.last_error_at)`

var activityFields = map[string]queryField{
	"id":              {expr: "a.id", kind: kindUUID},
	"type":            {expr: "a.activity_type", kind: kindString},
	"queue":           {expr: "a.queue_name", kind: kindString},
	"status":          {expr: "a.status", kind: kindStatus},
	"priority":        {expr: "a.priority", kind: kindInt},
	"root_id":         {expr: "a.root_activity_id", kind: kindUUID},
	"parent_id":       {expr: "a.parent_activity_id", kind: kindUUID, nullable: true},
	"depth":           {expr: "a.depth", kind: kindInt},
	"idempotency_key": {expr: "a.idempotency_key", kind: kindString, nullable: true},
	"attempt":         {expr: "(a.retry_count + 1)", kind: kindInt},
	// max_retries holds total attempts allowed; 0 (unlimited) matches no number.
	"max_attempts":  {expr: "NULLIF(a.max_retries, 0)", kind: kindInt},
	"created_at":    {expr: "a.created_at", kind: kindTime},
	"scheduled_for": {expr: "a.scheduled_at", kind: kindTime, nullable: true},
	"started_at":    {expr: "a.started_at", kind: kindTime, nullable: true},
	"completed_at":  {expr: "a.completed_at", kind: kindTime, nullable: true},
	"updated_at":    {expr: updatedAtSQL, kind: kindTime},
	"executor_id":   {expr: "NULLIF(split_part(a.current_worker_id, ':', 1), '')", kind: kindString, nullable: true},
}

var eventFields = map[string]queryField{
	// Increasing, so "seq > cursor" tails the log.
	"seq":         {expr: "e.id", kind: kindInt},
	"activity_id": {expr: "e.activity_id", kind: kindUUID},
	"queue":       {expr: "e.queue_name", kind: kindString},
	"type":        {expr: "e.event_type", kind: kindEventType},
	"at":          {expr: "e.created_at", kind: kindTime},
	"executor_id": {expr: "NULLIF(split_part(e.worker_id, ':', 1), '')", kind: kindString, nullable: true},
	// Special-cased in predicate: events carry no root.
	"root_id": {expr: "", kind: kindUUID},
}

// sortSentinel stands in for NULL in nullable sort keys so keyset paging
// stays total (nulls sort last in descending order).
var sortSentinel = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

type sortField struct {
	expr string
	kind fieldKind
}

var activitySorts = map[string]sortField{
	"created_at":   {expr: "a.created_at", kind: kindTime},
	"completed_at": {expr: "COALESCE(a.completed_at, '0001-01-01 00:00:00+00'::timestamptz)", kind: kindTime},
	"priority":     {expr: "a.priority", kind: kindInt},
}

var canonicalStatuses = map[string][]string{
	storage.RecordStatusPending:    {"pending"},
	storage.RecordStatusScheduled:  {"scheduled", "retrying"},
	storage.RecordStatusRunning:    {"processing"},
	storage.RecordStatusWaiting:    {"waiting"},
	storage.RecordStatusCompleted:  {"completed"},
	storage.RecordStatusFailed:     {"failed"},
	storage.RecordStatusDeadLetter: {"dead_letter"},
	storage.RecordStatusCancelled:  {"cancelled"},
}

const canonicalStatusSQL = `CASE a.status WHEN 'processing' THEN 'running' WHEN 'retrying' THEN 'scheduled' ELSE a.status END`

func canonicalStatus(internal string) string {
	switch internal {
	case "processing":
		return storage.RecordStatusRunning
	case "retrying":
		return storage.RecordStatusScheduled
	}
	return internal
}

var canonicalEvents = map[string]string{
	storage.EventFailed:          storage.RecordEventAttemptFailed,
	storage.EventRetrying:        storage.RecordEventAttemptFailed,
	storage.EventDeadLetter:      storage.RecordEventDeadLetter,
	storage.EventRequeued:        storage.RecordEventLeaseExpired,
	storage.EventYielded:         storage.RecordEventWaitParked,
	storage.EventSignaled:        storage.RecordEventSignalReceived,
	storage.EventSpawnLinked:     storage.RecordEventChildLinked,
	storage.EventCancelled:       storage.RecordEventCancelled,
	storage.EventRetried:         storage.RecordEventRetried,
	storage.EventRedriven:        storage.RecordEventRedriven,
	storage.EventRunNow:          storage.RecordEventRunNow,
	storage.EventRescheduled:     storage.RecordEventRescheduled,
	storage.EventPriorityChanged: storage.RecordEventPriorityChanged,
}

func canonicalEvent(internal string) string {
	if c, ok := canonicalEvents[internal]; ok {
		return c
	}
	return "other." + strings.ToLower(internal)
}

func internalEvents(canonical string) []string {
	var out []string
	for in, c := range canonicalEvents {
		if c == canonical {
			out = append(out, in)
		}
	}
	if rest, ok := strings.CutPrefix(canonical, "other."); ok && len(out) == 0 {
		// Best effort for types without a canonical name: match ignoring case.
		out = append(out, rest)
	}
	slices.Sort(out)
	return out
}

func (b *PostgresBackend) QueryCapabilities() storage.QueryCapabilities {
	fields := make([]string, 0, len(activityFields)+1)
	for f := range activityFields {
		fields = append(fields, f)
	}
	fields = append(fields, "metadata")
	slices.Sort(fields)
	var events []string
	for f := range eventFields {
		events = append(events, f)
	}
	slices.Sort(events)
	var sorts []string
	for f := range activitySorts {
		sorts = append(sorts, f)
	}
	slices.Sort(sorts)
	return storage.QueryCapabilities{
		ActivityFilters: fields,
		ActivitySorts:   sorts,
		EventFilters:    events,
		GroupBy:         []string{"queue", "root", "status", "type"},
		Buckets:         []string{"completed_at", "created_at"},
		Durations:       []string{"queue", "run", "total"},
	}
}

type sqlBuilder struct {
	args  []any
	nodes int
}

func (sb *sqlBuilder) arg(v any) string {
	sb.args = append(sb.args, v)
	return "$" + strconv.Itoa(len(sb.args))
}

func (sb *sqlBuilder) where(f *storage.QueryFilter, fields map[string]queryField) (string, error) {
	if f == nil {
		return "TRUE", nil
	}
	return sb.compile(*f, fields, 0)
}

func (sb *sqlBuilder) compile(f storage.QueryFilter, fields map[string]queryField, depth int) (string, error) {
	if depth > maxFilterDepth {
		return "", storage.NewInvalidQueryError("filter", fmt.Sprintf("filter nests deeper than %d", maxFilterDepth))
	}
	if sb.nodes++; sb.nodes > maxFilterNodes {
		return "", storage.NewInvalidQueryError("filter", fmt.Sprintf("filter has more than %d terms", maxFilterNodes))
	}
	set := 0
	for _, on := range []bool{len(f.And) > 0, len(f.Or) > 0, f.Not != nil, f.Field != ""} {
		if on {
			set++
		}
	}
	if set != 1 {
		return "", storage.NewInvalidQueryError("filter", "each filter term needs exactly one of and, or, not or field")
	}
	switch {
	case len(f.And) > 0 || len(f.Or) > 0:
		terms, joiner := f.And, " AND "
		if len(f.Or) > 0 {
			terms, joiner = f.Or, " OR "
		}
		parts := make([]string, 0, len(terms))
		for _, t := range terms {
			p, err := sb.compile(t, fields, depth+1)
			if err != nil {
				return "", err
			}
			parts = append(parts, p)
		}
		return "(" + strings.Join(parts, joiner) + ")", nil
	case f.Not != nil:
		p, err := sb.compile(*f.Not, fields, depth+1)
		if err != nil {
			return "", err
		}
		return "(NOT " + p + ")", nil
	}
	return sb.predicate(f, fields)
}

func (sb *sqlBuilder) predicate(f storage.QueryFilter, fields map[string]queryField) (string, error) {
	fd, ok := fields[f.Field]
	if key, isMeta := strings.CutPrefix(f.Field, "metadata."); !ok && isMeta && fields["id"].expr == "a.id" {
		if key == "" {
			return "", storage.NewInvalidQueryError(f.Field, "metadata filters need a key")
		}
		fd, ok = queryField{expr: "(a.metadata->>" + sb.arg(key) + ")", kind: kindString, nullable: true}, true
	}
	if !ok {
		return "", storage.NewUnsupportedQueryError(f.Field, fmt.Sprintf("field %q is not queryable", f.Field))
	}
	if f.Field == "root_id" && fd.expr == "" {
		return sb.eventRootPredicate(f)
	}
	if f.Field == "idempotency_key" && fields["id"].expr == "a.id" {
		return sb.idempotencyKeyPredicate(f)
	}

	switch f.Op {
	case storage.OpExists:
		want := true
		if f.Value != nil {
			b, ok := f.Value.(bool)
			if !ok {
				return "", storage.NewInvalidQueryError(f.Field, "exists takes a boolean value")
			}
			want = b
		}
		if !fd.nullable {
			return strconv.FormatBool(want), nil
		}
		if want {
			return fd.expr + " IS NOT NULL", nil
		}
		return fd.expr + " IS NULL", nil

	case storage.OpEq, storage.OpNe, storage.OpIn, storage.OpNin:
		multi := f.Op == storage.OpIn || f.Op == storage.OpNin
		var raw []any
		if multi {
			list, ok := f.Value.([]any)
			if !ok {
				return "", storage.NewInvalidQueryError(f.Field, f.Op+" takes an array value")
			}
			if len(list) > maxInValues {
				return "", storage.NewInvalidQueryError(f.Field, fmt.Sprintf("%s is limited to %d values", f.Op, maxInValues))
			}
			raw = list
		} else {
			raw = []any{f.Value}
		}
		vals, err := convertValues(f.Field, fd.kind, raw)
		if err != nil {
			return "", err
		}
		negate := f.Op == storage.OpNe || f.Op == storage.OpNin
		if vals == nil {
			// Nothing can match (e.g. an id that is not a UUID).
			return strconv.FormatBool(negate), nil
		}
		var cond string
		switch {
		case fd.kind == kindStatus:
			// Literals, not a parameter: Postgres proves a partial index's
			// predicate (idx_runnerq_query_status) only from constants. The
			// values come from canonicalStatuses.
			cond = fd.expr + " IN (" + quoteLiterals(vals.([]string)) + ")"
		case f.Field == "root_id":
			// Roots aren't in idx_runnerq_root_children; they're found by id.
			p := sb.arg(vals)
			cond = "(a.id = ANY(" + p + ") OR (a.root_activity_id = ANY(" + p + ") AND a.parent_activity_id IS NOT NULL))"
		default:
			cond = fd.expr + " = ANY(" + sb.arg(vals) + ")"
		}
		if negate {
			return "(NOT COALESCE(" + cond + ", false))", nil
		}
		return cond, nil

	case storage.OpLt, storage.OpLte, storage.OpGt, storage.OpGte:
		if fd.kind != kindInt && fd.kind != kindTime {
			return "", storage.NewUnsupportedQueryError(f.Field, f.Op+" is only supported on numeric and time fields")
		}
		vals, err := convertValues(f.Field, fd.kind, []any{f.Value})
		if err != nil {
			return "", err
		}
		op := map[string]string{storage.OpLt: "<", storage.OpLte: "<=", storage.OpGt: ">", storage.OpGte: ">="}[f.Op]
		return fd.expr + " " + op + " " + sb.arg(singleValue(vals)), nil

	case storage.OpPrefix:
		if fd.kind != kindString {
			return "", storage.NewUnsupportedQueryError(f.Field, "prefix is only supported on string fields")
		}
		s, ok := f.Value.(string)
		if !ok {
			return "", storage.NewInvalidQueryError(f.Field, "prefix takes a string value")
		}
		p := sb.arg(s)
		return "left(" + fd.expr + ", char_length(" + p + ")) = " + p, nil
	}
	return "", storage.NewUnsupportedQueryError(f.Field, fmt.Sprintf("operator %q is not supported", f.Op))
}

// idempotencyKeyPredicate matches the application key (see
// storage.ApplicationIdempotencyKey) against every stored form: v2 business
// key for the row's type, legacy "<key>-<type>", or raw (written directly via
// the storage API). Step-derived keys are excluded.
func (sb *sqlBuilder) idempotencyKeyPredicate(f storage.QueryFilter) (string, error) {
	const stored = "a.idempotency_key"
	hasKey := "(" + stored + " IS NOT NULL AND " + stored + " <> '' AND left(" + stored + ", " +
		strconv.Itoa(len(storage.StepKeyPrefix)) + ") <> " + sb.arg(storage.StepKeyPrefix) + ")"
	switch f.Op {
	case storage.OpExists:
		want := true
		if f.Value != nil {
			b, ok := f.Value.(bool)
			if !ok {
				return "", storage.NewInvalidQueryError(f.Field, "exists takes a boolean value")
			}
			want = b
		}
		if want {
			return hasKey, nil
		}
		return "(NOT " + hasKey + ")", nil

	case storage.OpEq, storage.OpNe, storage.OpIn, storage.OpNin:
		raw := []any{f.Value}
		if f.Op == storage.OpIn || f.Op == storage.OpNin {
			list, ok := f.Value.([]any)
			if !ok {
				return "", storage.NewInvalidQueryError(f.Field, f.Op+" takes an array value")
			}
			if len(list) > maxInValues {
				return "", storage.NewInvalidQueryError(f.Field, fmt.Sprintf("%s is limited to %d values", f.Op, maxInValues))
			}
			raw = list
		}
		keys := make([]string, 0, len(raw))
		for _, v := range raw {
			s, ok := v.(string)
			if !ok {
				return "", storage.NewInvalidQueryError(f.Field, "idempotency_key takes string values")
			}
			keys = append(keys, s)
		}
		// v2 encoding per row type: unpadded base64, newlines dropped since
		// Postgres wraps base64 at 76 characters.
		v2 := "'rq:key:v2:' || rtrim(translate(encode(convert_to(octet_length(k) || ':' || k || a.activity_type, 'UTF8'), 'base64'), E'\\n', ''), '=')"
		match := "EXISTS (SELECT 1 FROM unnest(" + sb.arg(keys) + "::text[]) AS keys(k) WHERE " +
			stored + " = " + v2 + " OR " + stored + " = k || '-' || a.activity_type OR (" + stored + " = k AND left(" + stored + ", 10) <> 'rq:key:v2:'))"
		cond := "(" + hasKey + " AND " + match + ")"
		if f.Op == storage.OpNe || f.Op == storage.OpNin {
			return "(NOT " + cond + ")", nil
		}
		return cond, nil
	}
	return "", storage.NewUnsupportedQueryError(f.Field, "idempotency_key supports eq, ne, in, nin and exists: stored keys are encoded, so prefix and contains can't match")
}

func (sb *sqlBuilder) eventRootPredicate(f storage.QueryFilter) (string, error) {
	var raw []any
	switch f.Op {
	case storage.OpEq:
		raw = []any{f.Value}
	case storage.OpIn:
		list, ok := f.Value.([]any)
		if !ok || len(list) > maxInValues {
			return "", storage.NewInvalidQueryError(f.Field, "in takes an array of at most 1000 values")
		}
		raw = list
	default:
		return "", storage.NewUnsupportedQueryError(f.Field, "root_id supports eq and in")
	}
	vals, err := convertValues(f.Field, kindUUID, raw)
	if err != nil {
		return "", err
	}
	if vals == nil {
		return "false", nil
	}
	p := sb.arg(vals)
	return "e.activity_id IN (SELECT x.id FROM runnerq_activities x WHERE x.id = ANY(" + p + ") OR (x.root_activity_id = ANY(" + p + ") AND x.parent_activity_id IS NOT NULL))", nil
}

func quoteLiterals(vals []string) string {
	quoted := make([]string, len(vals))
	for i, v := range vals {
		quoted[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	return strings.Join(quoted, ", ")
}

// convertValues returns nil (no error) when no value can possibly match.
func convertValues(field string, kind fieldKind, raw []any) (any, error) {
	switch kind {
	case kindString:
		out := make([]string, 0, len(raw))
		for _, v := range raw {
			s, ok := v.(string)
			if !ok {
				return nil, storage.NewInvalidQueryError(field, "expected a string")
			}
			out = append(out, s)
		}
		return out, nil
	case kindInt:
		out := make([]int64, 0, len(raw))
		for _, v := range raw {
			n, ok := v.(float64)
			if !ok || n != math.Trunc(n) || math.Abs(n) > 1<<53 {
				return nil, storage.NewInvalidQueryError(field, "expected an integer")
			}
			out = append(out, int64(n))
		}
		return out, nil
	case kindTime:
		out := make([]time.Time, 0, len(raw))
		for _, v := range raw {
			s, ok := v.(string)
			t, err := time.Parse(time.RFC3339Nano, s)
			if !ok || err != nil {
				return nil, storage.NewInvalidQueryError(field, "expected an RFC 3339 timestamp")
			}
			out = append(out, t)
		}
		return out, nil
	case kindUUID:
		out := make([]uuid.UUID, 0, len(raw))
		for _, v := range raw {
			s, ok := v.(string)
			if !ok {
				return nil, storage.NewInvalidQueryError(field, "expected a string id")
			}
			if id, err := uuid.Parse(s); err == nil {
				out = append(out, id)
			}
		}
		if len(out) == 0 {
			return nil, nil
		}
		return out, nil
	case kindStatus:
		var out []string
		for _, v := range raw {
			s, ok := v.(string)
			internal, known := canonicalStatuses[s]
			if !ok || !known {
				return nil, storage.NewInvalidQueryError(field, fmt.Sprintf("unknown status %v", v))
			}
			out = append(out, internal...)
		}
		return out, nil
	case kindEventType:
		var out []string
		for _, v := range raw {
			s, ok := v.(string)
			if !ok {
				return nil, storage.NewInvalidQueryError(field, "expected an event type string")
			}
			out = append(out, internalEvents(s)...)
		}
		if len(out) == 0 {
			return nil, nil
		}
		return out, nil
	}
	return nil, storage.NewUnsupportedQueryError(field, "unsupported field type")
}

func singleValue(vals any) any {
	switch v := vals.(type) {
	case []int64:
		return v[0]
	case []time.Time:
		return v[0]
	}
	return vals
}

type activityCursor struct {
	Sort string     `json:"s"`
	Desc bool       `json:"d"`
	Time *time.Time `json:"t,omitempty"`
	Num  *int64     `json:"n,omitempty"`
	ID   uuid.UUID  `json:"i"`
}

func encodeCursor(v any) string {
	b, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || json.Unmarshal(b, v) != nil {
		return storage.NewInvalidQueryError("cursor", "invalid cursor")
	}
	return nil
}

func clampLimit(limit, def, max int) int {
	if limit <= 0 {
		return def
	}
	return min(limit, max)
}

func activitySelect(inc storage.RecordInclude) (cols, joins string) {
	cols = `a.id, a.activity_type, a.queue_name, a.status, a.priority,
		COALESCE(a.root_activity_id, a.id), a.parent_activity_id, a.depth, a.idempotency_key,
		a.retry_count, a.max_retries, a.created_at, a.scheduled_at, a.started_at, a.completed_at,
		` + updatedAtSQL + `, a.timeout_seconds, a.lease_deadline_ms, a.current_worker_id, a.metadata,
		y.detail`
	// The latest park reason, only for waiting rows.
	joins = ` LEFT JOIN LATERAL (
			SELECT e.detail FROM runnerq_events e
			WHERE e.activity_id = a.id AND e.event_type = 'Yielded'
			ORDER BY e.created_at DESC, e.id DESC LIMIT 1
		) y ON a.status = 'waiting'`
	if inc.Payload {
		cols += ", (SELECT " + plainData("i.payload", "i.serialization") + " FROM runnerq_inputs i WHERE i.activity_id = a.id)"
	}
	if inc.LastError {
		cols += ", a.last_error, a.last_error_at"
	}
	if inc.Result {
		cols += ", r.state, " + plainData("r.data", "r.serialization")
		joins += " LEFT JOIN runnerq_results r ON r.activity_id = a.id"
	}
	return cols, joins
}

func scanRecord(rows pgx.Rows, inc storage.RecordInclude, extra ...any) (storage.ActivityRecord, error) {
	var (
		r                    storage.ActivityRecord
		status               string
		priority             int32
		depth                int16
		idemKey              *string
		retryCount, maxRetry int32
		timeoutSeconds       int64
		leaseMS              *int64
		workerID             *string
		metadata, yield      []byte
		payload              []byte
		lastError            *string
		lastErrorAt          *time.Time
		resultState          *string
		resultData           []byte
	)
	dest := []any{&r.ID, &r.Type, &r.Queue, &status, &priority, &r.RootID, &r.ParentID, &depth, &idemKey,
		&retryCount, &maxRetry, &r.CreatedAt, &r.ScheduledFor, &r.StartedAt, &r.CompletedAt,
		&r.UpdatedAt, &timeoutSeconds, &leaseMS, &workerID, &metadata, &yield}
	if inc.Payload {
		dest = append(dest, &payload)
	}
	if inc.LastError {
		dest = append(dest, &lastError, &lastErrorAt)
	}
	if inc.Result {
		dest = append(dest, &resultState, &resultData)
	}
	dest = append(dest, extra...)
	if err := rows.Scan(dest...); err != nil {
		return r, databaseError(err, fmt.Sprintf("Failed to scan activity: %v", err))
	}

	r.Status = canonicalStatus(status)
	r.Priority = int(priority)
	r.Depth = int(depth)
	if idemKey != nil {
		r.IdempotencyKey = storage.ApplicationIdempotencyKey(*idemKey, r.Type)
	}
	r.Attempt = int(retryCount) + 1
	r.MaxAttempts = int(maxRetry)
	r.Timeout = time.Duration(timeoutSeconds) * time.Second
	if leaseMS != nil && status == "processing" {
		t := time.UnixMilli(*leaseMS).UTC()
		r.LeaseExpiresAt = &t
	}
	if workerID != nil && status == "processing" {
		r.ExecutorID, _, _ = strings.Cut(*workerID, ":")
	}
	if len(metadata) > 0 {
		_ = json.Unmarshal(metadata, &r.Metadata)
	}
	if status == "waiting" {
		r.Wait = parseWait(yield)
	}
	if inc.Payload {
		r.Payload = payload
	}
	if inc.LastError && lastError != nil {
		r.LastError = &storage.RecordError{Message: *lastError, Kind: errorKind(status), At: lastErrorAt}
	}
	if inc.Result && resultState != nil {
		state := storage.ResultOk
		if *resultState != "Ok" {
			state = storage.ResultErr
		}
		r.Result = &storage.ActivityResult{State: state, Data: resultData}
	}
	return r, nil
}

func errorKind(status string) string {
	switch status {
	case "retrying", "scheduled", "pending", "processing":
		return "retryable"
	case "failed":
		return "non_retryable"
	case "dead_letter":
		return "dead_letter"
	}
	return ""
}

func parseWait(detail []byte) *storage.RecordWait {
	w := &storage.RecordWait{Kind: "other"}
	var d struct {
		Kind   string `json:"kind"`
		Step   string `json:"step"`
		WakeAt string `json:"wake_at"`
	}
	if len(detail) == 0 || json.Unmarshal(detail, &d) != nil {
		return w
	}
	switch d.Kind {
	case "sleep", "signal":
		w.Kind = d.Kind
	case "await":
		w.Kind = "children"
	}
	w.Name = d.Step
	if _, name, ok := strings.Cut(d.Step, ":"); ok && d.Kind != "await" {
		w.Name = name
	}
	if t, err := time.Parse(time.RFC3339, d.WakeAt); err == nil {
		w.Until = &t
	}
	return w
}

// QueryActivities spans every queue in the database, not just this backend's.
func (b *PostgresBackend) QueryActivities(ctx context.Context, q storage.ActivityQuery) (*storage.ActivityRecordPage, error) {
	sort := storage.QuerySort{Field: "created_at", Desc: true}
	if q.Sort != nil {
		sort = *q.Sort
	}
	sf, ok := activitySorts[sort.Field]
	if !ok {
		return nil, storage.NewUnsupportedQueryError(sort.Field, fmt.Sprintf("cannot sort by %q", sort.Field))
	}
	limit := clampLimit(q.Limit, defaultQueryLimit, maxQueryLimit)

	sb := &sqlBuilder{}
	where, err := sb.where(q.Filter, activityFields)
	if err != nil {
		return nil, err
	}
	cmp, dir := ">", "ASC"
	if sort.Desc {
		cmp, dir = "<", "DESC"
	}
	if q.Cursor != "" {
		var c activityCursor
		if err := decodeCursor(q.Cursor, &c); err != nil {
			return nil, err
		}
		if c.Sort != sort.Field || c.Desc != sort.Desc {
			return nil, storage.NewInvalidQueryError("cursor", "cursor was issued for a different sort")
		}
		var v any
		switch {
		case sf.kind == kindTime && c.Time != nil:
			v = *c.Time
		case sf.kind == kindInt && c.Num != nil:
			v = *c.Num
		default:
			return nil, storage.NewInvalidQueryError("cursor", "invalid cursor")
		}
		pv, pid := sb.arg(v), sb.arg(c.ID)
		where += fmt.Sprintf(" AND (%s %s %s OR (%s = %s AND a.id %s %s))", sf.expr, cmp, pv, sf.expr, pv, cmp, pid)
	}

	cols, joins := activitySelect(q.Include)
	sql := fmt.Sprintf(`SELECT %s, %s FROM runnerq_activities a%s WHERE %s ORDER BY %s %s, a.id %s LIMIT %d`,
		cols, sf.expr, joins, where, sf.expr, dir, dir, limit+1)
	rows, err := b.pool.Query(ctx, sql, sb.args...)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to query activities: %v", err))
	}
	defer rows.Close()

	page := &storage.ActivityRecordPage{Items: []storage.ActivityRecord{}}
	var lastTime time.Time
	var lastNum int32
	for rows.Next() {
		var sortDest any = &lastTime
		if sf.kind == kindInt {
			sortDest = &lastNum
		}
		rec, err := scanRecord(rows, q.Include, sortDest)
		if err != nil {
			return nil, err
		}
		if len(page.Items) == limit {
			// A row beyond the page: there is more. Cursor at the last kept row.
			last := page.Items[limit-1]
			c := activityCursor{Sort: sort.Field, Desc: sort.Desc, ID: last.ID}
			switch sort.Field {
			case "created_at":
				t := last.CreatedAt
				c.Time = &t
			case "completed_at":
				t := sortSentinel
				if last.CompletedAt != nil {
					t = *last.CompletedAt
				}
				c.Time = &t
			case "priority":
				n := int64(last.Priority)
				c.Num = &n
			}
			page.NextCursor = encodeCursor(c)
			break
		}
		page.Items = append(page.Items, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to query activities: %v", err))
	}
	return page, nil
}

func (b *PostgresBackend) CountActivities(ctx context.Context, filter *storage.QueryFilter, limit int64) (int64, bool, error) {
	if limit <= 0 {
		limit = 100_000
	}
	sb := &sqlBuilder{}
	where, err := sb.where(filter, activityFields)
	if err != nil {
		return 0, false, err
	}
	var n int64
	err = b.pool.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM (SELECT 1 FROM runnerq_activities a WHERE %s LIMIT %d) s`, where, limit+1),
		sb.args...).Scan(&n)
	if err != nil {
		return 0, false, databaseError(err, fmt.Sprintf("Failed to count activities: %v", err))
	}
	if n > limit {
		return limit, false, nil
	}
	return n, true, nil
}

var groupExprs = map[string]string{
	"status": canonicalStatusSQL,
	"type":   "a.activity_type",
	"queue":  "a.queue_name",
	"root":   "CASE WHEN a.parent_activity_id IS NULL THEN 'true' ELSE 'false' END",
}

var durationExprs = map[string]string{
	"queue": "EXTRACT(EPOCH FROM (a.started_at - COALESCE(a.scheduled_at, a.created_at))) * 1000",
	"run":   "EXTRACT(EPOCH FROM (a.completed_at - a.started_at)) * 1000",
	"total": "EXTRACT(EPOCH FROM (a.completed_at - a.created_at)) * 1000",
}

var bucketFields = map[string]string{
	"created_at":   "a.created_at",
	"completed_at": "a.completed_at",
}

func (b *PostgresBackend) AggregateActivities(ctx context.Context, q storage.AggregateQuery) (*storage.AggregateRows, error) {
	if !q.Count && len(q.Durations) == 0 {
		return nil, storage.NewInvalidQueryError("metrics", "ask for at least one metric")
	}
	sb := &sqlBuilder{}
	where, err := sb.where(q.Filter, activityFields)
	if err != nil {
		return nil, err
	}
	var selects, groups []string
	for _, g := range q.GroupBy {
		expr, ok := groupExprs[g]
		if !ok {
			return nil, storage.NewUnsupportedQueryError(g, fmt.Sprintf("cannot group by %q", g))
		}
		selects = append(selects, expr)
		groups = append(groups, strconv.Itoa(len(selects)))
	}
	bucketed := q.Bucket != nil
	if bucketed {
		field, ok := bucketFields[q.Bucket.Field]
		if !ok {
			return nil, storage.NewUnsupportedQueryError(q.Bucket.Field, fmt.Sprintf("cannot bucket by %q", q.Bucket.Field))
		}
		if q.Bucket.Interval < time.Second {
			return nil, storage.NewInvalidQueryError("bucket.interval_ms", "bucket interval must be at least 1s")
		}
		ms := sb.arg(float64(q.Bucket.Interval.Milliseconds()))
		selects = append(selects, fmt.Sprintf(
			"to_timestamp(floor(EXTRACT(EPOCH FROM %s) * 1000 / %s) * %s / 1000)", field, ms, ms))
		groups = append(groups, strconv.Itoa(len(selects)))
		where += " AND " + field + " IS NOT NULL"
		if q.Bucket.From != nil {
			where += " AND " + field + " >= " + sb.arg(*q.Bucket.From)
		}
		if q.Bucket.To != nil {
			where += " AND " + field + " < " + sb.arg(*q.Bucket.To)
		}
	}
	selects = append(selects, "count(*)")
	countCol := len(selects)
	type durCol struct {
		field string
		pcts  []float64
	}
	var durs []durCol
	for _, d := range q.Durations {
		expr, ok := durationExprs[d.Field]
		if !ok {
			return nil, storage.NewUnsupportedQueryError(d.Field, fmt.Sprintf("unknown duration %q", d.Field))
		}
		pcts := d.Percentiles
		if len(pcts) == 0 {
			pcts = []float64{50, 95, 99}
		}
		fracs := make([]float64, len(pcts))
		for i, p := range pcts {
			if p <= 0 || p >= 100 {
				return nil, storage.NewInvalidQueryError("percentiles", "percentiles must be between 0 and 100")
			}
			fracs[i] = p / 100
		}
		selects = append(selects, fmt.Sprintf("percentile_cont(%s::float8[]) WITHIN GROUP (ORDER BY %s)", sb.arg(fracs), expr))
		durs = append(durs, durCol{field: d.Field, pcts: pcts})
	}

	limit := clampLimit(q.Limit, defaultAggLimit, maxAggLimit)
	sql := "SELECT " + strings.Join(selects, ", ") + " FROM runnerq_activities a WHERE " + where
	if len(groups) > 0 {
		sql += " GROUP BY " + strings.Join(groups, ", ")
	}
	order := strconv.Itoa(countCol) + " DESC"
	if bucketed {
		order = strconv.Itoa(len(q.GroupBy)+1) + " ASC, " + order
	}
	sql += fmt.Sprintf(" ORDER BY %s LIMIT %d", order, limit+1)

	rows, err := b.pool.Query(ctx, sql, sb.args...)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to aggregate activities: %v", err))
	}
	defer rows.Close()
	out := &storage.AggregateRows{Rows: []storage.AggregateRow{}}
	for rows.Next() {
		if len(out.Rows) == limit {
			out.Truncated = true
			break
		}
		keys := make([]*string, len(q.GroupBy))
		var bucket *time.Time
		var count int64
		dest := make([]any, 0, len(selects))
		for i := range keys {
			dest = append(dest, &keys[i])
		}
		if bucketed {
			dest = append(dest, &bucket)
		}
		dest = append(dest, &count)
		pvals := make([][]*float64, len(durs))
		for i := range durs {
			dest = append(dest, &pvals[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, databaseError(err, fmt.Sprintf("Failed to scan aggregate: %v", err))
		}
		row := storage.AggregateRow{Count: count, Bucket: bucket}
		if len(keys) > 0 {
			row.Key = make(map[string]string, len(keys))
			for i, k := range keys {
				if k != nil {
					row.Key[q.GroupBy[i]] = *k
				}
			}
		}
		for i, d := range durs {
			vals := map[string]float64{}
			for j, p := range d.pcts {
				if j < len(pvals[i]) && pvals[i][j] != nil {
					vals["p"+strconv.FormatFloat(p, 'f', -1, 64)] = *pvals[i][j]
				}
			}
			if row.Durations == nil {
				row.Durations = map[string]map[string]float64{}
			}
			row.Durations[d.field] = vals
		}
		out.Rows = append(out.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to aggregate activities: %v", err))
	}
	return out, nil
}

func (b *PostgresBackend) QueryEvents(ctx context.Context, q storage.EventQuery) (*storage.EventRecordPage, error) {
	limit := clampLimit(q.Limit, defaultQueryLimit, maxQueryLimit)
	sb := &sqlBuilder{}
	where, err := sb.where(q.Filter, eventFields)
	if err != nil {
		return nil, err
	}
	cmp, dir := ">", "ASC"
	if q.Desc {
		cmp, dir = "<", "DESC"
	}
	if q.Cursor != "" {
		id, err := strconv.ParseInt(q.Cursor, 10, 64)
		if err != nil {
			return nil, storage.NewInvalidQueryError("cursor", "invalid cursor")
		}
		where += " AND e.id " + cmp + " " + sb.arg(id)
	}
	detail := "NULL::jsonb"
	if q.IncludeDetail {
		detail = "e.detail"
	}
	rows, err := b.pool.Query(ctx, fmt.Sprintf(`
		SELECT e.id, e.activity_id, e.event_type, e.created_at, e.worker_id, %s
		FROM runnerq_events e WHERE %s ORDER BY e.id %s LIMIT %d`, detail, where, dir, limit+1), sb.args...)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to query events: %v", err))
	}
	defer rows.Close()
	page := &storage.EventRecordPage{Items: []storage.EventRecord{}}
	for rows.Next() {
		if len(page.Items) == limit {
			page.NextCursor = page.Items[limit-1].Cursor
			break
		}
		var ev storage.EventRecord
		var typ string
		var worker *string
		if err := rows.Scan(&ev.ID, &ev.ActivityID, &typ, &ev.At, &worker, &ev.Detail); err != nil {
			return nil, databaseError(err, fmt.Sprintf("Failed to scan event: %v", err))
		}
		ev.Type = canonicalEvent(typ)
		ev.Cursor = strconv.FormatInt(ev.ID, 10)
		if worker != nil {
			ev.ExecutorID, _, _ = strings.Cut(*worker, ":")
		}
		page.Items = append(page.Items, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to query events: %v", err))
	}
	return page, nil
}

type stepCursor struct {
	At time.Time `json:"t"`
	ID uuid.UUID `json:"i"`
}

func (b *PostgresBackend) ListStepEntries(ctx context.Context, activityID uuid.UUID, includeData bool, limit int, cursor string) (*storage.StepEntryPage, error) {
	limit = clampLimit(limit, defaultQueryLimit, maxQueryLimit)
	sb := &sqlBuilder{}
	where := "owner_activity_id = " + sb.arg(activityID) + " AND step IS NOT NULL"
	if cursor != "" {
		var c stepCursor
		if err := decodeCursor(cursor, &c); err != nil {
			return nil, err
		}
		pt, pid := sb.arg(c.At), sb.arg(c.ID)
		where += fmt.Sprintf(" AND (created_at > %s OR (created_at = %s AND activity_id > %s))", pt, pt, pid)
	}
	data := "NULL::jsonb"
	if includeData {
		data = plainData("data", "serialization")
	}
	rows, err := b.pool.Query(ctx, fmt.Sprintf(`
		SELECT activity_id, step, state, %s, created_at FROM runnerq_results
		WHERE %s ORDER BY created_at, activity_id LIMIT %d`, data, where, limit+1), sb.args...)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to list steps: %v", err))
	}
	defer rows.Close()
	page := &storage.StepEntryPage{Items: []storage.StepEntry{}}
	for rows.Next() {
		if len(page.Items) == limit {
			last := page.Items[limit-1]
			page.NextCursor = encodeCursor(stepCursor{At: last.CreatedAt, ID: last.ID})
			break
		}
		var s storage.StepEntry
		var step, state string
		if err := rows.Scan(&s.ID, &step, &state, &s.Data, &s.CreatedAt); err != nil {
			return nil, databaseError(err, fmt.Sprintf("Failed to scan step: %v", err))
		}
		s.ActivityID = activityID
		s.Kind, s.Name = step, ""
		if k, n, ok := strings.Cut(step, ":"); ok {
			s.Kind, s.Name = k, n
		}
		switch s.Kind {
		case "run", "sleep", "signal":
		default:
			s.Kind = "other"
		}
		s.State = storage.ResultOk
		if state != "Ok" {
			s.State = storage.ResultErr
		}
		page.Items = append(page.Items, s)
	}
	if err := rows.Err(); err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to list steps: %v", err))
	}
	return page, nil
}

func (b *PostgresBackend) GetActivityTree(ctx context.Context, activityID uuid.UUID, inc storage.RecordInclude, maxNodes int) (*storage.ActivityTree, error) {
	maxNodes = clampLimit(maxNodes, maxTreeNodes, maxTreeNodes)
	var root uuid.UUID
	err := b.pool.QueryRow(ctx,
		`SELECT COALESCE(root_activity_id, id) FROM runnerq_activities WHERE id = $1`, activityID).Scan(&root)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, storage.NewNotFoundError(fmt.Sprintf("activity %s not found", activityID))
	}
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to resolve tree root: %v", err))
	}
	cols, joins := activitySelect(inc)
	rows, err := b.pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM runnerq_activities a%s
		WHERE a.id = $1 OR (a.root_activity_id = $1 AND a.parent_activity_id IS NOT NULL)
		ORDER BY a.depth, a.created_at, a.id LIMIT %d`, cols, joins, maxNodes+1), root)
	if err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to load tree: %v", err))
	}
	defer rows.Close()
	tree := &storage.ActivityTree{RootID: root, Items: []storage.ActivityRecord{}}
	for rows.Next() {
		if len(tree.Items) == maxNodes {
			tree.Truncated = true
			break
		}
		rec, err := scanRecord(rows, inc)
		if err != nil {
			return nil, err
		}
		tree.Items = append(tree.Items, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, databaseError(err, fmt.Sprintf("Failed to load tree: %v", err))
	}
	return tree, nil
}

var _ storage.QueryStorage = (*PostgresBackend)(nil)

// plainData reads json-v1 as stored and the TypeScript SDK's superjson-v1 as
// its "json" part; "meta" only matters to the SDK that decodes it.
func plainData(data, serialization string) string {
	return "CASE WHEN " + serialization + " = 'superjson-v1' THEN " + data + "->'json' ELSE " + data + " END"
}
