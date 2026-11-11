package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
)

// CommandFingerprint is a command's canonical fingerprint (runnerq-spec
// vectors/command_fingerprint.json): the SHA-256 of a canonical JSON form of
// its input, without its ID. SDK clients set Command.Fingerprint to it, so a
// command id repeated with the same input replays from the shared ledger
// whichever SDK sent it.
func CommandFingerprint(cmd Command) string {
	m := map[string]any{"kind": string(cmd.Kind)}
	if len(cmd.Target.IDs) > 0 {
		ids := make([]any, len(cmd.Target.IDs))
		for i, id := range cmd.Target.IDs {
			ids[i] = id.String()
		}
		m["ids"] = ids
	}
	if cmd.Target.Filter != nil {
		m["filter"] = fingerprintFilter(*cmd.Target.Filter)
	}
	if cmd.Target.Max != 0 {
		m["max"] = float64(cmd.Target.Max)
	}
	for k, v := range map[string]bool{"dry_run": cmd.DryRun, "cascade": cmd.CascadeChildren, "reset_attempts": cmd.ResetAttempts} {
		if v {
			m[k] = true
		}
	}
	for k, v := range map[string]string{"reason": cmd.Reason, "name": cmd.SignalName} {
		if v != "" {
			m[k] = v
		}
	}
	if !cmd.At.IsZero() {
		m["at"] = cmd.At.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z")
	}
	if cmd.Priority != 0 {
		m["priority"] = float64(cmd.Priority)
	}
	if p := decodedJSON(cmd.SignalPayload); p != nil {
		m["payload"] = p
	}
	var b strings.Builder
	canonicalJSON(&b, m)
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func fingerprintFilter(f QueryFilter) map[string]any {
	m := map[string]any{}
	for k, list := range map[string][]QueryFilter{"and": f.And, "or": f.Or} {
		if len(list) > 0 {
			out := make([]any, len(list))
			for i, g := range list {
				out[i] = fingerprintFilter(g)
			}
			m[k] = out
		}
	}
	if f.Not != nil {
		m["not"] = fingerprintFilter(*f.Not)
	}
	if f.Field != "" {
		m["field"] = f.Field
	}
	if f.Op != "" {
		m["op"] = f.Op
	}
	if f.Value != nil {
		raw, _ := json.Marshal(f.Value)
		if v := decodedJSON(raw); v != nil {
			m["value"] = v
		}
	}
	return m
}

// decodedJSON is raw as generic JSON values, or nil for empty, null or
// invalid input.
func decodedJSON(raw []byte) any {
	var v any
	if len(raw) == 0 || json.Unmarshal(raw, &v) != nil {
		return nil
	}
	return v
}

func canonicalJSON(b *strings.Builder, v any) {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case float64:
		b.WriteString(ecmaNumber(x))
	case string:
		b.WriteByte('"')
		for _, r := range x {
			switch {
			case r == '"':
				b.WriteString(`\"`)
			case r == '\\':
				b.WriteString(`\\`)
			case r == '\b':
				b.WriteString(`\b`)
			case r == '\t':
				b.WriteString(`\t`)
			case r == '\n':
				b.WriteString(`\n`)
			case r == '\f':
				b.WriteString(`\f`)
			case r == '\r':
				b.WriteString(`\r`)
			case r < 0x20:
				fmt.Fprintf(b, `\u%04x`, r)
			default:
				b.WriteRune(r)
			}
		}
		b.WriteByte('"')
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			canonicalJSON(b, e)
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			canonicalJSON(b, k)
			b.WriteByte(':')
			canonicalJSON(b, x[k])
		}
		b.WriteByte('}')
	}
}

// ecmaNumber writes f as ECMAScript's Number.prototype.toString does.
func ecmaNumber(f float64) string {
	if f == 0 {
		return "0"
	}
	if a := math.Abs(f); a >= 1e21 || a < 1e-6 {
		mant, exp, _ := strings.Cut(strconv.FormatFloat(f, 'e', -1, 64), "e")
		return mant + "e" + exp[:1] + strings.TrimLeft(exp[1:], "0")
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}
