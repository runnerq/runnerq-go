// Package cron parses runnerq-spec's cron dialect and computes fire times
// (runnerq-spec schedules.md, vectors/cron_next_fire.json).
//
// Instants and wall-clock times are both whole seconds since the Unix epoch
// here: a wall time is the instant it would be if the zone were UTC.
package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

type field struct {
	bits uint64
	star bool // the field's text begins with "*"
}

func (f field) has(v int) bool { return f.bits&(1<<uint(v)) != 0 }

// Expression is a parsed cron expression.
type Expression struct {
	sec, min, hour, dom, month, dow field
}

var macros = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

var (
	monthNames = []string{"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}
	dayNames   = []string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}
)

// Parse parses a 5-field expression, a 6-field one with seconds first, or a
// macro such as @daily.
func Parse(text string) (*Expression, error) {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "@") {
		expanded, ok := macros[strings.ToLower(text)]
		if !ok {
			return nil, fmt.Errorf("cron: unknown macro %q", text)
		}
		text = expanded
	}
	fields := strings.Fields(text)
	switch len(fields) {
	case 5:
		fields = append([]string{"0"}, fields...)
	case 6:
	default:
		return nil, fmt.Errorf("cron: want 5 or 6 fields, got %d", len(fields))
	}
	var e Expression
	specs := []struct {
		dst      *field
		lo, hi   int
		names    []string
		nameBase int
	}{
		{&e.sec, 0, 59, nil, 0},
		{&e.min, 0, 59, nil, 0},
		{&e.hour, 0, 23, nil, 0},
		{&e.dom, 1, 31, nil, 0},
		{&e.month, 1, 12, monthNames, 1},
		{&e.dow, 0, 7, dayNames, 0},
	}
	for i, s := range specs {
		f, err := parseField(fields[i], s.lo, s.hi, s.names, s.nameBase)
		if err != nil {
			return nil, fmt.Errorf("cron: field %d %q: %w", i+1, fields[i], err)
		}
		*s.dst = f
	}
	if e.dow.has(7) {
		e.dow.bits = e.dow.bits&^(1<<7) | 1
	}
	if !e.matchesSomeDate() {
		return nil, errors.New("cron: the expression matches no date")
	}
	return &e, nil
}

func parseField(text string, lo, hi int, names []string, nameBase int) (field, error) {
	f := field{star: strings.HasPrefix(text, "*")}
	value := func(s string) (int, error) {
		for i, n := range names {
			if strings.EqualFold(s, n) {
				return i + nameBase, nil
			}
		}
		if !digits(s) {
			return 0, fmt.Errorf("bad value %q", s)
		}
		v, _ := strconv.Atoi(s)
		if v < lo || v > hi {
			return 0, fmt.Errorf("%d is outside %d-%d", v, lo, hi)
		}
		return v, nil
	}
	for item := range strings.SplitSeq(text, ",") {
		rng, stepText, hasStep := strings.Cut(item, "/")
		step := 1
		if hasStep {
			if !digits(stepText) {
				return f, fmt.Errorf("bad step %q", stepText)
			}
			if step, _ = strconv.Atoi(stepText); step == 0 {
				return f, errors.New("step 0")
			}
		}
		var start, end int
		switch {
		case rng == "*":
			start, end = lo, hi
		case strings.Contains(rng, "-"):
			a, b, _ := strings.Cut(rng, "-")
			var err error
			if start, err = value(a); err != nil {
				return f, err
			}
			if end, err = value(b); err != nil {
				return f, err
			}
			if start > end {
				return f, fmt.Errorf("range %q runs backwards", rng)
			}
		default:
			v, err := value(rng)
			if err != nil {
				return f, err
			}
			start, end = v, v
			if hasStep {
				end = hi
			}
		}
		for v := start; v <= end; v += step {
			f.bits |= 1 << uint(v)
		}
	}
	return f, nil
}

// digits: one or two decimal digits.
func digits(s string) bool {
	return len(s) > 0 && len(s) <= 2 && strings.TrimLeft(s, "0123456789") == ""
}

var daysInMonth = [12]int{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// matchesSomeDate: with the OR rule any weekday matches, so only the AND rule
// can rule out every date (February 30, say).
func (e *Expression) matchesSomeDate() bool {
	if !e.dom.star && !e.dow.star {
		return true
	}
	for m := 1; m <= 12; m++ {
		for d := 1; d <= daysInMonth[m-1]; d++ {
			if e.month.has(m) && e.dom.has(d) {
				return true
			}
		}
	}
	return false
}

func (e *Expression) dayMatches(t time.Time) bool {
	dom, dow := e.dom.has(t.Day()), e.dow.has(int(t.Weekday()))
	if e.dom.star || e.dow.star {
		return dom && dow
	}
	return dom || dow
}

const (
	searchYears = 50
	day         = 86400
	week        = 7 * day
)

// firstMatch is the earliest matching wall time at or after w.
func (e *Expression) firstMatch(w int64) (int64, bool) {
	t := time.Unix(w, 0).UTC()
	limit := t.Year() + searchYears
	for t.Year() <= limit {
		switch {
		case !e.month.has(int(t.Month())):
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		case !e.dayMatches(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
		case !e.hour.has(t.Hour()):
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, time.UTC)
		case !e.min.has(t.Minute()):
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, 0, 0, time.UTC)
		case !e.sec.has(t.Second()):
			t = t.Add(time.Second)
		default:
			return t.Unix(), true
		}
	}
	return 0, false
}

type zone struct{ loc *time.Location }

func (z zone) offset(t int64) int64 {
	_, off := time.Unix(t, 0).In(z.loc).Zone()
	return int64(off)
}

// transition is the first instant in (lo, hi] whose offset differs from lo's.
func (z zone) transition(lo, hi int64) int64 {
	o := z.offset(lo)
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		if z.offset(mid) == o {
			lo = mid
		} else {
			hi = mid
		}
	}
	return hi
}

// atOrAfter is the first fire instant at or after t.
func (e *Expression) atOrAfter(z zone, t int64) (int64, bool) {
	fixed := !e.hour.star
	if fixed {
		// Inside the repeated hour after a fall-back the wall times have
		// fired once already: move to its end.
		if before := z.offset(t - day); before > z.offset(t) {
			tr := z.transition(t-day, t)
			if end := tr + before - z.offset(tr); t < end {
				t = end
			}
		}
	}
	for range 100_000 {
		o := z.offset(t)
		w, ok := e.firstMatch(t + o)
		if !ok {
			return 0, false
		}
		c := w - o
		lim := min(c, t+week)
		if z.offset(lim) != o {
			tr := z.transition(t, lim)
			after := z.offset(tr)
			if after > o {
				// Spring forward: wall times [tr+o, tr+after) never happen.
				// A fixed-hour schedule fires once at tr for any of them.
				if fixed {
					if m, ok := e.firstMatch(tr + o); ok && m < tr+after {
						return tr, true
					}
				}
				t = tr
			} else {
				// Fall back: wall times [tr+after, tr+o) happen again. A
				// fixed-hour schedule skips the second pass.
				t = tr
				if fixed {
					t = tr + o - after
				}
			}
			continue
		}
		if c == lim {
			return c, true
		}
		t = lim
	}
	return 0, false
}

// Next is the first fire time strictly after from, in loc's wall-clock time;
// false when there is none within 50 years.
func (e *Expression) Next(from time.Time, loc *time.Location) (time.Time, bool) {
	t, ok := e.atOrAfter(zone{loc}, from.Unix()+1)
	return time.Unix(t, 0).UTC(), ok
}

// Last is the last n fire times in [from, to], oldest first.
func (e *Expression) Last(from, to time.Time, n int, loc *time.Location) []time.Time {
	z := zone{loc}
	a := from.Unix()
	if from.Nanosecond() > 0 {
		a++
	}
	b := to.Unix()
	window := int64(60)
	for {
		start := max(a, b-window)
		var ticks []int64
		for t, ok := e.atOrAfter(z, start); ok && t <= b; t, ok = e.atOrAfter(z, t+1) {
			ticks = append(ticks, t)
			if len(ticks) > n {
				ticks = ticks[1:]
			}
		}
		if len(ticks) >= n || start <= a {
			out := make([]time.Time, len(ticks))
			for i, t := range ticks {
				out[i] = time.Unix(t, 0).UTC()
			}
			return out
		}
		switch {
		case window < 3600:
			window = 3600
		case window < day:
			window = day
		default:
			window *= 8
		}
	}
}

var zones sync.Map // name -> *time.Location

// LoadZone loads an IANA zone by name. "" and "Local" are rejected: a
// schedule's zone must mean the same on every host.
func LoadZone(name string) (*time.Location, error) {
	if loc, ok := zones.Load(name); ok {
		return loc.(*time.Location), nil
	}
	if name == "" || name == "Local" {
		return nil, fmt.Errorf("cron: unknown time zone %q", name)
	}
	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("cron: unknown time zone %q (on a host without zoneinfo, import _ \"time/tzdata\")", name)
	}
	zones.Store(name, loc)
	return loc, nil
}
