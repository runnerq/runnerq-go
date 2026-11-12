# Schedules

A schedule enqueues an activity on a cron expression. Schedules are stored
with the queue (`runnerq_schedules`), so every engine of the queue, Go or
TypeScript, fires them, and each tick runs once however many engines race for
it. No separate scheduler process.

## Declaring schedules

Register schedules on the engine before `Start`; `Start` upserts them, so the
schedules your code declares are the ones stored:

```go
err := engine.RegisterSchedule(runnerq.Schedule{
    Name:     "nightly-report",
    Cron:     "30 2 * * *",
    Timezone: "Europe/London",
    Activity: runnerq.NameOf[NightlyReport](),
    Payload:  json.RawMessage(`{"format":"pdf"}`),
    Priority: runnerq.PriorityHigh,
    Timeout:  10 * time.Minute,
    CatchUp:  storage.CatchUpOne,
})
```

| Field | Default | Meaning |
| --- | --- | --- |
| `Name` | required | Identifies the schedule in its queue. |
| `Cron` | required | The expression (below). |
| `Timezone` | `UTC` | IANA zone the expression's wall-clock times are in. |
| `Activity` | required | Activity type to enqueue. |
| `Payload` | `null` | Each fired activity's payload. |
| `Priority`, `MaxRetries`, `Timeout`, `MaxRetryDelay`, `Metadata` | as for `ActivityBuilder`: normal, 3 attempts, 300s, 1h | The fired activities' execution options. |
| `CatchUp`, `CatchUpLimit` | `CatchUpOne`, 100 | What to do with ticks missed while no engine ran (below). |

Upserting keeps a schedule's paused state and its next fire time, unless the
cron or time zone changed. Two deployments declaring the same name
differently overwrite each other on each start: roll such a change out like a
schema change.

Time zones need zoneinfo on the host. In a minimal container, add
`import _ "time/tzdata"` to your main package.

## Managing schedules

`runnerq.NewSchedules(backend)` (or `engine.Schedules()`) manages a queue's
schedules from any process, worker or not:

```go
schedules, err := runnerq.NewSchedules(backend)

schedules.Upsert(ctx, runnerq.Schedule{...})
schedules.Pause(ctx, "nightly-report")
schedules.Resume(ctx, "nightly-report")     // from the next tick; no catch-up
future, err := schedules.Trigger(ctx, "nightly-report") // run once now, even if paused
list, err := schedules.List(ctx)            // []storage.Schedule
s, err := schedules.Get(ctx, "nightly-report") // s.NextFireAt, s.LastFireAt, s.Paused
schedules.Delete(ctx, "nightly-report")
```

Pause, resume, trigger, get and delete of an unknown name return a
`storage.ErrNotFound` error. Triggers within the same second share one
activity.

## Cron expressions

Five fields, `minute hour day-of-month month day-of-week`, or six with
`second` first:

| Expression | Fires |
| --- | --- |
| `*/5 * * * *` | every 5 minutes |
| `0 9 * * 1-5` | 09:00 on weekdays |
| `30 2 1 * *` | 02:30 on the 1st of each month |
| `0 0 13 * FRI` | midnight on the 13th and on every Friday |
| `*/10 * * * * *` | every 10 seconds |
| `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly` | the usual macros |

Fields take `*`, values, ranges `a-b`, steps `/n` and comma lists; months
`JAN`-`DEC` and days `SUN`-`SAT` in any case; 0 and 7 are Sunday. When both
day fields are restricted a day matches either (as in Vixie cron). `L`, `W`,
`#` and `?` are not supported. An expression that matches no date, such as
`0 0 30 2 *`, is rejected.

**Daylight saving.** Schedules run on wall-clock time in their zone:

- A schedule with a fixed hour (`30 2 * * *`) fires once a day. On the
  spring-forward day it fires when the skipped hour ends; on the fall-back day
  it fires in the first pass only.
- A schedule whose hour field starts with `*` (`0 * * * *`, `*/15 * * * *`)
  follows real time: skipped wall times don't fire, repeated ones fire twice,
  so an hourly job runs every real hour.

Go and TypeScript compute the same fire times; the
[runnerq-spec vectors](https://github.com/runnerq/runnerq-spec/blob/main/vectors/cron_next_fire.json)
pin them.

## Firing and catch-up

Each engine checks for due schedules about every second, using the database
clock. A due schedule is fired in one transaction that enqueues the activity
with the idempotency key `schedule:<name>:<fire time>` (for example
`schedule:nightly-report:2026-03-08T02:30:00Z`) and advances its next fire
time. Racing engines skip a schedule another is firing, and a crash leaves
both writes or neither, so a tick never runs twice.

When no engine ran for a while, several ticks may be due at once:

| `CatchUp` | Fires |
| --- | --- |
| `storage.CatchUpOne` (default) | the latest due tick, once |
| `storage.CatchUpSkip` | the latest due tick only if it is under a minute late |
| `storage.CatchUpAll` | every due tick, oldest first, at most `CatchUpLimit` (default 100, at most 1000) |

A fired activity's application idempotency key is
`storage.ScheduleIdempotencyKey(name, at)`, which queries report.

## Custom storage

Schedules are the optional `storage.ScheduleStorage` capability. A backend
implementing it should use `storage.ValidateSchedule`,
`storage.NextFireTime`, `storage.ScheduleFireTimes` and
`storage.ScheduledActivity` so its fire times match, and pass the
`Schedules` group of `storagetest` (implement `storagetest.ScheduleClock` on
the harness to run the firing tests). See [Storage Backends](storage-backends.md).
