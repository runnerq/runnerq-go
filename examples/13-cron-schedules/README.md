# 13 — Cron schedules

A schedule enqueues an activity on a cron expression. Declared on the engine,
it is stored with the queue when the engine starts, and every engine of the
queue fires it: each tick runs once, however many engines are up.

## What this shows

A `Heartbeat` activity runs every 10 seconds. Start the program in two
terminals: every heartbeat is printed by exactly one of them. Stop both for a
minute and start one again: with `CatchUpOne`, the missed heartbeats run once,
then the schedule carries on.

## Run it

```bash
docker compose up -d        # from the examples/ directory, if not already up
go run .
```

## Expected output

```
heartbeat 0b366e1c-… (payload {"source": "cron"})
heartbeat 5f1d7a20-… (payload {"source": "cron"})
```

See [docs/schedules.md](../../docs/schedules.md) for the cron dialect,
time zones, catch-up and the management API.
