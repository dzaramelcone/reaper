# Getting started

Start PostgreSQL from the repository root:

```console
docker compose up -d
```

Export the connection string in each terminal that uses Reaper:

```console
export REAPER_POSTGRES_DSN=postgresql://reaper:reaper@127.0.0.1:55433/reaper
```

## 1. Start Reaper

Durable work needs a process that outlives the client submitting it. Reaper is
that long-running supervisor: it starts warm skeleton processes, replaces them
when they fail, and shuts them down cleanly.

Start Reaper with one task skeleton:

```console
uv run reaper --pool 1
```

Nothing has been submitted yet. The skeleton should become ready and remain
idle; that is a complete, healthy Reaper runtime.

`ReaperClient` does not contain or execute the worker. Clients connect to
PostgreSQL to submit promises, while independently running skeletons claim and
execute them. Reaper supervises those skeletons.

## 2. Hello world

With Reaper still running, open another terminal and run the smallest durable
function:

```console
uv run python -m examples.hello_world
```

It decorates one async function and awaits it like an ordinary call. Reaper
assigns the root a UUID, persists it, and lets the skeleton execute it.

## 3. Compose durable steps

When a workflow should delegate a reusable step, it can await another durable
function with ordinary Python syntax:

```console
uv run python -m examples.composition
```

The root suspends while its child runs, then resumes with the child result.

## 4. Fan work out

When several independent items can run concurrently, fan them out and combine
their promise results:

```console
uv run python -m examples.fanout
```

## 5. Give a root a stable ID

An HTTP handler can use its request idempotency key as the root ID:

```console
uv run python -m examples.idempotent_root
```

Run it repeatedly: identical work returns the same promise result. Reusing the
ID for different work raises an idempotency conflict.

## 6. Submit without waiting

When an HTTP endpoint should return `202 Accepted` immediately, submit the root
and return its promise ID:

```console
uv run python -m examples.submit_root
```

This is only the framework-independent handler core; it introduces no web
framework or polling protocol.

## 7. Wait durably

Timers are advanced by a maintenance skeleton. Stop the task-only Reaper and
restart it with both pools:

```console
uv run reaper --pool 1 --pool maintenance:1
uv run python -m examples.durable_timer
```

Each example keeps its durable functions and client entry point in one readable
module. Run them with `python -m` as shown so Reaper records their importable
module names for remote skeletons.
