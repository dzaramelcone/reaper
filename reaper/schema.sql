-- Reaper durable promises for PostgreSQL 16+

CREATE SCHEMA reaper;
COMMENT ON SCHEMA reaper IS 'Reaper durable promise store';

CREATE TYPE reaper.promise_state AS ENUM (
    'pending', 'resolved', 'rejected', 'timed_out'
);

CREATE TABLE reaper.promises (
    id text PRIMARY KEY,
    idempotency_key text NOT NULL CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),
    state reaper.promise_state NOT NULL DEFAULT 'pending',
    root_id text,
    graph_id text GENERATED ALWAYS AS (coalesce(root_id, id)) STORED,
    result jsonb,
    error jsonb,
    due_at timestamptz,
    expires_at timestamptz,
    delete_after timestamptz,
    settled_at timestamptz,
    CHECK (id <> '' AND octet_length(id) <= 1024),
    CHECK (root_id IS NULL OR root_id <> id),
    UNIQUE (graph_id, id),
    FOREIGN KEY (graph_id, root_id)
    REFERENCES reaper.promises (graph_id, id) ON DELETE CASCADE,
    CHECK (
        (due_at IS NOT NULL AND expires_at IS NULL)
        OR (due_at IS NULL AND expires_at IS NOT NULL)
    ),
    CHECK (
        (state = 'pending' AND settled_at IS NULL AND result IS NULL AND error IS NULL)
        OR (state = 'resolved' AND settled_at IS NOT NULL AND error IS NULL)
        OR (
            state IN ('rejected', 'timed_out')
            AND settled_at IS NOT NULL AND result IS NULL AND error IS NOT NULL
        )
    ),
    CHECK (
        (root_id IS NULL AND delete_after > coalesce(expires_at, due_at))
        OR (root_id IS NOT NULL AND delete_after IS NULL)
    )
);

CREATE INDEX promises_due
ON reaper.promises (due_at, id)
WHERE due_at IS NOT NULL AND state = 'pending';

CREATE INDEX promises_expiring
ON reaper.promises (expires_at, id)
WHERE expires_at IS NOT NULL AND state = 'pending';

CREATE INDEX promises_delete
ON reaper.promises (delete_after, id)
WHERE root_id IS NULL;

CREATE TABLE reaper.tasks (
    promise_id text PRIMARY KEY
    REFERENCES reaper.promises (id) ON DELETE CASCADE,
    function text NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    topic text NOT NULL,
    input jsonb NOT NULL,
    depth smallint NOT NULL CHECK (depth BETWEEN 0 AND 256),
    pending_waits integer NOT NULL DEFAULT 0 CHECK (pending_waits >= 0),
    priority smallint NOT NULL,
    available_at timestamptz NOT NULL,
    failures integer NOT NULL DEFAULT 0 CHECK (failures >= 0),
    max_failures integer NOT NULL
    CHECK (max_failures BETWEEN 1 AND 100),
    execution_timeout_ms integer NOT NULL
    CHECK (execution_timeout_ms BETWEEN 1 AND 86400000),
    CHECK (function <> '' AND octet_length(function) <= 1024),
    CHECK (topic <> '' AND octet_length(topic) <= 255),
    CHECK (failures < max_failures)
);

CREATE INDEX tasks_ready
ON reaper.tasks (topic, priority DESC, available_at, promise_id)
INCLUDE (function, version, execution_timeout_ms)
WHERE pending_waits = 0;

CREATE TABLE reaper.waits (
    waiter_id text NOT NULL REFERENCES reaper.tasks (promise_id) ON DELETE CASCADE,
    awaited_id text NOT NULL,
    graph_id text NOT NULL,
    PRIMARY KEY (waiter_id, awaited_id),
    CHECK (waiter_id <> awaited_id),
    FOREIGN KEY (graph_id, waiter_id)
    REFERENCES reaper.promises (graph_id, id) ON DELETE CASCADE,
    FOREIGN KEY (graph_id, awaited_id)
    REFERENCES reaper.promises (graph_id, id)
    ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX waits_awaited
ON reaper.waits (awaited_id, waiter_id);
