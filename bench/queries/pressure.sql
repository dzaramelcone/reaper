SELECT
    count(*) FILTER (WHERE backend_type = 'client backend') AS connections,
    count(*) FILTER (WHERE state = 'active') AS active,
    count(*) FILTER (WHERE wait_event_type = 'Lock') AS lock_waiters
FROM pg_stat_activity
WHERE datname = current_database();
