SELECT jsonb_build_object(
    'database', (
        SELECT to_jsonb(database_stats) - 'datid' - 'datname' - 'stats_reset'
        FROM pg_stat_database AS database_stats
        WHERE database_stats.datname = current_database()
    ),
    'wal', (
        SELECT to_jsonb(wal_stats) - 'stats_reset'
        FROM pg_stat_wal AS wal_stats
    ),
    'tables', (
        SELECT
            coalesce(
                jsonb_object_agg(
                    table_stats.relname,
                    to_jsonb(table_stats) - 'relid' - 'schemaname' - 'relname'
                ),
                '{}'::jsonb
            )
        FROM pg_stat_user_tables AS table_stats
        WHERE table_stats.schemaname = 'reaper'
    ),
    'io', (
        SELECT
            coalesce(
                jsonb_object_agg(
                    io_stats.relname,
                    to_jsonb(io_stats) - 'relid' - 'schemaname' - 'relname'
                ),
                '{}'::jsonb
            )
        FROM pg_statio_user_tables AS io_stats
        WHERE io_stats.schemaname = 'reaper'
    ),
    'sizes', (
        SELECT
            coalesce(
                jsonb_object_agg(
                    classes.relname,
                    jsonb_build_object(
                        'table_bytes', pg_table_size(classes.oid),
                        'index_bytes', pg_indexes_size(classes.oid),
                        'total_bytes', pg_total_relation_size(classes.oid)
                    )
                ),
                '{}'::jsonb
            )
        FROM pg_class AS classes
        JOIN pg_namespace AS namespaces ON namespaces.oid = classes.relnamespace
        WHERE namespaces.nspname = 'reaper' AND classes.relkind = 'r'
    )
)::text;
