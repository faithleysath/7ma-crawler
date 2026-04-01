create table if not exists fetch_attempt_log (
    id uuid primary key,
    fetch_id uuid not null,
    sweep_id uuid not null references crawl_sweep(id),
    point_id uuid not null references crawl_point(id),
    point_name text not null,
    source_namespace text not null,
    collector_id text not null,
    attempt integer not null,
    requested_at timestamptz not null,
    finished_at timestamptz not null,
    request_latitude double precision not null,
    request_longitude double precision not null,
    http_status integer,
    status_code integer,
    trace_id text,
    error_type text,
    error_message text,
    response_body text,
    created_at timestamptz not null default now()
);

create index if not exists idx_fetch_attempt_log_fetch
    on fetch_attempt_log (fetch_id, attempt);

create index if not exists idx_fetch_attempt_log_point_time
    on fetch_attempt_log (point_id, requested_at desc);

create index if not exists idx_fetch_attempt_log_sweep_time
    on fetch_attempt_log (sweep_id, requested_at desc);
