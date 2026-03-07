create table if not exists crawl_point (
    id uuid primary key,
    name text not null unique,
    latitude double precision not null,
    longitude double precision not null,
    radius_m integer not null default 100,
    enabled boolean not null default true,
    created_at timestamptz not null default now()
);

create table if not exists crawl_sweep (
    id uuid primary key,
    source_namespace text not null,
    collector_id text not null,
    logical_slot timestamptz not null,
    started_at timestamptz not null,
    finished_at timestamptz,
    status text not null check (status in ('running', 'completed', 'partial', 'failed')),
    point_count integer not null,
    success_count integer not null default 0,
    failure_count integer not null default 0,
    created_at timestamptz not null default now()
);

create index if not exists idx_crawl_sweep_slot
    on crawl_sweep (source_namespace, logical_slot desc);

create index if not exists idx_crawl_sweep_started
    on crawl_sweep (started_at desc);

create table if not exists point_fetch (
    id uuid primary key,
    sweep_id uuid not null references crawl_sweep(id),
    point_id uuid not null references crawl_point(id),
    requested_at timestamptz not null,
    finished_at timestamptz,
    http_status integer,
    status_code integer,
    trace_id text,
    error_type text,
    error_message text,
    raw_json jsonb,
    created_at timestamptz not null default now()
);

create index if not exists idx_point_fetch_sweep
    on point_fetch (sweep_id);

create index if not exists idx_point_fetch_point_time
    on point_fetch (point_id, requested_at desc);

create table if not exists raw_observation (
    id uuid primary key,
    fetch_id uuid not null references point_fetch(id),
    sweep_id uuid not null references crawl_sweep(id),
    point_id uuid not null references crawl_point(id),
    observed_at timestamptz not null,
    bucket text not null check (bucket in ('danche', 'zhuli')),
    vehicle_uid text not null,
    car_id bigint,
    number text,
    vendor_lock_id text,
    carmodel_id integer,
    api_type integer,
    lock_id text,
    battery_name text,
    distance_m double precision,
    vehicle_longitude double precision,
    vehicle_latitude double precision,
    raw_vehicle jsonb not null
);

create index if not exists idx_raw_observation_vehicle_time
    on raw_observation (vehicle_uid, observed_at desc, id desc);

create index if not exists idx_raw_observation_sweep_time
    on raw_observation (sweep_id, observed_at desc);

create index if not exists idx_raw_observation_point_time
    on raw_observation (point_id, observed_at desc);

drop view if exists vehicle_latest;

create view vehicle_latest as
select distinct on (s.source_namespace, o.vehicle_uid)
    s.source_namespace,
    o.vehicle_uid,
    o.bucket,
    o.number,
    o.vendor_lock_id,
    o.carmodel_id,
    o.api_type,
    o.lock_id,
    o.battery_name,
    o.distance_m,
    o.vehicle_longitude,
    o.vehicle_latitude,
    o.observed_at,
    o.point_id,
    o.fetch_id,
    o.sweep_id,
    s.collector_id,
    s.logical_slot
from raw_observation as o
join crawl_sweep as s on s.id = o.sweep_id
order by s.source_namespace, o.vehicle_uid, o.observed_at desc, o.id desc;
