-- Setup the `bench` schema for running benchmarks

-- drop schema if exists bench cascade;
create schema if not exists bench;
select set_config('search_path', 'bench, public', false);

-- clear out existing tables
drop table if exists
    categories, user_stats, user_stats_staging, users cascade;


create table if not exists benchmarks (
    insert_method varchar not null,
    logging varchar not null,
    n_users int not null,
    n_initial_stats int not null,
    n_inserted_stats int not null,
    duration double precision not null,
    run_at timestamp with time zone not null default current_timestamp,
    check (insert_method in ('copy', 'insert')),
    check (logging in ('logged', 'unlogged'))
);


create table categories (
    category_id int primary key
);


create table users (
    user_id int primary key
);


create unlogged table user_stats (
    user_id int not null
        references users (user_id) on delete cascade on update cascade,
    category_id int not null
        references categories (category_id) on delete cascade on update cascade,
    value double precision not null,
--    created_at timestamp with time zone not null default current_timestamp,
    primary key (user_id, category_id)
);

create unlogged table user_stats_staging (
    like user_stats
    including defaults
);

create or replace function fill_users(n bigint) returns void as $$
    insert into users
    select generate_series(1, $1)
$$
language sql;


create or replace function random_user_stats(categories int[])
returns table (
    user_id int, category_id int, value double precision
) as $$
    select
        user_id,
        cats.category_id,
        random() as value
    from
        users
    cross join (
        select
            category_id
        from
            categories
        where
            category_id = any($1)
    ) cats
    ;
$$
language sql;

-- create unlogged table user_stats (
--     id serial not null primary key,
--     user_id int not null
--         references users (user_id) on delete cascade on update cascade,
--     category_id int not null
--         references categories (category_id) on delete cascade on update cascade,
--     value double precision not null,
-- --    created_at timestamp with time zone not null default current_timestamp,
--     constraint unq_user_id_category_id unique (user_id, category_id)
-- );
