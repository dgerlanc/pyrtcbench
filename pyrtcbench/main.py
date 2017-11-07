import os
import time
from collections import namedtuple
from datetime import datetime
from itertools import product


import psycopg2

from .bcopy import bulk_read_write


BenchmarkResult = namedtuple(
    'BenchmarkResult',
    'insert_method logging n_users n_initial_stats n_inserted_stats' 
    ' duration run_at'
)


def main(n_reps, n_user_settings, logging_types):
    benchmark_params = product(n_user_settings, logging_types)
    for n_users, _logging in benchmark_params:
        run_benchmark(
            n_users=n_users, use_copy=True, unlogged=_logging, n_reps=n_reps)
        run_benchmark(
            n_users, use_copy=False, unlogged=_logging, n_reps=n_reps)


def run_benchmark(n_users, use_copy, unlogged, n_reps):
    """Entrypoint for running the benchmark

    Parameters
    ----------
    n_users : str
    use_copy : bool
    unlogged : bool
    n_reps : int
        Number of repetitions to run

    Notes
    -----

    Should take additional parameters for:

    #. amount of data initially in `user_stats`
    #. amount of data inserted into `user_stats`
    #. number of repetitions

    """

    n_users_int = _parse_n_users(n_users)
    unlogged = unlogged == 'unlogged'

    dsn = os.environ['DSN']
    with psycopg2.connect(dsn) as conn:
        print('Running %d replications...' % n_reps)
        for i in range(1, n_reps + 1):
            print('  rep %d' % i)
            setup(conn, n_users_int)
            bm_res = _run_benchmark(conn, use_copy, unlogged)
            bm_res_obj = BenchmarkResult(
                insert_method='copy' if use_copy else 'insert',
                logging='unlogged' if unlogged else 'logged',
                n_users=n_users_int,
                n_initial_stats=5 * n_users_int,
                n_inserted_stats=5 * n_users_int,
                duration=bm_res['duration'],
                run_at=bm_res['run_at']
            )
            with conn.cursor() as cursor:
                _insert_benchmark_result(cursor, bm_res_obj)


def _run_benchmark(conn, use_copy, unlogged):

    # Insert into `user_stats` categories 1 - 5
    truncate_and_insert_random_stats(conn, staging=False)
    conn.commit()

    # set user_stats to logged or unlogged per spec
    with conn.cursor() as cursor:
        if unlogged:
            cursor.execute('alter table user_stats set unlogged')
        else:
            cursor.execute('alter table user_stats set logged')
    conn.commit()

    # Add categories which have not yet been inserted
    # TODO: should insert all categories not yet inserted
    category_ids = tuple(range(6, 11)) # Hard-coded hack to insert 6 - 10
    truncate_and_insert_random_stats(conn, True, category_ids)
    conn.commit()

    vacuum_analyze(conn)

    run_at = datetime.utcnow()
    with conn.cursor() as cursor:
        start = time.monotonic()
        _staging_to_user_stats(cursor, use_copy)

    conn.commit()
    end = time.monotonic()

    rv = {
        'run_at': run_at,
        'duration': end - start
    }
    return rv


def setup(conn, n_users:int):
    """Setup the `bench` schema for running the test."""

    with conn.cursor() as cursor:
        with open('setup_bench.sql') as f:
            cursor.execute(' '.join(f))
        # TODO: think search_path is set in setup.sql
        cursor.execute(
            "select set_config('search_path', 'bench, public', false)")
        cursor.execute(
            "select set_config('work_mem', '512MB', false)")
        cursor.execute(
            "select set_config('maintenance_work_mem', '512MB', false)")

        _fill_users(cursor, n_users)
        _fill_categories(cursor) # TODO: Specify n_categories

    conn.commit()


def truncate_and_insert_random_stats(conn, staging=False,
                                     category_ids=(1, 2, 3, 4, 5,)):
    """Bulk insert random user stats into `user_stats_staging` or `user_stats`.

    Truncates the table first so `COPY` is unlogged and fast.

    Parameters
    ----------
    conn : psycopg2.connection
    staging : bool
    category_ids: iterable[int]

    """
    table = 'user_stats_staging' if staging else 'user_stats'

    with conn.cursor() as cursor:
        cursor.execute('truncate table %s' % table)
        _fill_user_stats(cursor, table, category_ids)


def _fill_user_stats(cursor, table, category_ids):
    """Populate table with random user stats data."""

    sql = "select user_id, category_id, value from random_user_stats(%s)"

    cmd = {
        'select': {'query': sql, 'params': [list(category_ids)]},
        'insert': {
            'table': table,
            'columns': ['user_id', 'category_id', 'value']
        }
    }

    bulk_read_write(cursor, **cmd)


def _fill_users(cursor, n_users:int):
    cursor.execute('select fill_users(%s::bigint)', [n_users])


def _fill_categories(cursor, n_categories=10000):
    sql = 'insert into categories select generate_series(1, %s)'
    cursor.execute(sql, [n_categories])


def _staging_to_user_stats(cursor, use_copy=False):
    # TODO: Properly parameterize SQL w/ psycopg2
    select = """
        select
            user_id, category_id, value
        from
            user_stats_staging
    """
    table = 'user_stats'

    cmd = {
        'select': {'query': select},
        'insert': {
            'table': table,
            'columns': ['user_id', 'category_id', 'value']
        }
    }

    if use_copy:
        print('COPYing')
        bulk_read_write(cursor, **cmd)
    else:
        print('Inserting')
        sql = "insert into %s(user_id, category_id, value) %s" % (table, select)
        cursor.execute(sql)


def vacuum_analyze(conn):
    conn.set_session(autocommit=True)
    with conn.cursor() as cursor:
        cursor.execute('vacuum analyze')
    conn.set_session(autocommit=False)
    conn.rollback()


def _insert_benchmark_result(cursor, benchmark_result):
    sql = 'insert into benchmarks values (%s, %s, %s, %s, %s, %s, %s)'
    cursor.execute(sql, benchmark_result)


def _parse_n_users(n_users:str) -> int:
    try:
        rv = int(n_users)
    except ValueError:
        unit = n_users[-1].lower()
        if unit == 'k':
            rv = 1000 * int(n_users[:-1])
        else:
            msg = "%s is not a valid int/unit combination. Only K, e.g. 10K " \
                  "allowed"
            raise ValueError(msg)

    return rv


if __name__ == '__main__':
    main()
