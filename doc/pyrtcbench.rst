
------------------------
Benchmark Initialization
------------------------

#. Drop and recreate all tables except `benchmarks` result table
#. Populate `users`
#. Populate `categories`
#. Populate `user_stats` with random data for all users, 5 categories
#. Populate `user_stats_staging` with random data for all users and additional
   5 categories
#. Run `vacuum analyze`
#. Copy records from `user_stats_staging` to `user_stats` using `INSERT` or
   `COPY`

------------------
Run ``main.setup``
------------------

Create 4 tables:
* categories
* user_stats
* user_stats_staging
* users

``setup`` reads and executes the ``setup_bench.sql`` script. ``setup_bench.sql`` is
idempotent and will *drop and recreate* the ``bench`` schema and all its
children with each execution!

``setup`` creates the schema and inserts `n_users` with integer IDs of
`1` to `n` in the `users` table. `n_users` completely determines how
many records are inserted

Next, `categories` is populated with category IDs `1` to `10,000`.

## Populate `user_stats`

Next, we insert random data into `user_stats` because we want to test
the performance of inserting into an already populated table.

## Populate `user_stats_staging`

Next, `user_stats_staging`, is truncated and filled with random data using
`COPY`. This
table is truncated and filled with random data before each run to prevent
caching of the subsequent selection from the table. We insert the random
data into the staging table and select from it for subsequent inserts so
we can isolate the select and insert performance from the cost of random
data generation.

Finally, after all the staging tables have been created, we run
`vacuum analyze`.

## Run benchmark

`SELECT` from user_stats_staging and insert into `user_stats` either
with a Table-to-Table insert or using a round-trip client-side `COPY`.

## Results
Table-to-Table (Logged)
Table-to-Table (Unlogged)
COPY (Logged)
COPY (Unlogged)
