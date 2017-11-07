# Benchmarks for Postgres Round Trip Client-Side Copy

To run the benchmarks you must have a `DSN` environmental variable set
in a format understandable by `psycopg2`, for example:

```
export DSN="dbname=test user=postgres password=secret"
```

The full documentation for the DSN string is available 
[here](http://initd.org/psycopg/docs/module.html#psycopg2.connect).

Run the benchmarks with the command: 

```

pyrtcbench --n-reps 30 -n '1k' -n '10k -n '50k' \
    -l 'logged' -l 'unlogged'

```

This runs 30 repetitions for each condition with 3 `n_users` settings on
logged and unlogged targets.
