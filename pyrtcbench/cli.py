import click

from .main import main


@click.command()
@click.option('--n-reps', '-r', nargs=1, default=10, type=int,
              help='Number of repetitions per benchmark')
@click.option('--n-users', '-n', multiple=True, default='1k',
              help='Number of users, e.g. 1K')
@click.option('--logging', '-l', multiple=True, default=('logged', 'unlogged',),
              type=click.Choice(['logged', 'unlogged']),
              help="Logging on `user_stats` table")
def cli(n_reps, n_users, logging):
    main(n_reps, n_users, logging)

