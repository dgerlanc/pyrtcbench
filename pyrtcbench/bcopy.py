"""
Utility functions for bulk reads and writes with Postgres.
"""

from tempfile import TemporaryFile


def bulk_read_write(cursor, select, insert):
    """Bulk copy to a client buffer then bulk insert to a table

    Parameters
    ----------
    cursor: psycopg2.cursor
    select: dict
        Required key `query` is the psycopg2 template
        string used in the select statement of the copy from.
        Optional key `params`, is a list of parameters escaped into
        the psycopg2 template
    insert: dict
        Required key `table` is the name of the table to insert the
        data retrieved by the select statement. Optional key `columns`
        restricts the columns of `table` inserted into

    """

    with TemporaryFile('wb+') as f:
        _bulk_select(cursor, f, select['query'], select.get('params'))
        f.seek(0)
        _bulk_insert(cursor, f, insert['table'], insert.get('columns'))


def _bulk_select(cursor, buffer, query, params=None):
    sql = """
        copy (
            %s
        ) to stdout with binary
        """ % cursor.mogrify(query, params).decode()
    cursor.copy_expert(sql, buffer)


def _bulk_insert(cursor, buffer, table, columns=None):
    from psycopg2 import sql
    opts = "from stdin with binary"

    if columns is not None:
        _sql = 'copy {} ({}) ' + opts
        params = [sql.Identifier(table),
                  sql.SQL(', ').join(sql.Identifier(c) for c in columns)]
    else:
        _sql = 'copy {} ' + opts
        params = [sql.Identifier(table)]

    query = sql.SQL(_sql).format(*params)
    cursor.copy_expert(query, buffer)
