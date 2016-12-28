"""
This script finds county databases of the form "cDDDDDyDDDD_nr_YYYYMMDD"
and combines their tables into a single master table with 3 extra columns:
 - county_id
 - year
 - submission_date

"""

import argparse
import MySQLdb
import re


COUNTY_PATTERN = re.compile(r'c(\d+)y(\d+)_nr_(\d+)')


def find_county_databases(conn):
    dbs = conn.execute('SHOW DATABASES;').fetchall()
    return filter(COUNTY_PATTERN.match, dbs)


def fetch_table_rows(conn, db_name):
    # Note: we can't use the 2-argument form of "execute" here.
    # See: http://stackoverflow.com/questions/27746795/python-using-mysql-connector-list-databases-like-and-then-use-those-databases-in
    conn.execute('USE %' % db_name)
    tables = conn.execute('SHOW TABLES').fetchall()
    for table in tables:
        rows = conn.execute('SELECT * FROM ?', table)
        for row in iter(rows.fetchone()):
            yield db_name, table, row


def insert_table_row(conn, entry):
    db_name, table, row = entry
    conn.execute('USE master')
    conn.execute('INSERT INTO ' + db_name + 'VALUES (' +
                 ','.join([r'%s'] * len(row))
                 + ')',
                 row)


def parse_args():
    parser = argparse.ArgumentParser('combine_county_databases')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', type=int, default=3306)
    parser.add_argument('--user', default='root')
    parser.add_argument('--password')
    return parser.parse_args()


def main():
    """Main entry point.

    Creates a connection, finds the county databases, queries rows,
    """
    args = parse_args()

    conn = MySQLdb.connect(host=args.host,
                           port=args.port,
                           user=args.user,
                           passwd=args.password)

    dbs = find_county_databases(conn)

    rows = (row
            for db in dbs
            for row in fetch_table_rows(conn, db))

    for row in rows:
        insert_table_row(conn, row)


if __name__ == '__main__':
    main()
