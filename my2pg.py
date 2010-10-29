#!/usr/bin/env python

import sys
import optparse
import MySQLdb, psycopg2

def main ():
    parser = optparse.OptionParser(
        '%prog [options] mysql-host mysql-db pg-host pg-db')
    parser.add_option('--mysql-user',
                      action="store",
                      dest="mysql_user",
                      help="User for login if not current user.")
    parser.add_option('--mysql-password',
                      action="store",
                      dest="mysql_password",
                      help="Password to use when connecting to server.")
    parser.add_option('--pg-user',
                      action="store",
                      dest="pg_user",
                      help="User for login if not current user.")
    parser.add_option('--pg-password',
                      action="store", default='',
                      dest="pg_password",
                      help="Password to use when connecting to server.")

    options, args = parser.parse_args()
    if len(args) != 4:
        parser.print_help()
        sys.exit(1)

    mysql_host, mysql_db, pg_host, pg_db = args

    # Set up connections
    mysql_conn = MySQLdb.Connection(
        user=options.mysql_user,
        passwd=options.mysql_password,
        db=mysql_db,
        host=mysql_host,
        )
    pg_conn = psycopg2.connect(
        database=pg_db,
        host=pg_host,
        user=options.pg_user,
        password=options.pg_password,
        )


    # Convert tables

    # Close connections
    mysql_conn.close()
    pg_conn.close()
        
        
    


if __name__ == '__main__':
    main()
