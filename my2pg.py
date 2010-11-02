#!/usr/bin/env python

import sys
import optparse
import MySQLdb, psycopg2

def pg_execute(pg_conn, sql):
    """(Connection, str)

    Log and execute a SQL command on the PostgreSQL connection.
    """
    print sql
    # XXX execute command

def convert_type(typ):
    """(str): str

    Parses a MySQL type declaration and returns the corresponding PostgreSQL
    type.
    """
    # XXX implement conversion
    return typ

def convert_data(col, data):
    """(Column, any) : any

    Convert a Python value retrieved from MySQL into a PostgreSQL value.
    """

class Column:
    """
    Represents a column.

    Instance attributes:
    name : str
    type : str
    position : int
    default : str
    is_nullable : bool

    """

    def __init__(self, **kw):
        for k,v in kw.items():
            setattr(self, k, v)

    def pg_decl(self):
        """(): str

        Return the PostgreSQL declaration syntax for this column.
        """
        typ = convert_type(self.type)
        decl = '%s %s' % (self.name, typ)
        if self.default:
            decl += ' ' + self.default
        if not self.is_nullable:
            decl += ' NOT NULL'
        return decl

class Index:
    """
    Represents an index.

    Instance attributes:
    name : str
    table : str
    type : str
    column_name : str
    non_unique : bool
    nullable : bool

    """

    def __init__(self, **kw):
        for k,v in kw.items():
            setattr(self, k, v)

    def pg_decl(self):
        """(): str

        Return the PostgreSQL declaration syntax for this index.
        """
        sql = 'CREATE INDEX %s ON %s' % (self.name, self.table)
        if self.index_type:
            # XXX convert index_type:
            # BTREE, etc.
            pass
        return sql



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
    mysql_cur = mysql_conn.Cursor()
    pg_cur = pg_conn.Cursor()

    # Make list of tables to process.
    mysql_cur.execute("""
SELECT * FROM information_schema.tables WHERE table_schema = '?'
""", mysql_db)
    tables = sorted(row['TABLE_NAME'] for row in mysql_cur.fetchall())

    # Convert tables
    table_cols = {}
    for table in tables:
        mysql_cur.execute("""
SELECT * FROM information_schema.columns
WHERE table_schema = ? and table_name = ?
""", (mysql_db, table))
        cols = table_cols[table] = []
        for row in mysql_cur.fetchall():
            c = Column()
            cols.append(c)
            c.name = row['COLUMN_NAME']
            c.type = row['COLUMN_TYPE']  #
            c.position = row['ORDINAL_POSITION']
            c.default = row['COLUMN_DEFAULT']
            c.is_nullable = bool(row['IS_NULLABLE'] == 'YES')
            # XXX character set?

        # Sort columns into left-to-right order.
        cols.sort(key=lambda c: c.position)

        # Convert indexes
        mysql_cur.execute("""
SELECT * FROM information_schema.statistics
WHERE table_schema = ? AND table_name = ?
""", (mysql_db, table))
        indexes = []
        for row in mysql_cur.fetchall():
            i = Index()
            indexes.append(i)
            i.table = table
            i.name = row['INDEX_NAME']
            i.column_name = row['COLUMN_NAME']
            i.type = row['INDEX_TYPE']
            i.non_unique = bool(row['NON_UNIQUE'])
            i.nullable = bool(row['NULLABLE'] == 'YES')

        # Assemble into a PGSQL declaration
        sql = "CREATE TABLE (\n"
        for c in column:
            sql += ' ' + c.pg_decl() + ',\n'

        # Look for index named PRIMARY, and add PRIMARY KEY if found.
        primary_L = [i for i in indexes if i.name == 'PRIMARY']
        if len(primary_L):
            assert len(primary_L) == 1
            primary = primary_L.pop()
            sql += 'PRIMARY KEY %s' % primary.column_name


        sql += ');'
        pg_execute(pg_conn, sql)

        # Create indexes
        for i in indexes:
            if i.name == 'PRIMARY':
                continue

            sql = i.pg_decl()
            pg_execute(pg_conn, sql)


    for table in tables:
        # Convert data.
        mysql_cur.execute("SELECT * FROM ?", table)
        cols = table_cols[table]

        # Assemble the INSERT statement once.
        ins_sql = ('INSERT INTO %s (%s) VALUES (%s);' %
                   (table,
                    ', '.join(c.name for c in cols),
                    ','.join(['?'] * len(cols))))

        # We don't do a fetchall() since the table contents are
        # very likely to not fit into memory.
        while True:
            row = mysql_cur.fetchone()
            if row is None:
                continue

            # Assemble a list of the output data that we'll subsequently
            # convert to a tuple.
            output_L = []
            for c in cols:
                data = row[c.name]
                newdata = convert_data(c, data)
                output_L.append(newdata)

            pg_cur.execute(ins_sql, tuple(output_L))

        pass

    # Close connections
    mysql_conn.close()
    pg_conn.close()





if __name__ == '__main__':
    main()
