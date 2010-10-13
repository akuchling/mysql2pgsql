#!/usr/bin/env python

import sys
#import argparse
import sqlalchemy 

def main ():
    mysql_host, mysql_db, pg_host, pg_db = sys.argv[1:]
    
    mysql_engine = create_engine('sqlite://')
    pg_engine = create_engine('sqlite://')

    mysql_meta = sqlalchemy.MetaData()
    pg_meta = sqlalchemy.MetaData()

    mysql_meta.reflect(bind=mysql_engine)

    # Convert tables
    for t in mysql_meta.sorted_tables:
        tbl = sqlalchemy.Table(t, mysql_meta, autoload=True, engine=mysql_engine)
        tbl.create(pg_engine)

        
        
        
    


if __name__ == '__main__':
    main()
