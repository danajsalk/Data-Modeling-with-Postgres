import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""
References/Material:
Template downloads: https://github.com/lucaskjaero/udacity-data-engineering-projects/tree/master/Project%201%20-%20Data%20Modeling%20with%20PostgreSQL

Class Material:
PROJECT SPECIFICATION
https://review.udacity.com/#!/rubrics/2500/view
Udacity Project Template Details:
https://classroom.udacity.com/nanodegrees/nd027/parts/f7dbb125-87a2-4369-bb64-dc5c21bb668a/modules/c0e48224-f2d0-4bf5-ac02-3e1493e530fc/lessons/01995bb4-db30-4e01-bf38-ff11b631be0f/concepts/1533c19b-0505-49fd-b1b7-06c987641f0d
SongDataSet/LogDataSet:
https://classroom.udacity.com/nanodegrees/nd027/parts/f7dbb125-87a2-4369-bb64-dc5c21bb668a/modules/c0e48224-f2d0-4bf5-ac02-3e1493e530fc/lessons/01995bb4-db30-4e01-bf38-ff11b631be0f/concepts/a5609601-2314-4d8b-a7cf-e40048b3ee96

"""


def create_database():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    # drop tables from drop_table_queries;list of DROP statements
    for query in drop_table_queries:
        try: 
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Dropping table")
            print (e)


def create_tables(cur, conn):
    # create tables from create_table_queries; list of INSERT statements
    for query in create_table_queries:
        print(query)
        try: 
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e: 
            print("Error: Issue creating table")
            print (e)

            
def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()
