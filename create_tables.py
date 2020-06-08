import psycopg2
from queries import create_table_queries, drop_table_queries

# Function to establish database connection and return the connection and cursor reference
def create_database():

    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=practice user=postgres password=password")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # dropping all connection to sparkifydb database
    cur.execute("SELECT pg_terminate_backend(pid) from pg_stat_activity where datname='songsdb' ")
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS songsdb")
    cur.execute("CREATE DATABASE songsdb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=songsdb user=postgres password=password")
    cur = conn.cursor()
    
    return cur, conn

# Function to run all the drop table queries defined in sql_queries.py
# :param cur: cursor to the database
# :param conn: database connection reference
def drop_tables(cur, conn):
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


# Function to run all the create table queries defined in sql_queries.py
# :param cur: cursor to the database
# :param conn: database connection reference
def create_tables(cur, conn):
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# Driver function
def main():
   
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    print("Table dropped successfully!!")

    create_tables(cur, conn)
    print("Table created successfully!!")

    conn.close()