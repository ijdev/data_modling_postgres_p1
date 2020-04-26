import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")

        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # create sparkify database with UTF8 encoding

        ### To close connection if another session is connected
        # cur.execute("""
        #     SELECT pg_terminate_backend(pg_stat_activity.pid)
        #     FROM pg_stat_activity
        #     WHERE pg_stat_activity.datname = 'sparkifydb'
        #     AND pid <> pg_backend_pid();
        # """)
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

        # close connection to default database
        conn.close()

        # connect to sparkify database
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")

        cur = conn.cursor()

        print('Database created successfully')
        return cur, conn

    except psycopg2.Error as e:
        print(e)
        print('Error creating database')


def drop_tables(cur, conn):
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
        print('Tabled dropped successfully')
    except psycopg2.Error as e:
        print(e)
        print('Error dropping tables')


def create_tables(cur, conn):
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        print('Tabled created successfully')

    except psycopg2.Error as e:
        print(e)
        print('Error creating tables')


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
