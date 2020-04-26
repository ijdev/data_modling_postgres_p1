import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    connect to a default database and set auto-commit to true
    then drop sparkifydb db if exists otherwise it get created
    finally connect to sparkifydb and returns db connection and cursor.

    :return:
        conn: connection to the database
        cur: the cursor of the database
    """
    # connect to default database
    try:
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

        print('Database created successfully')
        return cur, conn

    except psycopg2.Error as e:
        print(e)
        print('Error creating database')


def drop_tables(cur, conn):
    """
    loop and execute each query in drop_table_queries defined in sql_quires

    :param cur: curser of the database
    :param conn: the connection to the database
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
        print('Tabled dropped successfully')
    except psycopg2.Error as e:
        print(e)
        print('Error dropping tables')


def create_tables(cur, conn):
    """
    loop and execute each query in drop_table_queries defined in sql_quires

    :param cur: curser of the database
    :param conn: the connection to the database
    """
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
