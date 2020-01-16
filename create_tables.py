import psycopg2
from sql_queries import create_table_queries, drop_table_queries, database_drop, database_create


def create_database():
    """Creates the sparkify database.
    
    Returns:
        cursor: cursor object from the sparkify database connection, 
            if the connection was successful, otherwise None.
        connection: connection object to sparkify database, 
            if connection to the database was successful, otherwise None.
    """
    
    try:        
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        conn.set_session(autocommit=True)
        cur = conn.cursor()
    
        cur.execute(database_drop)
        cur.execute(database_create)

        conn.close()    
    
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    
        return cur, conn
    except psycopg2.Error as e: 	    
	    print(e)



def drop_tables(cur, conn):
    """Drops all tables in the sparkify database.
    
    Args:
        cur (:obj:`cursor`): cursor object from the sparkify database connection.
        conn (:obj:`connection`): connection object to the sparkify database.    
    """
    
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e: 	    
	    print(e)


def create_tables(cur, conn):
    """Creates the required tables in the sparkify database.
    
    Args:
        cur (:obj:`cursor`): cursor object from the sparkify database connection.
        conn (:obj:`connection`): connection object to the sparkify database.      
    """
    
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e: 	    
	    print(e)


def main():
    try:
        cur, conn = create_database()
    
        drop_tables(cur, conn)
        create_tables(cur, conn)

        conn.close()
    except psycopg2.Error as e: 	    
	    print(e)


if __name__ == "__main__":
    main()