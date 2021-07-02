import mysql.connector
import pandas as pd


def get_db_connection():
    """
    Establishes connection to database where ticket sales defined
    Arguments:
        None
    Returns:
        connection to sql database
    """
    connection = None
    try:
        connection = mysql.connector.connect(user='root',
        password='db123',
        host='127.0.0.1',
        port='3306',
        database='new_schema')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection


def load_third_party(connection, file_path_csv):
    """
    Establishes connection to database and writes csv from path defined into sales table
    Arguments:
       connection -- connection to db (connection)
       file_path_csv -- path (str)
    Returns:
       None
       """
    cursor = connection.cursor()
    data = pd.read_csv(file_path_csv)
    try:
        for i, row in data.iterrows():
            sql = "INSERT INTO new_schema.ticket_sales VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
        connection.commit()
        cursor.close()
    except mysql.connector.IntegrityError:
        print("-------Already populated database-------\n")





def query_popular_ticket_sales(connection):
    """
    Queries most popular movies based on number of ticket sales in the last month
    Arguments:
        connection -- connection to db (connection)
    Returns:
       tuple of query results
       """
    # Get the most popular ticket in the past month
    sql_statement = r"SELECT event_name, SUM(num_tickets) AS tickets_sold FROM ticket_sales GROUP BY event_name ORDER BY tickets_sold DESC"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    print('Here are the most popular tickets in the past month:')
    return records


def print_tuples(records):
    """
    Prints tuple of records in an easy to read format
    Arguments:
        records -- query results (tuple)
    Returns:
        prints records
    """
    for i in records:
        print(*i)
    print('\n')


def main():
    """
    Main execution script
    """
    file = 'third_party_sales_1.csv'
    connection = get_db_connection()
    load_third_party(connection=connection, file_path_csv=file)
    records = query_popular_ticket_sales(connection)
    print_tuples(records)


if __name__=='__main__':
    main()



