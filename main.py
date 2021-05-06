import mysql.connector

def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(user = 'root', 
                                    password = 'Riley123$',
                                    host = '127.0.0.1',
                                    port = '3306',
                                    database = 'PipelineMiniProject')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)

    return connection

def load_third_party(connection, file_path_csv ):
    cursor = connection.cursor()
    insert_stmt = ("INSERT INTO sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets) " 
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    )
    for record in file_path_csv:
        cursor.execute(insert_stmt,record)
    
    connection.commit()
    cursor.close()
    return

def query_popular_tickets(connection):
    sql_statement = 'SELECT * FROM sales ORDER BY num_tickets DESC LIMIT 3'
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records





get_db_connection()
# load_third_party(connection, '/Users/kobihancz/Downloads/Springboard Data engineering/Data Pipeline Mini Project /third_party_sales_1.csv')
# query_popular_tickets(connection)
 

 





