import mysql.connector

class Connect_toD:
    def __init__(self,user,password,host,port,database):
        try:
            self.connection = mysql.connector.connect(user = 'root', 
                                        password = 'Riley123$',
                                        host = '127.0.0.1',
                                        port = '3306',
                                        database = 'PipelineMiniProject')
        except Exception as ex:
            print(f"Error while connecting to {database} as {user}", ex)
        print(f"Sucssesfully connected to {database} as {user}")

    def load_third_party(self,connection, file_path_csv):
        insert_stmt = ("INSERT INTO sales" 
                    "(ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets)" 
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        with self.connection.cursor() as cursor:
            with open(file_path_csv) as csv_file:
                try:
                    for row in csv.reader(csv_file):
                        cursor.execute(insert_stmt,row)
                    connection.commit()
                    cursor.close()
                    return
                
                except Exception as ex:
                    print("Error while reading Sales CSV into PipelineMiniProject", ex)
                print("Succsesfully added Sales CSV into PipelineMiniProject")

    def query_popular_tickets(self,connection):
        sql_statement = 'SELECT * FROM sales ORDER BY num_tickets DESC LIMIT 3'
        cursor = connection.cursor()
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        cursor.close()
        return records

    def show_top3_records(self,records):
        print("Here are the most popular tickets in the past month.")
        print("-"+ records1)
        print("-"+ records2)
        print("-"+  records3)

pipeline = Connect_toDB
pipeline.load_third_party(self.connection,"/Users/kobihancz/Downloads/Springboard Data engineering/Data Pipeline Mini Project /third_party_sales_1.csv")
piepline.query_popular_tickets(self.connection)
pipeline.show_top3_records






 





