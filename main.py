import mysql.connector
import csv

class DatabseHandler:
    def __init__(self,user,password,database,host = '127.0.0.1',port = '3306'):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        try:
            self.connection = mysql.connector.connect(user = self.user, 
                                        password = self.password,
                                        host = self.host,
                                        port = self.port,
                                        database = self.database)
        except Exception as ex:
            print(f"Error while connecting to {database} as {user}", ex)
        print(f"Sucssesfully connected to {database} as {user}")

    def load_third_party(self,file_path_csv):
        insert_stmt = ("INSERT INTO sales" 
                    "(ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets)" 
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        with self.connection.cursor() as cursor:
            with open(file_path_csv) as csv_file:
                try:
                    for row in csv.reader(csv_file):
                        cursor.execute(insert_stmt,row)
                
                except Exception as ex:
                    print("Error while reading Sales CSV into PipelineMiniProject", ex)
        print("Successfully added Sales CSV into PipelineMiniProject")
        
        self.connection.commit()

    def query_popular_tickets(self):
        sql_statement = 'SELECT event_name, SUM(num_tickets) FROM sales GROUP BY event_name ORDER BY SUM(num_tickets) DESC LIMIT 3'
        with self.connection.cursor() as cursor:
            cursor.execute(sql_statement)
            records = cursor.fetchall()
            return records

    def show_top3_records(self,records):
        print("Here are the most popular tickets in the past month.")
        print("-"+records[0]+"("+records[1]+"tickets sold)")
        print("-"+records[2]+"("+records[3]+"tickets sold)")
        print("-"+records[4]+"("+records[5]+"tickets sold)")

pipeline = DatabseHandler('root', 'Riley123$','PipelineMiniProject')
print(pipeline)
pipeline.load_third_party("/Users/kobihancz/Downloads/Springboard Data engineering/Data Pipeline Mini Project /third_party_sales_1.csv")
pipeline.query_popular_tickets()
pipeline.show_top3_records(pipeline.query_popular_tickets())








 





