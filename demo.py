import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

conn1 = mysql.connector.connect(host='localhost',user='root',password='Akash',database='source_db')
conn2 = mysql.connector.connect(host='localhost',user='root',password='Akash',database='target_db')

if(conn1.is_connected() and conn2.is_connected()):
    print("Both database connected successfully");
cursor = conn1.cursor()

engine1 = create_engine('mysql+pymysql://root:Akash@localhost/source_db')

query1 = "select * from sourcet_table where date=CURDATE() - INTERVAL 1 DAY"
df = pd.read_sql(query1, engine1)

if not df.empty:
    engine2 =create_engine('mysql+pymysql://root:Akash@localhost/target_db')

    ids_to_check = df['id'].tolist()

    if ids_to_check:
        query2 = "SELECT COUNT(*) FROM target_table WHERE id IN ({})".format(','.join(map(str, ids_to_check)))


        with engine2.connect() as conn2:
            result = conn2.execute(text(query2)).fetchone()

        if result[0] == 0:  
            df.to_sql('target_table', engine2, if_exists='append', index=False)
            print("New data inserted")
        else:
            print("Date already present in target table")

    else:
        df.to_sql('target_table', engine2, if_exists='append', index=False)

else:
    print("Yesterday data not present")

conn1.close()
conn2.close()