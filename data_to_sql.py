import mysql.connector
from mysql.connector import Error
from phonepe_data_extraction import *

def sql_connection():
    host = 'localhost'
    user = 'your_user_id'
    password = 'your_password'
    database = 'your_database_name'
    try:
        # Create a connection object
        conn= mysql.connector.connect(host=host, user=user, password=password, database=database)
        cur = conn.cursor()
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    return cur,conn
# sql_connection()

#***************************************   Creating tables and transferring data to SQL  ******************************************
def agg_tran_table():
    cursor,conn = sql_connection()
    create_table1='''CREATE TABLE IF NOT EXISTS aggregate_transaction (state varchar(100), 
                 year int,
                 quarter int, 
                 transaction_type varchar(100), 
                 transaction_count int, 
                 transaction_amount float);
                 '''

    cursor.execute(create_table1)

    # Inserting Values to aggregate_transaction table
    for i,row in get_data_for_agg_transaction().iterrows(): 
        query = "INSERT INTO aggregate_transaction (state,year,quarter,transaction_type,transaction_count,transaction_amount)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['transaction_type'],row['transaction_count'],row['transaction_amount'])
        cursor.execute(query,values)
        
    # Commit the changes    
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()

def agg_user_table():
    cursor,conn = sql_connection()
    create_table2='''CREATE TABLE IF NOT EXISTS aggregate_user (state VARCHAR(100), 
            year INT,
            quarter INT, 
            brand_name VARCHAR(100), 
            user_count INT, 
            user_percentage FLOAT);
            '''

    cursor.execute(create_table2)

    # Inserting Values to aggregate_user table
    for i,row in get_data_for_agg_user().iterrows(): 
        query = "INSERT INTO aggregate_user (state,year,quarter,brand_name,user_count,user_percentage)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['brand_name'],row['user_count'],row['user_percentage'])
        cursor.execute(query,values)
        
    # Commit and close   
    conn.commit()
    cursor.close()
    conn.close()

def map_tran_table():
    cursor,conn = sql_connection()
    create_table3='''CREATE TABLE IF NOT EXISTS map_transaction (state VARCHAR(100), 
                    year INT,
                    quarter INT, 
                    district VARCHAR(100), 
                    transaction_count INT, 
                    transaction_amount FLOAT);
                    '''

    cursor.execute(create_table3)

    # Inserting Values to map_transaction table
    for i,row in get_data_for_map_transaction().iterrows(): 
        query = "INSERT INTO map_transaction (state,year,quarter,district,transaction_count,transaction_amount)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['district'],row['transaction_count'],row['transaction_amount'])
        cursor.execute(query,values)
        
    # Commit and close      
    conn.commit()
    cursor.close()
    conn.close()

def map_user_table():
    cursor,conn = sql_connection()
    create_table4='''CREATE TABLE IF NOT EXISTS map_user (state VARCHAR(100), 
                 year INT,
                 quarter INT, 
                 district VARCHAR(100), 
                 registered_users INT, 
                 no_of_app_opens INT);
                 '''

    cursor.execute(create_table4)

    # Inserting Values to map_user table
    for i,row in get_data_for_map_user().iterrows(): 
        query = "INSERT INTO map_user (state,year,quarter,district,registered_users,no_of_app_opens)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['district'],row['registered_users'],row['no_of_app_opens'])
        cursor.execute(query,values)
        
    # Commit and close     
    conn.commit()
    cursor.close()
    conn.close()

def top_tran_pincode_table():
    cursor,conn = sql_connection()
    create_table5='''CREATE TABLE IF NOT EXISTS top_tran_pincode (state VARCHAR(100), 
                 year INT,
                 quarter INT, 
                 pincode VARCHAR(100), 
                 transaction_count INT, 
                 transaction_amount FLOAT);
                 '''

    cursor.execute(create_table5)

    # Inserting Values to top_transaction_pincode table
    for i,row in get_data_for_top_transaction_pincodes().iterrows(): 
        query = "INSERT INTO top_tran_pincode (state,year,quarter,pincode,transaction_count,transaction_amount)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['pincode'],row['transaction_count'],row['transaction_amount'])
        cursor.execute(query,values)
        
    # Commit and close    
    conn.commit()
    cursor.close()
    conn.close()

def top_user_pincode_table():
    cursor,conn = sql_connection()
    create_table6='''CREATE TABLE IF NOT EXISTS top_user_pincode (state VARCHAR(100), 
                 year INT,
                 quarter INT, 
                 pincode VARCHAR(100), 
                 registered_user FLOAT);
                 '''

    cursor.execute(create_table6)

    # Inserting Values to top_user_pincode table
    for i,row in get_data_for_top_user_pincodes().iterrows(): 
        query = "INSERT INTO top_user_pincode (state,year,quarter,pincode,registered_user)VALUES (%s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['pincode'],row['registered_user'])
        cursor.execute(query,values)
        
    # Commit and close      
    conn.commit()
    cursor.close()
    conn.close()

def top_tran_district_table():
    cursor,conn = sql_connection()
    create_table7='''CREATE TABLE IF NOT EXISTS top_tran_district (state VARCHAR(100), 
                 year INT,
                 quarter INT, 
                 district VARCHAR(100), 
                 transaction_count INT, 
                 transaction_amount FLOAT);
                 '''

    cursor.execute(create_table7)

    # Inserting Values to top_tran_district table
    for i,row in get_data_for_top_transaction_districts().iterrows(): 
        query = "INSERT INTO top_tran_district (state,year,quarter,district,transaction_count,transaction_amount)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['district'],row['transaction_count'],row['transaction_amount'])
        cursor.execute(query,values)
        
    # Commit and close     
    conn.commit()
    cursor.close()
    conn.close()

def top_user_district_table():
    cursor,conn = sql_connection()
    create_table8='''CREATE TABLE IF NOT EXISTS top_user_district (state VARCHAR(100), 
                 year INT,
                 quarter INT, 
                 district VARCHAR(100), 
                 registered_user FLOAT);
                 '''

    cursor.execute(create_table8)

    # Inserting Values to top_user_district table
    for i,row in get_data_for_top_user_districts().iterrows(): 
        query = "INSERT INTO top_user_district (state,year,quarter,district,registered_user)VALUES (%s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['district'],row['registered_user'])
        cursor.execute(query,values)
        
    # Commit and close     
    conn.commit()
    cursor.close()
    conn.close()

def create_tables():
    agg_tran_table()
    agg_user_table()
    map_tran_table()
    map_user_table()
    top_tran_pincode_table()
    top_user_pincode_table()
    top_tran_district_table()
    top_user_district_table()
    
# create_tables()

#to drop a table
def drop_table():
    cursor,conn = sql_connection()
    cursor = conn.cursor()

    # Define the table name
    table_name = 'aggregate_transaction','aggregate_user'

    # Execute the DROP TABLE query
    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(drop_query)

    # Commit and close  
    conn.commit()
    cursor.close()
    conn.close()
    
# drop_table()
