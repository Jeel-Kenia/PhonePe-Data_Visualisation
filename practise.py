import streamlit as st
import pandas as pd
import os
import json
import plotly.express as px
import mysql.connector
from mysql.connector import Error
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_to_sql import *


#####Creating dataframes from cloned data######

#Fetching data from aggregate transaction
def get_data_for_agg_transaction():
    path1 = "/Users/jk/Documents/PhonePe_project/pulse/data/aggregated/transaction/country/india/state/"
    list_of_agg_trans_state = os.listdir(path1)

    #convert data into dataframe
    agg_trans = {'state' : [], 
                'year' : [],
                'quarter' : [],
                'transaction_type' : [],
                'transaction_count' : [],
                'transaction_amount' : []
            }
    #Load data inside quarter in json format
    for state in list_of_agg_trans_state:
        state_path = path1 + state + "/"        
        # Skip if the item is not a directory
        if not os.path.isdir(state_path):
            continue            
        agg_trans_yr = os.listdir(state_path)
        
        for year in agg_trans_yr:
            year_path = state_path + year + "/"            
            if not os.path.isdir(year_path):
                continue                
            agg_trans_quart = os.listdir(year_path)
            
            for quarter in agg_trans_quart:
                quarter_path = year_path + quarter                
                if not os.path.isfile(quarter_path):
                    continue
                    
                file1 = open(quarter_path , 'r')
                data1 = json.load(file1)

                for item in data1['data']['transactionData']:
                    name = item['name']
                    count = item['paymentInstruments'][0]['count']
                    amount = item['paymentInstruments'][0]['amount']
                    agg_trans['state'].append(state)
                    agg_trans['year'].append(year)
                    agg_trans['quarter'].append(int(quarter.strip('.json')))
                    agg_trans['transaction_type'].append(name)
                    agg_trans['transaction_count'].append(count)
                    agg_trans['transaction_amount'].append(amount)
                    
    df1 = pd.DataFrame(agg_trans)
    df1['state'] = df1['state'].str.replace('-', ' ')
    df1['state'] = df1['state'].str.title()
    df1['state'] = df1['state'].str.replace('and ', '& ')
    df1['state'] = df1['state'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    return df1

#Fetching data for aggregate user
def get_data_for_agg_user():
    path2 = "/Users/jk/Documents/PhonePe_project/pulse/data/aggregated/user/country/india/state/"
    list_of_agg_user_state = os.listdir(path2)

    agg_user = {'state' : [],
                'year' : [],
                'quarter' : [],
                'brand_name' : [],
                'user_count' : [],
                'user_percentage' : []
            }

    for state in list_of_agg_user_state:
        state_path = path2 + state + "/"       
        if not os.path.isdir(state_path):
            continue            
        agg_user_yr = os.listdir(state_path)
        
        for year in agg_user_yr:
            year_path = state_path + year + "/"
            if not os.path.isdir(year_path):
                continue
            agg_user_quart = os.listdir(year_path)           
            for quarter in agg_user_quart:
                quarter_path = year_path + quarter               
                if not os.path.isfile(quarter_path):
                    continue
                    
                file2 = open(quarter_path , 'r')
                data2 = json.load(file2)

                if data2['data']['usersByDevice'] is not None:
                    for item in data2['data']['usersByDevice']:
                            brand = item['brand']
                            count = item['count']
                            percent = item['percentage']

                            agg_user['state'].append(state)
                            agg_user['year'].append(year)
                            agg_user['quarter'].append(int(quarter.strip('.json')))
                            agg_user['brand_name'].append(brand)
                            agg_user['user_count'].append(count)
                            agg_user['user_percentage'].append(round(percent * 100,2))


    df2 = pd.DataFrame(agg_user)
    df2['state'] = df2['state'].str.replace('-', ' ')
    df2['state'] = df2['state'].str.title()
    df2['state'] = df2['state'].str.replace('and ', '& ')
    df2['state'] = df2['state'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    return df2

#Fetching data from map transaction
def get_data_for_map_transaction():
    path3 = "/Users/jk/Documents/PhonePe_project/pulse/data/map/transaction/hover/country/india/state/"
    list_of_map_trans_state = os.listdir(path3)

    map_trans = {'state' : [],
                'year' : [],
                'quarter' : [],
                'district' : [],
                'transaction_count' : [],
                'transaction_amount' : []
            }

    for state in list_of_map_trans_state:
        state_path = path3 + state + "/"
        if not os.path.isdir(state_path):
            continue        
        map_trans_yr = os.listdir(state_path)
                
        for year in map_trans_yr:
            year_path = state_path + year + "/"            
            if not os.path.isdir(year_path):
                continue
            map_trans_quart = os.listdir(year_path)
            
            for quarter in map_trans_quart:
                quarter_path = year_path + quarter
                if not os.path.isfile(quarter_path):
                    continue
                
                file3 = open(quarter_path , 'r')
                data3 = json.load(file3)
                
                for item in data3['data']['hoverDataList']:
                    name = item['name']
                    count = item['metric'][0]['count']
                    amount = item['metric'][0]['amount']
                    map_trans['state'].append(state)
                    map_trans['year'].append(year)
                    map_trans['quarter'].append(int(quarter.strip('.json')))
                    map_trans['district'].append(name)
                    map_trans['transaction_count'].append(count)
                    map_trans['transaction_amount'].append(amount)
                    
    df3 = pd.DataFrame(map_trans)
    df3['state'] = df3['state'].str.replace('-', ' ')
    df3['state'] = df3['state'].str.title()
    df3['state'] = df3['state'].str.replace('and ', '& ')
    df3['state'] = df3['state'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df3['district'] = df3['district'].str.title()
    return df3

#Fetching data for map user
def get_data_for_map_user():
    path4 = "/Users/jk/Documents/PhonePe_project/pulse/data/map/user/hover/country/india/state/"
    list_of_map_user_state = os.listdir(path4)

    map_user = {'state' : [],
                'year' : [],
                'quarter' : [],
                'district' : [],
                'registered_users' : [],
                'no_of_app_opens' : []
            }

    for state in list_of_map_user_state:
        state_path = path4 + state + "/"
        if not os.path.isdir(state_path):
            continue
        map_user_yr = os.listdir(state_path)
        
        for year in map_user_yr:
            year_path = state_path + year + "/"
            if not os.path.isdir(year_path):
                continue
            map_user_quart = os.listdir(year_path)
            
            for quarter in map_user_quart:
                quarter_path = year_path + quarter
                if not os.path.isfile(quarter_path):
                    continue
                
                file4 = open(quarter_path , 'r')
                data4 = json.load(file4)
                
                for item in data4['data']['hoverData'].items():
                    dist = item[0]
                    users = item[1]['registeredUsers']
                    app_opens = item[1]['appOpens']
                    map_user['state'].append(state)
                    map_user['year'].append(year)
                    map_user['quarter'].append(int(quarter.strip('.json')))
                    map_user['district'].append(dist)
                    map_user['registered_users'].append(users)
                    map_user['no_of_app_opens'].append(app_opens)
                    
    df4 = pd.DataFrame(map_user)
    df4['state'] = df4['state'].str.replace('-', ' ')
    df4['state'] = df4['state'].str.title()
    df4['state'] = df4['state'].str.replace('and ', '& ')
    df4['state'] = df4['state'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df4['district'] = df4['district'].str.title()
    return df4

#Fetching data from top transaction according to pincodes
def get_data_for_top_transaction_pincodes():
    path5 = "/Users/jk/Documents/PhonePe_project/pulse/data/top/transaction/country/india/state/"
    list_of_top_trans_state = os.listdir(path5)

    top_trans = {'state' : [],
                'year' : [],
                'quarter' : [],
                'pincode' : [],
                'transaction_count' : [],
                'transaction_amount' : []
            }

    for state in list_of_top_trans_state:
        state_path = path5 + state + "/"
        if not os.path.isdir(state_path):
            continue
        top_trans_yr = os.listdir(state_path)
        
        for year in top_trans_yr:
            year_path = state_path + year + "/"
            if not os.path.isdir(year_path):
                continue
            top_trans_quart = os.listdir(year_path)
            
            for quarter in top_trans_quart:
                quarter_path = year_path + quarter
                if not os.path.isfile(quarter_path):
                    continue
                
                file5 = open(quarter_path , 'r')
                data5 = json.load(file5)
                
                for item in data5['data']['pincodes']:
                    pin = item['entityName']
                    count = item['metric']['count']
                    amount = item['metric']['amount']
                    top_trans['state'].append(state)
                    top_trans['year'].append(year)
                    top_trans['quarter'].append(int(quarter.strip('.json')))
                    top_trans['pincode'].append(pin)
                    top_trans['transaction_count'].append(count)
                    top_trans['transaction_amount'].append(amount)
                    
    df5 = pd.DataFrame(top_trans)
    df5['state'] = df5['state'].str.replace('-', ' ')
    df5['state'] = df5['state'].str.title()
    df5['state'] = df5['state'].str.replace('and ', '& ')
    df5['state'] = df5['state'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    return df5

#Fetching data for top user according to pincodes
def get_data_for_top_user_pincodes():
    path6 = "/Users/jk/Documents/PhonePe_project/pulse/data/top/user/country/india/state/"
    list_of_top_user_state = os.listdir(path6)

    top_user = {'state' : [],
                'year' : [],
                'quarter' : [],
                'pincode' : [],
                'registered_user' : [],
                }

    for state in list_of_top_user_state:
        state_path = path6 + state + "/"
        if not os.path.isdir(state_path):
            continue
        top_user_yr = os.listdir(state_path)
        
        for year in top_user_yr:
            year_path = state_path + year + "/"
            if not os.path.isdir(year_path):
                continue
            top_user_quart = os.listdir(year_path)
            
            for quarter in top_user_quart:
                quarter_path = year_path + quarter
                if not os.path.isfile(quarter_path):
                    continue
                
                file6 = open(quarter_path , 'r')
                data6 = json.load(file6)
                
                for item in data6['data']['pincodes']:
                    pin = item['name']
                    user = item['registeredUsers']
                    top_user['state'].append(state)
                    top_user['year'].append(year)
                    top_user['quarter'].append(int(quarter.strip('.json')))
                    top_user['pincode'].append(pin)
                    top_user['registered_user'].append(user)
                    
    df6 = pd.DataFrame(top_user)
    df6['state'] = df6['state'].str.replace('-', ' ')
    df6['state'] = df6['state'].str.title()
    df6['state'] = df6['state'].str.replace('and ', '& ')
    df6['state'] = df6['state'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    return df6

#Fetching data from top transaction according to districts
def get_data_for_top_transaction_districts():
    path7 = "/Users/jk/Documents/PhonePe_project/pulse/data/top/transaction/country/india/state/"
    list_of_top_trans_dist_state = os.listdir(path7)

    top_trans_dist = {'state' : [],
                'year' : [],
                'quarter' : [],
                'district' : [],
                'transaction_count' : [],
                'transaction_amount' : []
            }

    for state in list_of_top_trans_dist_state:
        state_path = path7 + state + "/"
        if not os.path.isdir(state_path):
            continue
        top_trans_dist_yr = os.listdir(state_path)
        
        for year in top_trans_dist_yr:
            year_path = state_path + year + "/"
            if not os.path.isdir(year_path):
                continue
            top_trans_dist_quart = os.listdir(year_path)
            
            for quarter in top_trans_dist_quart:
                quarter_path = year_path + quarter
                if not os.path.isfile(quarter_path):
                    continue
                
                file7 = open(quarter_path , 'r')
                data7 = json.load(file7)
                
                for item in data7['data']['districts']:
                    dist = item['entityName']
                    count = item['metric']['count']
                    amount = item['metric']['amount']
                    top_trans_dist['state'].append(state)
                    top_trans_dist['year'].append(year)
                    top_trans_dist['quarter'].append(int(quarter.strip('.json')))
                    top_trans_dist['district'].append(dist)
                    top_trans_dist['transaction_count'].append(count)
                    top_trans_dist['transaction_amount'].append(amount)
                    
    df7 = pd.DataFrame(top_trans_dist)
    df7['state'] = df7['state'].str.replace('-', ' ')
    df7['state'] = df7['state'].str.title()
    df7['state'] = df7['state'].str.replace('and ', '& ')
    df7['state'] = df7['state'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df7['district'] = df7['district'].str.title()
    return df7

#Fetching data from top user according to districts
def get_data_for_top_user_districts():
    path8 = "/Users/jk/Documents/PhonePe_project/pulse/data/top/user/country/india/state/"
    list_of_top_user_dist_state = os.listdir(path8)

    top_user_dist = {'state' : [],
                    'year' : [],
                    'quarter' : [],
                    'district' : [],
                    'registered_user' : [],
                    }

    for state in list_of_top_user_dist_state:
        state_path = path8 + state + "/"
        if not os.path.isdir(state_path):
            continue
        top_user_dist_yr = os.listdir(state_path)
        
        for year in top_user_dist_yr:
            year_path = state_path + year + "/"
            if not os.path.isdir(year_path):
                continue
            top_user_dist_quart = os.listdir(year_path)
            
            for quarter in top_user_dist_quart:
                quarter_path = year_path + quarter
                if not os.path.isfile(quarter_path):
                    continue
                
                file8 = open(quarter_path , 'r')
                data8 = json.load(file8)
                
                for item in data8['data']['districts']:
                    dist = item['name']
                    user = item['registeredUsers']
                    top_user_dist['state'].append(state)
                    top_user_dist['year'].append(year)
                    top_user_dist['quarter'].append(int(quarter.strip('.json')))
                    top_user_dist['district'].append(dist)
                    top_user_dist['registered_user'].append(user)
                    
    df8 = pd.DataFrame(top_user_dist)
    df8['state'] = df8['state'].str.replace('-', ' ')
    df8['state'] = df8['state'].str.title()
    df8['state'] = df8['state'].str.replace('and ', '& ')
    df8['state'] = df8['state'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
    df8['district'] = df8['district'].str.title()
    return df8

def get_all_data():
    get_data_for_agg_transaction()
    get_data_for_agg_user()
    get_data_for_map_transaction()
    get_data_for_map_user()
    get_data_for_top_transaction_pincodes()
    get_data_for_top_user_pincodes()
    get_data_for_top_transaction_districts()
    get_data_for_top_user_districts()
# get_all_data()
def describe_data():
    print(get_data_for_agg_transaction().describe())
    print(get_data_for_agg_user().describe())
    print(get_data_for_map_transaction().describe())
    print(get_data_for_map_user().describe())
    print(get_data_for_top_transaction_pincodes().describe())
    print(get_data_for_top_user_pincodes().describe())
    print(get_data_for_top_transaction_districts().describe())
    print(get_data_for_top_user_districts().describe())
    
def null_data():
    print(get_data_for_agg_transaction().isnull().sum())
    print(get_data_for_agg_user().isnull().sum())
    print(get_data_for_map_transaction().isnull().sum())
    print(get_data_for_map_user().isnull().sum())
    print(get_data_for_top_transaction_pincodes().isnull().sum())
    print(get_data_for_top_user_pincodes().isnull().sum())
    print(get_data_for_top_transaction_districts().isnull().sum())
    print(get_data_for_top_user_districts().isnull().sum())

def data_shape():
    print(get_data_for_agg_transaction().shape)
    print(get_data_for_agg_user().shape)
    print(get_data_for_map_transaction().shape)
    print(get_data_for_map_user().shape)
    print(get_data_for_top_transaction_pincodes().shape)
    print(get_data_for_top_user_pincodes().shape)
    print(get_data_for_top_transaction_districts().shape)
    print(get_data_for_top_user_districts().shape)

def data_to_csv():
    print(get_data_for_agg_transaction().to_csv('aggregate_transaction_csv', index=False ))
    print(get_data_for_agg_user().to_csv('aggregate_user_csv', index=False ))
    print(get_data_for_map_transaction().to_csv('map_transaction_csv', index=False ))
    print(get_data_for_map_user().to_csv('map_user_csv', index=False ))
    print(get_data_for_top_transaction_pincodes().to_csv('top_transaction_pincode_csv', index=False ))
    print(get_data_for_top_user_pincodes().to_csv('top_user_pincode_csv', index=False ))
    print(get_data_for_top_transaction_districts().to_csv('top_transaction_district_csv', index=False ))
    print(get_data_for_top_user_districts().to_csv('top_user_district_csv', index=False ))
data_to_csv()
########Establishing connection with MySQL###########

def sql_connection():
    host = 'localhost'
    user = 'root'
    password = 'Jeel@1414'
    database = 'PhonePe'
    try:
        # Create a connection object
        conn= mysql.connector.connect(host=host, user=user, password=password, database=database)
        cur = conn.cursor()
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    return cur,conn
# sql_connection()

#####create tables and transfer data to sql
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

    # Inserting Values to aggregate_transaction table
    for i,row in get_data_for_agg_user().iterrows(): 
        query = "INSERT INTO aggregate_user (state,year,quarter,brand_name,user_count,user_percentage)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['brand_name'],row['user_count'],row['user_percentage'])
        cursor.execute(query,values)
        
    # Commit the changes    
    conn.commit()

    # Close the cursor and connection
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

    # Inserting Values to aggregate_transaction table
    for i,row in get_data_for_map_transaction().iterrows(): 
        query = "INSERT INTO map_transaction (state,year,quarter,district,transaction_count,transaction_amount)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['district'],row['transaction_count'],row['transaction_amount'])
        cursor.execute(query,values)
        
    # Commit the changes    
    conn.commit()

    # Close the cursor and connection
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

    # Inserting Values to aggregate_transaction table
    for i,row in get_data_for_map_user().iterrows(): 
        query = "INSERT INTO map_user (state,year,quarter,district,registered_users,no_of_app_opens)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['district'],row['registered_users'],row['no_of_app_opens'])
        cursor.execute(query,values)
        
    # Commit the changes    
    conn.commit()

    # Close the cursor and connection
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

    # Inserting Values to aggregate_transaction table
    for i,row in get_data_for_top_transaction_pincodes().iterrows(): 
        query = "INSERT INTO top_tran_pincode (state,year,quarter,pincode,transaction_count,transaction_amount)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['pincode'],row['transaction_count'],row['transaction_amount'])
        cursor.execute(query,values)
        
    # Commit the changes    
    conn.commit()

    # Close the cursor and connection
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

    # Inserting Values to aggregate_transaction table
    for i,row in get_data_for_top_user_pincodes().iterrows(): 
        query = "INSERT INTO top_user_pincode (state,year,quarter,pincode,registered_user)VALUES (%s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['pincode'],row['registered_user'])
        cursor.execute(query,values)
        
    # Commit the changes    
    conn.commit()

    # Close the cursor and connection
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

    # Inserting Values to aggregate_transaction table
    for i,row in get_data_for_top_transaction_districts().iterrows(): 
        query = "INSERT INTO top_tran_district (state,year,quarter,district,transaction_count,transaction_amount)VALUES (%s, %s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['district'],row['transaction_count'],row['transaction_amount'])
        cursor.execute(query,values)
        
    # Commit the changes    
    conn.commit()

    # Close the cursor and connection
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

    # Inserting Values to aggregate_transaction table
    for i,row in get_data_for_top_user_districts().iterrows(): 
        query = "INSERT INTO top_user_district (state,year,quarter,district,registered_user)VALUES (%s, %s, %s, %s, %s)"
        values=(row['state'],row['year'],row['quarter'],row['district'],row['registered_user'])
        cursor.execute(query,values)
        
    # Commit the changes    
    conn.commit()

    # Close the cursor and connection
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
##to drop a table
def drop_table():
    cursor,conn = sql_connection()
    cursor = conn.cursor()

    # Define the table name
    table_name = 'aggregate_transaction','aggregate_user'

    # Execute the DROP TABLE query
    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(drop_query)

    # Commit the changes
    conn.commit()

    # Close the cursor and connection
    cursor.close()
    conn.close()
# drop_table()


##########     run queries    ##################
def format_amount(amount):
    crore = amount / 10000000
    formatted_amount = f"Rs. {crore:,.2f}cr"
    return formatted_amount

def format_count(count):
    formatted_count = f"{count:,}"
    return formatted_count

#total transaction count and transaction amount for all states for a particular year and quarter
def total_tran(year,quarter):
    cursor, conn = sql_connection()
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT state, sum(transaction_amount) as transaction_amount, sum(transaction_count) as transaction_count FROM aggregate_transaction WHERE year = %s AND quarter = %s GROUP BY state"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['State', 'Transaction Amount', 'Transaction Count'])
    # df['Transaction Count'] = df['Transaction Count'].apply(format_count)
    # df['Transaction Amount'] = df['Transaction Amount'].apply(format_amount)
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    return df

#Total Transaction count for a particular type for a particular year and quarter
def total_tran_count(year,quarter):
    cursor, conn = sql_connection()
    # Define the table name
    table_name = 'aggregate_transaction'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT transaction_type, sum(transaction_count) as transaction_count FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY transaction_type"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Transaction Type', 'Transaction Count'])
    df['Transaction Count'] = df['Transaction Count'].apply(format_count)
    # Close the cursor and connection
    cursor.close()
    conn.close()

    st.dataframe(df)

    # Display the fetched data as a styled table
    # st.table(df.style.set_properties(**{'background-color': 'pink',
    #                                     'color': 'black',
    #                                     'border': '1px solid black'}))

#Total Transaction count for a particular type for a particular year and quarter(pie)
def total_tran_amount_pie(year,quarter):
    cursor, conn = sql_connection()
    # Define the table name
    table_name = 'aggregate_transaction'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT transaction_type, sum(transaction_amount) as transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY transaction_type"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Transaction Type', 'Transaction Amount'])
    # df['Transaction Amount'] = df['Transaction Amount'].apply(format_amount)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    # Create a pie chart using plotly
    fig = px.pie(df, values='Transaction Amount', names='Transaction Type', title = 'Transaction Amount for each type payment', color_discrete_sequence=px.colors.qualitative.Vivid)
    fig.update_layout(width=400, height=400) # Specify the angle at which the rotation should start (in degrees)
    fig.update_traces(rotation=90)
    fig.update_traces(hole=0.4)
    # Display the pie chart
    st.plotly_chart(fig,)

#Total transaction count and transaction amount for a particular year and quarter
def total_tran_count_and_amount(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'aggregate_transaction'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT sum(transaction_count) as transaction_count, sum(transaction_amount) as transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Transaction Count', 'Transaction Amount'])
    df['Transaction Count'] = pd.to_numeric(df['Transaction Count'], errors='coerce')
    df['Transaction Amount'] = pd.to_numeric(df['Transaction Amount'], errors='coerce')

    df['Avg. Transaction Value'] = df['Transaction Amount'] / df['Transaction Count']
    df['Avg. Transaction Value'] = df['Avg. Transaction Value'].apply(lambda x: f'Rs. {x:,.2f}')
    df['Transaction Amount'] = df['Transaction Amount'] / 10000000
    df['Transaction Amount'] = df['Transaction Amount'].apply(lambda x: f'Rs. {x:,.2f} cr')
    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)
    
#Top 10 states with maximum transaction amount for a particular year and quarter
def top_states_with_max_count(year,quarter):
    cursor, conn = sql_connection()
    # Define the table name
    table_name = 'top_tran_district'
    
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT state,sum(transaction_count) as transaction_count FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY state ORDER BY transaction_count DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['State', 'Transaction Count'])
    df['Transaction Count'] = df['Transaction Count'].apply(format_count)

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#Top 10 states with maximum transaction amount for a particular year and quarter(bar)
def top_states_with_max_count_bar(year,quarter):
    cursor, conn = sql_connection()
    # Define the table name
    table_name = 'top_tran_district'
    
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT state,sum(transaction_amount) as transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY state ORDER BY transaction_amount DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['State', 'Transaction Amount'])
    # df['Transaction Amount'] = df['Transaction Amount'].apply(format_amount)


    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x="Transaction Amount", y="State", orientation='h', color_continuous_scale= 'Aggrnyl', color= 'Transaction Amount', hover_data=["State", "Transaction Amount"], height=400, title= f'Top States with maximum Transaction Amount in Quarter  {quarter} of  {year}')
    st.plotly_chart(fig)

#top 10 districts with maximim transaction count for a particular year and quarter
def top_districts_with_max_count(year,quarter):
    cursor, conn = sql_connection()
    # Define the table name
    table_name = 'top_tran_district'
    
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT district, transaction_count FROM {table_name} WHERE year = %s AND quarter = %s ORDER BY transaction_count DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['District', 'Transaction Count'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#top 10 districts with maximim transaction count for a particular year and quarter(bar)
def top_districts_with_max_count_bar(year,quarter):
    cursor, conn = sql_connection()
    # Define the table name
    table_name = 'top_tran_district'
    
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT district, transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s ORDER BY transaction_amount DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['District', 'Transaction Amount'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x="Transaction Amount", y="District", orientation='h', color_continuous_scale= 'Aggrnyl',color= 'Transaction Amount', hover_data=["District", "Transaction Amount"], height=400, title= f'Top Districts with maximum Transaction Amount in Quarter  {quarter} of  {year}')
    st.plotly_chart(fig)

#top 10 pincodes with maximum transaction count for a particular year and quarter
def top_pincodes_with_max_count(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'top_tran_pincode'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT pincode, transaction_count FROM {table_name} WHERE year = %s AND quarter = %s ORDER BY transaction_amount DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Pincode', 'Transaction Count'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#top 10 pincodes with maximum transaction count for a particular year and quarter(bar)
def top_pincodes_with_max_count_bar(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'top_tran_pincode'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT pincode, transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s ORDER BY transaction_amount DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Pincode', 'Transaction Amount'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x="Transaction Amount", y="Pincode", orientation='h', color_continuous_scale= 'Aggrnyl',color= 'Transaction Amount', hover_data=["Pincode", "Transaction Amount"], height=400, title= f'Top Pincodes with maximum Transaction Amount in Quarter  {quarter} of  {year}')
    fig.update_layout(yaxis={'type': 'category'}, xaxis={'title': 'Transaction Amount'},)
    st.plotly_chart(fig)

#total registered users for all states for a particular year and quarter
def total_user(year,quarter):
    cursor, conn = sql_connection()
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT state, sum(registered_users) as registered_users FROM map_user WHERE year = %s AND quarter = %s GROUP BY state"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['State', 'Registered Users'])
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    return df

#Total number of registered users and app opens for a particular quarter and year.
def total_user_app_opens(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'map_user'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT sum(registered_users) as registered_users, sum(no_of_app_opens) as no_of_app_opens FROM {table_name} WHERE year = %s AND quarter = %s"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Registered Users', 'No. of App Opens'])
    df['Registered Users'] = df['Registered Users'].apply(format_count)
    df['No. of App Opens'] = df['No. of App Opens'].apply(format_count)

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#Total number of registered users and app opens for a particular quarter and year(pie)
def total_user_app_opens_pie(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'map_user'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT sum(registered_users) as registered_users, sum(no_of_app_opens) as no_of_app_opens FROM {table_name} WHERE year = %s AND quarter = %s"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Registered Users', 'No. of App Opens'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    colors = ['brown', 'cyan']
    # Create the pie chart
    fig = go.Figure(data=go.Pie(labels=['Registered Users', 'No. of App Opens'],
                                values=[df['Registered Users'].iloc[0], df['No. of App Opens'].iloc[0]],
                                marker=dict(colors=colors),
                                hole=0.4))

    # Customize the pie chart layout
    fig.update_layout(width=400, height=400, title='Registered Users vs No. of App Opens')

    # fig = px.pie(df, names=['Registered Users', 'No. of App Opens'], values=[df['Registered Users'].iloc[0], df['No. of App Opens'].iloc[0]], title='Registered Users vs No. of App Opens')

    # # Customize the pie chart layout
    # fig.update_traces(hole=0.4)
    # fig.update_layout(width=400, height=400)

    # Display the pie chart
    st.plotly_chart(fig)

#Top 10 states with maximum registered users
def top_states_with_max_users(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_district'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT state, sum(registered_user) as registered_user FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY state ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['State', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    
    st.dataframe(df)

#Top 10 states with maximum registered users(bar)
def top_states_with_max_users_bar(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_district'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT state, sum(registered_user) as registered_user FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY state ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['State', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x='Registered Users', y='State', orientation='h', color_continuous_scale= 'Aggrnyl',color= 'Registered Users', hover_data=['State', 'Registered Users'], height=400, title= f'Top States with maximum Users in Quarter  {quarter} of  {year}')
    st.plotly_chart(fig)

#Top 10 district with max number of registered users for a particular year and quarter
def top_districts_with_max_users(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_district'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT district, registered_user FROM {table_name} WHERE year = %s AND quarter = %s ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['District', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()

    st.dataframe(df)

#Top 10 district with max number of registered users for a particular year and quarter(bar)
def top_districts_with_max_users_bar(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_district'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT district, registered_user FROM {table_name} WHERE year = %s AND quarter = %s ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['District', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x='Registered Users', y='District', orientation='h', color_continuous_scale= 'Aggrnyl',color= 'Registered Users', hover_data=['District', 'Registered Users'], height=400, title= f'Top Districts with maximum Users in Quarter  {quarter} of  {year}')
    st.plotly_chart(fig)

#top 10 pincodes with maximum number of regsitered users for a particular year and quarter
def top_pincodes_with_max_users(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_pincode'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT pincode, registered_user FROM {table_name} WHERE year = %s AND quarter = %s ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Pincode', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#top 10 pincodes with maximum number of regsitered users for a particular year and quarter(bar)
def top_pincodes_with_max_users_bar(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_pincode'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT pincode, registered_user FROM {table_name} WHERE year = %s AND quarter = %s ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Pincode', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x='Registered Users', y='Pincode', orientation='h', color_continuous_scale= 'Aggrnyl',color= 'Registered Users', hover_data=['Pincode', 'Registered Users'], height=400, title= f'Top Pincodes with maximum Users in Quarter  {quarter} of  {year}')
    fig.update_layout(yaxis={'type': 'category'}, xaxis={'title': 'Transaction Count'},)
    st.plotly_chart(fig)

#total transactions and amount for a particular state for a particular year and quarter
def total_tran_for_a_state(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'aggregate_transaction'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT sum(transaction_count) as transaction_count, sum(transaction_amount) as transaction_amount FROM {table_name} WHERE year = %s AND state = %s AND quarter = %s"
    cursor.execute(select_query, (year, state, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Transaction Count', 'Transaction Amount'])
    df['Transaction Count'] = df['Transaction Count'].apply(format_count)
    df['Transaction Amount'] = df['Transaction Amount'].apply(format_amount)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    st.dataframe(df)

#Transaction types and their count for a particular state in a particular year and quarter
def tran_count_for_a_state(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'aggregate_transaction'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT transaction_type, transaction_count FROM {table_name} WHERE year = %s AND state = %s AND quarter = %s"
    cursor.execute(select_query, (year, state, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Transaction Type', 'Transaction Count'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#total registered users and app opens for a particular state for a particular year and quarter
def reg_users_for_a_state(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'map_user'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT sum(registered_users) as registered_users, sum(no_of_app_opens) as no_of_app_opens FROM {table_name} WHERE year = %s AND state = %s AND quarter = %s GROUP BY state"
    cursor.execute(select_query, (year, state, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Registered Users', 'No. of App Opens'])
    df['Registered Users'] = df['Registered Users'].apply(format_count)
    df['No. of App Opens'] = df['No. of App Opens'].apply(format_count)
    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#Transaction types and their count for a particular state in a particular year and quarter(pie)
def tran_amount_for_a_state_pie(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'aggregate_transaction'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT transaction_type, transaction_amount FROM {table_name} WHERE year = %s AND state = %s AND quarter = %s"
    cursor.execute(select_query, (year, state, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Transaction Type', 'Transaction Amount'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.pie(df, values='Transaction Amount', names='Transaction Type', color_discrete_sequence=px.colors.qualitative.Set2, labels={'Transaction Type':'Transaction Type'})
    st.plotly_chart(fig)

#top 10 districts with maximum count for a particular state for a particular year and quarter
def top_districts_count_for_a_state(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'top_tran_district'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT district, transaction_count, transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s AND state = %s ORDER BY transaction_amount DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter, state))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['District', 'Transaction Amount', 'Transaction Count'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#top 10 districts with maximum count for a particular state for a particular year and quarter(bar)
def top_districts_count_for_a_state_bar(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'top_tran_district'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT district, transaction_count, transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s AND state = %s ORDER BY transaction_amount DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter, state))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['District', 'Transaction Amount', 'Transaction Count'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x='District', y='Transaction Count', hover_data=['District', 'Transaction Count', 'Transaction Amount'], title = f'Top 10 Districts with maximum Transactions in {state} ' , color='Transaction Count', color_continuous_scale='thermal', labels={'Districts':'Districts', 'Transaction Count':'Transaction Count'}, height=400)
    fig.update_layout(xaxis={'title': 'Districts'}, yaxis={'title': 'Transaction Count'}, coloraxis_colorbar={'title': 'Transaction Count'})
    st.plotly_chart(fig)

#top 10 pincodes with maximum count and amount for a particular state for a particular year and quarter
def top_pincodes_count_for_a_state(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'top_tran_pincode'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT pincode, transaction_count, transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s AND state = %s ORDER BY transaction_amount DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter, state))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Pincode', 'Transaction Amount', 'Transaction Count'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#top 10 pincodes with maximum count and amount for a particular state for a particular year and quarter(bar) --->issue with code
def top_pincodes_count_for_a_state_bar(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'top_tran_pincode'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT pincode, transaction_count, transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s AND state = %s ORDER BY transaction_amount DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter, state))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Pincode', 'Transaction Amount', 'Transaction Count'])
    df['Pincode'] = df['Pincode'].astype(str)

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x='Pincode', y='Transaction Count', hover_data=['Pincode', 'Transaction Count', 'Transaction Amount'], title = f'Top 10 Pincodes with maximum Transactions in {state}', color='Transaction Count', color_continuous_scale='thermal', labels={'Pincode' : 'Pincodes','Transaction Count':'Transaction Count'}, height=400)
    fig.update_layout(xaxis={'type': 'category'}, yaxis={'title': 'Transaction Count'},)
    st.plotly_chart(fig)

#top 10 users according to districts for a particular state in a particular year and quarter
def top_districts_users_for_a_state(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_district'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT district, registered_user FROM {table_name} WHERE year = %s AND quarter = %s AND state = %s ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter, state))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['District', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#top 10 users according to districts for a particular state in a particular year and quarter(bar)
def top_districts_users_for_a_state_bar(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_district'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT district, registered_user FROM {table_name} WHERE year = %s AND quarter = %s AND state = %s ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter, state))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['District', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x='District', y='Registered Users', hover_data=['District', 'Registered Users', 'Registered Users'], title = f'Top 10 Districts with maximum Users in {state}', color='Registered Users', color_continuous_scale='thermal', labels={'District' : 'Districts','Registered Userst':'Registered Users'}, height=400)
    fig.update_layout(xaxis={'type': 'category'}, yaxis={'title': 'Registered Users'},)
    st.plotly_chart(fig)

#top 10 pincodes with max users for a particular state in a particular year and quarter
def top_pincodes_users_for_a_state(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_pincode'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT pincode, registered_user FROM {table_name} WHERE year = %s AND quarter = %s AND state = %s ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter, state))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Pincode', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    st.dataframe(df)

#top 10 pincodes with max users for a particular state in a particular year and quarter(bar)
def top_pincodes_users_for_a_state_bar(year, state, quarter):
    cursor, conn = sql_connection()
    table_name = 'top_user_pincode'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT pincode, registered_user FROM {table_name} WHERE year = %s AND quarter = %s AND state = %s ORDER BY registered_user DESC LIMIT 10"
    cursor.execute(select_query, (year, quarter, state))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Pincode', 'Registered Users'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = px.bar(df, x='Pincode', y='Registered Users', hover_data=['Pincode', 'Registered Users'], title = f'Top 10 Pincodes with maximum Users in {state}', color='Registered Users', color_continuous_scale='thermal', labels={'Pincode' : 'Pincodes','Registered Users':'Registered Users'}, height=400)
    fig.update_layout(xaxis={'type': 'category'}, yaxis={'title': 'Registered Users'},)
    st.plotly_chart(fig)

#total transaction count over the years
def transaction_count(transaction_type):
    cursor, conn = sql_connection()
    table_name = 'aggregate_transaction'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT year, sum(transaction_count) as transaction_count, sum(transaction_amount) as transaction_amount FROM {table_name} WHERE transaction_type = %s GROUP BY year ORDER BY year DESC"
    cursor.execute(select_query, (transaction_type,))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Year', 'Transaction Count', 'Transaction Amount'])
    df['Year'] = df['Year'].astype(str)
    df['Transaction Count'] = df['Transaction Count'].apply(format_count)

    # Close the cursor and connection
    cursor.close()
    conn.close()
    #Create subplots with one row and two columns
    fig = make_subplots(rows=1, cols=2,horizontal_spacing=0.2)

    # Add the line chart to the first subplot
    fig.add_trace(go.Scatter(x=df['Year'], y=df['Transaction Count'], name='Count',marker=dict(color='cyan')), row=1, col=1)

    # Add the bar chart to the second subplot
    fig.add_trace(go.Bar(x=df['Year'], y=df['Transaction Amount'], name='Amount',marker=dict(color='cyan')), row=1, col=2)

    # Update layout and axis labels
    fig.update_layout(title=f'Trend in {transaction_type} over the years (2018-2022)',
                      xaxis1=dict(title='Year'),
                      yaxis1=dict(title='Transaction Count'),
                      xaxis2=dict(title='Year'),
                      yaxis2=dict(title='Transaction Amount'),
                      height=600, width=1000)   
 
    st.plotly_chart(fig)
    st.write('Tabular Insights')
    df['Transaction Amount'] = df['Transaction Amount'] / 10000000
    df['Transaction Amount'] = df['Transaction Amount'].apply(lambda x: f'Rs. {x:,.2f} cr')
    st.dataframe(df, width = 900)

#total registered users over the years
def users_count():
    cursor, conn = sql_connection()
    table_name = 'map_user'

    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT year, sum(registered_users) as registered_users, sum(no_of_app_opens) as no_of_app_opens FROM {table_name} GROUP BY year ORDER BY year DESC"
    cursor.execute(select_query)

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['Year', 'User Count', 'No. of App Opens'])
    df['Year'] = df['Year'].astype(str)
    df['User Count'] = df['User Count'].apply(format_count)
    df['No. of App Opens'] = df['No. of App Opens'].apply(format_count)

    # Close the cursor and connection
    cursor.close()
    conn.close()
    fig = make_subplots(rows=1, cols=2,horizontal_spacing=0.2)

    # Add the line chart to the first subplot
    fig.add_trace(go.Scatter(x=df['Year'], y=df['User Count'], name='Count',marker=dict(color='cyan')), row=1, col=1)

    # Add the bar chart to the second subplot
    fig.add_trace(go.Bar(x=df['Year'], y=df['No. of App Opens'], name='App Opens',marker=dict(color='cyan')), row=1, col=2)

    # Update layout and axis labels
    fig.update_layout(title=f'Trend in users over the years (2018-2022)',
                      xaxis1=dict(title='Year'),
                      yaxis1=dict(title='User Count'),
                      xaxis2=dict(title='Year'),
                      yaxis2=dict(title='App Opens'),
                      height=600, width=1000)   
 
    st.plotly_chart(fig)
    st.write('Tabular Insights')
    st.dataframe(df, width = 900)

#district wise transaction count and amount
def district_tran(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'map_transaction'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT state, district,transaction_count, transaction_amount FROM {table_name} WHERE year = %s AND quarter = %s"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['State', 'District', 'Transaction Count', 'Transaction Amount'])
    # Close the cursor and connection
    cursor.close()
    conn.close()
    return df

#district wise user count
def district_user(year,quarter):
    cursor, conn = sql_connection()
    table_name = 'map_user'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT state, district,registered_users, no_of_app_opens FROM {table_name} WHERE year = %s AND quarter = %s"
    cursor.execute(select_query, (year, quarter))

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Display the fetched data as a Pandas DataFrame
    df = pd.DataFrame(rows, columns=['State', 'District', 'Registered Users', 'No. of App Opens'])
    # Close the cursor and connection
    cursor.close()
    conn.close()
    return df

#most used brands for a particular year and quarter
def brand_name(year,quarter):
    cursor, conn = sql_connection()

    # Define the table 
    table_name = 'aggregate_user'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT brand_name, sum(user_count) as user_count FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY brand_name"
    cursor.execute(select_query, (year, quarter))
        

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Check if there is data available
    if rows:
        # Display the fetched data as a Pandas DataFrame
        df = pd.DataFrame(rows, columns=['Brand', 'User Count'])
        st.dataframe(df)
    else:
        st.write(f':red[No data available.]')
        
    # Close the cursor and connection
    cursor.close()
    conn.close()

#most used brands for a particular year and quarter(bar)
def brand_name_bar(year,quarter):
    cursor, conn = sql_connection()

    # Define the table 
    table_name = 'aggregate_user'
    # Execute the SELECT query with placeholders for values
    select_query = f"SELECT brand_name, sum(user_count) as user_count FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY brand_name ORDER BY user_count DESC"
    cursor.execute(select_query, (year, quarter))
        

    # Fetch all rows from the result set
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['Brand', 'User Count'])
    # Check if there is data available
    if rows:
        # Display the fetched data as a Pandas DataFrame
        df = pd.DataFrame(rows, columns=['Brand', 'User Count'])
        fig = px.bar(df, x='Brand', y='User Count', hover_data=['Brand', 'User Count'], title = f'Most used brands in quarter {quarter} of year {year}', color='User Count', color_continuous_scale='thermal', labels={'Brand' : 'Brand','User Count':'User Count'}, height=400)
        fig.update_layout(xaxis={'type': 'category'}, yaxis={'title': 'User Count'},)
        st.plotly_chart(fig)
    else:
        st.write(f':red[No data available.]')
        
    # Close the cursor and connection
    cursor.close()
    conn.close()
    

#####################################
json_file = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

def query1(year,quarter):
    column1, column2 = st.columns([2,1])
    with column1:
        st.subheader(':purple[Total Transaction Count and amount]')
        total_tran_count_and_amount(year,quarter)
        # Fetch the data from the database
        df = total_tran(year,quarter)
        file = '/Users/jk/Desktop/VSCode/Data_Map_Districts_Longitude_Latitude2.csv'
        data = pd.read_csv(file)
        
        data['State'] = data['State'].str.replace('-', ' ')
        data['State'] = data['State'].str.title()
        data['State'] = data['State'].str.replace('and ', '& ')
        data['State'] = data['State'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        data['District'] = data['District'].str.title()
        transaction_data = district_tran(year,quarter)
        # Merge the transaction data with the district data based on the 'state' and 'district' columns
        merged_data = pd.merge(data, transaction_data, how='left', left_on=['State', 'District'], right_on=['State', 'District'])

        # Create a choropleth map
        fig = px.choropleth(
            df,
            geojson= json_file,
            featureidkey='properties.ST_NM',
            locations='State',
            projection= 'orthographic',
            color='Transaction Amount',
            hover_data={'Transaction Count': ':,', 'Transaction Amount': ':,'},
            color_continuous_scale="dense",
            # range_color = (0, 20) mercator
            
        )
        # Prepare hover text with district names and transaction information
        hover_text = merged_data['State'] + '<br>' + \
                     merged_data['District'] + '<br>' + \
                     '<br>' + \
                     merged_data['Transaction Count'].fillna(0).astype(int).apply(str) + ' Transactions,' + '<br>' + \
                     'Rs. ' + merged_data['Transaction Amount'].fillna(0).round(2).apply(str)

        # Add scattergeo traces for the districts
        fig.add_trace(
            go.Scattergeo(
                lat=merged_data['Latitude'],  # Replace 'Latitude' with the column name in your file
                lon=merged_data['Longitude'],  # Replace 'Longitude' with the column name in your file
                mode='markers',
                marker=dict(
                    symbol = 'cross',
                    size=4,
                    color='purple'
                ),
                hovertext=hover_text,
                name=' '
            )
        )

        # Update the map layout
        fig.update_geos(fitbounds='locations', visible=False)
        fig.update_layout(margin={'r': 0, 't': 0, 'l': 0, 'b': 0})
        fig.update_layout(width=800, height=600)
        fig.update_traces(textsrc='top center', text=df['State'])

        # Display the map
        st.plotly_chart(fig, use_container_width=True)

    with column2:
        st.subheader('Category wise count')
        total_tran_count(year,quarter)
        total_tran_amount_pie(year,quarter)

    st.write(' ')
    st.subheader(f':green[Top 10 Performers]')
    option = st.radio(f':violet[Transactions by]',('State', 'District','Pincode'),horizontal=True)
    if option == 'State':
        col1, col2 = st.columns([1,2])
        with col1:
            top_states_with_max_count(year,quarter)
        with col2:
            top_states_with_max_count_bar(year,quarter)
    if option == 'District':
        col1, col2 = st.columns([1,2])
        with col1:
            top_districts_with_max_count(year,quarter)
        with col2:
            top_districts_with_max_count_bar(year,quarter)       
    if option == 'Pincode':
        col1, col2 = st.columns([1,2])
        with col1:
            top_pincodes_with_max_count(year,quarter)
        with col2:
            top_pincodes_with_max_count_bar(year,quarter)


##########
def query2(year,quarter):
    column1, column2 = st.columns([2.5,1])
    with column1:
        st.write(' ')
        st.write(' ')
        st.subheader(':purple[Region wise Data ]')

        # Fetch the data from the database
        df = total_user(year,quarter)
        file = '/Users/jk/Desktop/VSCode/Data_Map_Districts_Longitude_Latitude2.csv'
        data = pd.read_csv(file)
        data['State'] = data['State'].str.replace('-', ' ')
        data['State'] = data['State'].str.title()
        data['State'] = data['State'].str.replace('and ', '& ')
        data['State'] = data['State'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
        data['District'] = data['District'].str.title()
        transaction_data = district_user(year,quarter)
        # Merge the transaction data with the district data based on the 'state' and 'district' columns
        merged_data = pd.merge(data, transaction_data, how='left', left_on=['State', 'District'], right_on=['State', 'District'])

        # Create a choropleth map
        fig = px.choropleth(
            df,
            geojson= json_file,
            featureidkey='properties.ST_NM',
            locations='State',
            projection= 'orthographic',
            color='State',
            hover_data={'Registered Users': ':,'},
            # color_continuous_scale="oranges",
            # range_color = (0, 20) mercator
            
        )
        # Prepare hover text with district names and transaction information
        hover_text = merged_data['State'] + '<br>' + \
                     merged_data['District'] + '<br>' + \
                     '<br>' + \
                     merged_data['Registered Users'].fillna(0).astype(int).apply(str) + ' Registered Users,' + '<br>' + \
                     merged_data['No. of App Opens'].fillna(0).round(2).apply(str) + ' App opens'
        # Add scattergeo traces for the districts
        fig.add_trace(
            go.Scattergeo(
                lat=data['Latitude'],  # Replace 'Latitude' with the column name in your file
                lon=data['Longitude'],  # Replace 'Longitude' with the column name in your file
                mode='markers',
                marker=dict(
                    symbol = 'cross',
                    size=4,
                    color='purple'
                ),
                hovertext=hover_text,
                name='Districts'
            )
        )

        # Update the map layout
        fig.update_geos(fitbounds='locations', visible=False)
        fig.update_layout(margin={'r': 0, 't': 0, 'l': 0, 'b': 0})
        fig.update_layout(width=800, height=600)
        fig.update_traces(textsrc='top center', text=df['State'])

        # Display the map
        st.plotly_chart(fig, use_container_width=True)

    with column2:
        st.write(' ')
        st.subheader(f':violet[No. of Registered Users and App Opens]')
        st.write(' ')
        total_user_app_opens(year,quarter)
        st.write(' ')
        total_user_app_opens_pie(year,quarter)
        
    st.write(' ')
    st.subheader(f':green[Top 10 Performers]')
    option = st.radio(f':violet[Users by]',('State', 'District','Pincode'),horizontal=True)
    if option == 'State':
        col1, col2 = st.columns([1,2])
        with col1:
            top_states_with_max_users(year,quarter)
        with col2:
            top_states_with_max_users_bar(year,quarter)
    if option == 'District':
        col1, col2 = st.columns([1,2])
        with col1:
            top_districts_with_max_users(year,quarter)
        with col2:
            top_districts_with_max_users_bar(year,quarter)
    if option == 'Pincode':
        col1, col2 = st.columns([1,2])
        with col1:
            top_pincodes_with_max_users(year,quarter)
        with col2:
            top_pincodes_with_max_users_bar(year,quarter)

    st.write(' ')
    st.subheader('Data on Mobile Brands used')
    col1, col2 = st.columns([1,2])
    with col1:
        brand_name(year,quarter)
    with col2:
        brand_name_bar(year,quarter)




##############################################        STREAMLIT        ##################################################
st.set_page_config(layout="wide")

def home_page():
    # Path to your image file
    image_path = "/Users/jk/Desktop/VSCode/phonepe.png"

    # Display the image
    col1,col2 = st.columns([1,7])
    with col1:
        st.image(image_path, caption="PhonePe Pulse", width=100)
    with col2:
        st.title(':violet[Phonepe Pulse: The Beat of Progress ]')
        st.markdown('[Source: PhonePe Pulse](https://www.phonepe.com/pulse/explore/transaction/2022/4/)', unsafe_allow_html=True)
    st.sidebar.write('--------------------------------------------------------------')
    st.sidebar.write(':blue[Created By]')
    st.sidebar.write('Jeel Kenia')
    st.sidebar.markdown('[GitHub](https://github.com/Jeel-Kenia)', unsafe_allow_html=True)
    st.sidebar.markdown('[LinkedIn](https://www.linkedin.com/in/jeel-kenia/)', unsafe_allow_html=True)

    phonepe_description = """Introducing PhonePe Pulse, a fascinating gateway into the world of digital payments in India!
                             With over 30 crore registered users and a staggering 2000 crore transactions, PhonePe is not 
                             just a digital payments platform, but a true beholder of the Indian digital payments revolution.
                             Unlock the power of PhonePe Pulse, an extraordinary data analytics platform that offers you a
                             front-row seat to witness the dynamic landscape of digital transactions in India. Dive deep 
                             into the depths of data and explore the intriguing insights and captivating trends that shape 
                             the way Indians embrace digital payments.With a remarkable 46% UPI market share, 
                             PhonePe stands tall as India's largest digital payments platform. Now, you have the 
                             incredible opportunity to tap into the wealth of information it holds and bring it to 
                             life through stunning visualizations. Experience the pulse of India's digital transactions 
                             like never before!PhonePe Pulse is your gateway to a universe of insights, where data meets 
                             innovation and sparks fly. Join us on this extraordinary adventure and unleash the true potential of digital payments in India!
                        """

    st.write(phonepe_description)
    st.image('/Users/jk/Desktop/VSCode/Pulse.gif', use_column_width = True)

def analysis_page():
    st.title(':blue[Countrywise analysis of PhonePe users ]')
    
    col1, col2, col3 = st.columns(3)
    with col1:
        option = st.sidebar.radio(f':blue[Select Option]', ['Transaction', "User"])
    with col2:
        year = st.sidebar.radio(f':blue[Select a year]', ['2022','2021','2020','2019','2018'])
    with col3:
        quarter = st.sidebar.radio(f':blue[Select a quarter]',['4','3','2','1'])
    if option == 'Transaction':
        query1(year,quarter)
        st.sidebar.write('-----------------------------------------------------------------')
        transaction_type = st.sidebar.radio(f':blue[Select a type for Trend analysis]', ['Merchant payments','Peer-to-peer payments','Recharge & bill payments','Financial Services','Others'])
        transaction_count(transaction_type)
    elif option == 'User':
        query2(year,quarter)
        users_count()

def analysis_page2():
    st.title(':blue[Statewise Analysis of Phonepe users ]')
    states = ['Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chhattisgarh', 'Goa', 'Gujarat', 'Haryana',
              'Himachal Pradesh', 'Jammu & Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Madhya Pradesh', 
              'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Punjab', 'Rajasthan', 
              'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura','Uttar Pradesh', 'West Bengal','Andaman & Nicobar',
              'Chandigarh','Dadra & Nagar Haveli & Daman & Diu','Delhi','Ladakh', 'Lakshadweep','Puducherry','Uttarakhand']
    col1, col2, col3 = st.columns(3)
    with col1:
        year = st.sidebar.radio(f':blue[Select a year]', ['2022','2021','2020','2019','2018'])
    with col2:
        quarter = st.sidebar.radio(f':blue[Select a quarter]',['4','3','2','1']) 
    with col3:
        state = st.sidebar.selectbox(f':blue[Select a State]', states)

    col1,col2 = st.columns(2)
    with col1:
        st.write(f":violet[Total Transaction count and Amount for {state} :]")
        total_tran_for_a_state(year, state, quarter)
    with col2:
        st.write(f":violet[Total Regsitered Users and No. of App Opens in {state} :]")
        reg_users_for_a_state(year, state, quarter)
    st.write(' ')
    st.write(' ')
    col1 ,col2 = st.columns([1,2])
    with col1:
        st.write(' ')
        st.write(' ')
        st.write(' ')
        st.write(' ')
        st.write(f":violet[Total Transaction count and Amount for each type in {state} :]")
        tran_count_for_a_state(year, state, quarter)
    with col2:
        tran_amount_for_a_state_pie(year, state, quarter)

    st.subheader(f':green[Top 10 Performers]')
    option = st.radio(f':violet[Transactions by:] ',('District','Pincode'),horizontal=True)
    if option == 'District':
        col1 ,col2 = st.columns([1.5,2])
        with col1:
            st.write(' ')
            st.write(' ')
            st.write(' ')
            st.write(' ')
            st.write(' ')
            top_districts_count_for_a_state(year, state, quarter)
        with col2:
            top_districts_count_for_a_state_bar(year, state, quarter)
            pass
    elif option =='Pincode':
        col1 ,col2 = st.columns([1.5,2])
        with col1:
            st.write(' ')
            st.write(' ')
            st.write(' ')
            st.write(' ')
            st.write(' ')
            top_pincodes_count_for_a_state(year, state, quarter)
        with col2:
            top_pincodes_count_for_a_state_bar(year, state, quarter)
    st.write(' ')
    option2 = st.radio(f':violet[Registered users by:] ',('Districts','Pincodes'),horizontal=True)
    if option2 == 'Districts':
        col1 ,col2 = st.columns([1.5,2])
        with col1:
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            top_districts_users_for_a_state(year, state, quarter)        
        with col2:
            top_districts_users_for_a_state_bar(year, state, quarter)
    elif option2 =='Pincodes':
        col1 ,col2 = st.columns([1.5,2])
        with col1:
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            st.write('')
            top_pincodes_users_for_a_state(year, state, quarter)
        with col2:
            top_pincodes_users_for_a_state_bar(year, state, quarter)
            


# Create a dictionary to map page names to their corresponding functions
pages = {
    'Home': home_page,
    'Country Level Insights': analysis_page,
    'State Level Insights': analysis_page2
}

# Add a sidebar to select the page
st.sidebar.write(' ')
st.sidebar.write(' ')
st.sidebar.write(' ')
# selected_page = st.sidebar.selectbox(f':blue[Select a page from the dropdown to move on to analysis]', list(pages.keys()))
selected_page = st.sidebar.radio(f':blue[Select a button from the list to move on to analysis]', list(pages.keys()))
# Run the function corresponding to the selected page
pages[selected_page]()