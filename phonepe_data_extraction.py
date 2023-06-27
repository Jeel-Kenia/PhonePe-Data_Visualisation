import pandas as pd
import os
import json

repo_url = 'https://github.com/PhonePe/pulse.git'
destination_path = '/Users/Desktop'

# Change the current working directory to the desired destination path
os.chdir(destination_path)

# Execute the git clone command
os.system(f'git clone {repo_url}')

#******************************************    Creating dataframes from cloned data    *********************************************

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

#********************************************     Clean The Data    ******************************************
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
