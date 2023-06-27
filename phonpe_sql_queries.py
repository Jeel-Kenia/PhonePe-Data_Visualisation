import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_to_sql import *

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
    fig = px.pie(df, values='Transaction Amount', names='Transaction Type', title = 'Transaction Amount for each type payment', color_discrete_sequence=px.colors.qualitative.Prism)
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
    fig = px.bar(df, x="Transaction Amount", y="State", orientation='h', color_continuous_scale= 'twilight', color= 'Transaction Amount', hover_data=["State", "Transaction Amount"], height=400, title= f'Top States with maximum Transaction Amount in Quarter  {quarter} of  {year}')
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
    fig = px.bar(df, x="Transaction Amount", y="District", orientation='h', color_continuous_scale= 'twilight',color= 'Transaction Amount', hover_data=["District", "Transaction Amount"], height=400, title= f'Top Districts with maximum Transaction Amount in Quarter  {quarter} of  {year}')
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
    fig = px.bar(df, x="Transaction Amount", y="Pincode", orientation='h', color_continuous_scale= 'twilight',color= 'Transaction Amount', hover_data=["Pincode", "Transaction Amount"], height=400, title= f'Top Pincodes with maximum Transaction Amount in Quarter  {quarter} of  {year}')
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
    colors = ['green', 'brown']
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
    fig = px.bar(df, x='Registered Users', y='State', orientation='h', color_continuous_scale= 'twilight',color= 'Registered Users', hover_data=['State', 'Registered Users'], height=400, title= f'Top States with maximum Users in Quarter  {quarter} of  {year}')
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
    fig = px.bar(df, x='Registered Users', y='District', orientation='h', color_continuous_scale= 'twilight',color= 'Registered Users', hover_data=['District', 'Registered Users'], height=400, title= f'Top Districts with maximum Users in Quarter  {quarter} of  {year}')
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
    fig = px.bar(df, x='Registered Users', y='Pincode', orientation='h', color_continuous_scale= 'twilight',color= 'Registered Users', hover_data=['Pincode', 'Registered Users'], height=400, title= f'Top Pincodes with maximum Users in Quarter  {quarter} of  {year}')
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
    fig = px.pie(df, values='Transaction Amount', names='Transaction Type', color_discrete_sequence=px.colors.qualitative.Prism, labels={'Transaction Type':'Transaction Type'})
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
    fig.add_trace(go.Scatter(x=df['Year'], y=df['Transaction Count'], name='Count',marker=dict(color='red')), row=1, col=1)

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
    fig.add_trace(go.Scatter(x=df['Year'], y=df['User Count'], name='Count',marker=dict(color='red')), row=1, col=1)

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
    select_query = f"SELECT brand_name, sum(user_count) as user_count FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY brand_name LIMIT 10"
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
    select_query = f"SELECT brand_name, sum(user_count) as user_count FROM {table_name} WHERE year = %s AND quarter = %s GROUP BY brand_name ORDER BY user_count DESC LIMIT 10"
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