import streamlit as st
import pandas as pd
import plotly.express as px
from mysql.connector import Error
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from phonpe_sql_queries import *

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