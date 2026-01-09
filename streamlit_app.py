# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

# Write directly to the app
st.title("Customise Your Smoothie:cup_with_straw:")
st.write(
  """  Choose the fruits you want in your custom smoothie!
  """)

name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on the smoothie will be:", name_on_order)
cnx = st.connection("snowflake")
session = cnx.session()

#my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'))
my_dataframe = session.table("smoothies.public.fruit_options") \
    .select(col('FRUIT_NAME'), col('SEARCH_ON'))
#st.dataframe(data=my_dataframe, use_container_width=True)
#st.stop()
pd_df=my_dataframe.to_pandas()
#st.dataframe(pd_df)
#st.stop()

ingredients_List = st.multiselect(
    'Choose up to 5 ingrediants:'
    #, my_dataframe    
    ,pd_df['FRUIT_NAME'].tolist(),
    , max_selections=5
    )


if ingredients_List:
    ingredients_string = ''

    for fruit_chosen in ingredients_List:
        ingredients_string += fruit_chosen + ' '
     
        SEARCH_ON=pd_df.loc[pd_df['FRUIT_NAME'] == fruit_chosen, 'SEARCH_ON'].iloc[0]
        st.write('The search value for ', fruit_chosen,' is ', SEARCH_ON, '.')

        st.subheader(f"{fruit_chosen} Nutrition Information")
        smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/{SEARCH_ON}")
        sf_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

        my_insert_stmt = """ insert into smoothies.public.orders(ingredients,name_on_order)
            values ('""" + ingredients_string + """','""" + name_on_order + """')"""

    # st.write(my_insert_stmt)
    # st.stop()

    
    time_to_insert = st.button('Submit Order')

    if time_to_insert:
        session.sql(my_insert_stmt).collect()
        st.success('Your Smoothie is ordered!',icon="✅")
