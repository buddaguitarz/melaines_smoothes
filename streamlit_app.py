
# Import python packages
import streamlit as st
from snowflake.snowpark.functions import col
import requests
import pandas as pd

st.title("Customise Your Smoothie :cup_with_straw:")
st.write("""Choose the fruits you want in your custom smoothie!""")

# Name input
name_on_order = st.text_input("Name on Smoothie:")
st.write("The name on the smoothie will be:", name_on_order)

# Snowflake connection & fetch fruit list
cnx = st.connection("snowflake")
session = cnx.session()

# Get fruit names from Snowflake
# This returns Snowpark Row objects -> convert to a simple Python list of strings
rows = (
    session.table("smoothies.public.fruit_options")
    .select(col("FRUIT_NAME"))
    .collect()
)
fruit_options = [r["FRUIT_NAME"] for r in rows]  # e.g., ["Banana", "Strawberry", ...]

# Multiselect for ingredients
ingredients_list = st.multiselect(
    "Choose up to 5 ingredients:",
    fruit_options,
    max_selections=5
)

# Only proceed if the user selected something
if ingredients_list:
    # Build a string safely once, rather than concatenating inside the loop
    ingredients_string = " ".join(ingredients_list)

    # Optional: fetch external fruit info (moved OUT of the for-loop)
    try:
        smoothiefroot_response = requests.get(
            "https://my.smoothiefroot.com/api/fruit/watermelon",
            timeout=10
        )
        smoothiefroot_response.raise_for_status()
        data = smoothiefroot_response.json()

        # Display the API data in Streamlit (normalize if nested)
        if isinstance(data, list):
            df = pd.DataFrame(data)
            st.dataframe(df, use_container_width=True)
        elif isinstance(data, dict):
            # Show raw JSON; if you want a flat table, use json_normalize
            st.json(data)
            # df = pd.json_normalize(data)
            # st.dataframe(df, use_container_width=True)
        else:
            st.write("Unexpected API response type:", type(data))
    except requests.RequestException as e:
        st.error(f"Failed to fetch SmoothieFroot data: {e}")

    # Prepare an insert using Snowpark parameters (safer than string concatenation)
    time_to_insert = st.button("Submit Order")

    if time_to_insert:
        if not name_on_order.strip():
            st.warning("Please enter a name for your smoothie before submitting.")
        else:
            # Use Snowpark DataFrame API for parameterized insert
            orders_df = session.create_dataframe(
                [[ingredients_string, name_on_order]],
                schema=["INGREDIENTS", "NAME_ON_ORDER"]
            )
            orders_df.write.save_as_table(
                "smoothies.public.orders",
                mode="append"
            )

            st.success("Your Smoothie is ordered!", icon="✅")
