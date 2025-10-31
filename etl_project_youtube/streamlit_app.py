# Import python packages
import streamlit as st
from snowflake.snowpark import Session

# -------------------------
# Snowflake session setup
# -------------------------
connection_parameters = {
    'user': 'dbt',
    'password': 'dbtPassword123',
    'account': 'sskjybh-yw79994',   # e.g., xy12345.us-east-1
    'warehouse': 'COMPUTE_WH',
    'database': 'GA_ANALYTICS',
    'schema': 'GA_EXPORT'
}

# Create Snowpark session
session = Session.builder.configs(connection_parameters).create()

# -------------------------
# Streamlit app
# -------------------------
st.title(f"Example Streamlit App 🎈 {st.__version__}")
st.write(
    """Replace this example with your own code!
    **And if you're new to Streamlit,** check
    out our easy-to-follow guides at
    [docs.streamlit.io](https://docs.streamlit.io).
    """
)

# -------------------------
# Interactive slider
# -------------------------
hifives_val = st.slider(
    "Number of high-fives in Q3",
    min_value=0,
    max_value=90,
    value=60,
    help="Use this to enter the number of high-fives you gave in Q3",
)

# -------------------------
# Create example Snowpark DataFrame
# -------------------------
created_dataframe = session.create_dataframe(
    [[50, 25, "Q1"], [20, 35, "Q2"], [hifives_val, 30, "Q3"]],
    schema=["HIGH_FIVES", "FIST_BUMPS", "QUARTER"],
)

# Convert to pandas for visualization
queried_data = created_dataframe.to_pandas()

# -------------------------
# Display bar chart
# -------------------------
st.subheader("Number of high-fives")
st.bar_chart(data=queried_data, x="QUARTER", y="HIGH_FIVES")

# -------------------------
# Display underlying data
# -------------------------
st.subheader("Underlying data")
st.dataframe(queried_data, width='stretch')
