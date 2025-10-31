import streamlit as st
import pandas as pd
import plotly.express as px

# -------------------------
# Load data
# -------------------------
df = pd.read_csv("etl_project_youtube/all_crimes.csv")

# -------------------------
# Sidebar filters
# -------------------------
st.sidebar.header("Filters")

# Category filter
categories = df['category'].dropna().unique()
selected_categories = st.sidebar.multiselect(
    "Crime Categories", categories, default=categories
)

# Outcome status filter
outcomes = df['outcome_status_category'].dropna().unique()
selected_outcomes = st.sidebar.multiselect(
    "Outcome Status", outcomes, default=list(outcomes)
)

# Apply filters
filtered_df = df[
    df['category'].isin(selected_categories) &
    df['outcome_status_category'].isin(selected_outcomes)
]

# -------------------------
# Title
# -------------------------
st.title("Police Crime Dashboard in Midlands UK (July 2025)")
st.markdown("Interactive storytelling dashboard for crime analysis in the Midlands UK")

# -------------------------
# KPI Cards
# -------------------------
st.subheader("Key Metrics")
col1, col2, col3 = st.columns(3)
col1.metric("Total Crimes", len(filtered_df))
col2.metric("Crime Categories", filtered_df['category'].nunique())
under_investigation_count = filtered_df[filtered_df['outcome_status_category'] == 'Investigation complete; no suspect identified'].shape[0]
col3.metric("Investigation Complete – No Suspect", under_investigation_count)


# -------------------------
# Bar Chart: Crimes by Category
# -------------------------
st.subheader("Which Crimes Dominate the Month of July 2025?")
category_counts = filtered_df['category'].value_counts().reset_index()
category_counts.columns = ['category', 'count']
bar_fig = px.bar(
    category_counts,
    x='category',
    y='count',
    color='category',
    color_discrete_sequence=px.colors.qualitative.Set2,
    labels={'category': 'Category', 'count': 'Number of Crimes'},
)
bar_fig.update_layout(showlegend=False)
st.plotly_chart(bar_fig, use_container_width=True)

# -------------------------
# Stacked Bar: Crimes by Outcome per Category
# -------------------------
st.subheader("Crime Outcomes by Category")
outcome_counts = filtered_df.groupby(['category', 'outcome_status_category']).size().reset_index(name='count')
stacked_fig = px.bar(
    outcome_counts,
    x='category',
    y='count',
    color='outcome_status_category',
    labels={'category': 'Category', 'count': 'Number of Crimes', 'outcome_status_category': 'Outcome'},
    color_discrete_sequence=px.colors.qualitative.Pastel,
)
st.plotly_chart(stacked_fig, use_container_width=True)

# -------------------------
# Horizontal Bar: Top 10 Crime Types
# -------------------------
st.subheader("Top 10 Crime Types")
top10 = category_counts.sort_values('count', ascending=False).head(10)
hbar_fig = px.bar(
    top10,
    x='count',
    y='category',
    orientation='h',
    labels={'category': 'Category', 'count': 'Number of Crimes'},
    color='count',
    color_continuous_scale=px.colors.sequential.Viridis
)
hbar_fig.update_layout(showlegend=False)
st.plotly_chart(hbar_fig, use_container_width=True)

# -------------------------
# Data Table
# -------------------------
st.subheader("Filtered Data Table")
st.dataframe(filtered_df, width='stretch', height=500)
