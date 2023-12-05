import streamlit as st
import pandas as pd
import numpy as np
import Utils as ut
import plotly.express as plx

cases_deaths_df = pd.read_csv('./files_wrangled/cases_deaths.csv')

selected_metric_cases = st.selectbox("Metric", ["New_cases", "Cumulative_cases", "Weekly_cases", 
                                          "Weekly_pct_growth_cases", "Cumulative_cases_per_million", 
                                          "Weekly_cases_per_million", "Weekly_pct_change_per_million"])


fig1 = plx.choropleth(
    data_frame=cases_deaths_df,
    locations="Country_code",
    locationmode="ISO-3",
    color=selected_metric_cases,
    animation_frame="Date_reported",
    title="Covid-19 Evolution Map",
    color_continuous_scale="reds",
    range_color=(0, cases_deaths_df[selected_metric_cases].max()/3),
)

fig1.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 1


st.plotly_chart(fig1)



selected_metric_deaths = st.selectbox("Metric", ["New_deaths", "Cumulative_deaths", "Weekly_deaths", 
                                          "Weekly_pct_growth_deaths", "Cumulative_deaths_per_million", 
                                          "Weekly_deaths_per_million", "Weekly_pct_change_deaths_per_million"])


fig2 = plx.choropleth(
    data_frame=cases_deaths_df,
    locations="Country_code",
    locationmode="ISO-3",
    color=selected_metric_cases,
    animation_frame="Date_reported",
    title="Covid-19 Evolution Map",
    color_continuous_scale="reds",
    range_color=(0, cases_deaths_df[selected_metric_cases].max()/3),
)

fig2.layout.updatemenus[0].buttons[0].args[1]['frame']['duration'] = 1

st.plotly_chart(fig2)