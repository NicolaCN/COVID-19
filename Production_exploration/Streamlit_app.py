import numpy as np
import pandas as pd
import streamlit as st
import Utils as ut
import plotly.express as plx
import requests
import wbgapi as wb
from matplotlib import pyplot as plt
import matplotlib.colors as mcolors
import math
import plotly.graph_objects as go
from plotly.subplots import make_subplots



st.write("""# COVID-19 Visual Analytics""")

def assign_color(val, range, percentage=False):
    for i, (lower, upper) in enumerate(zip(range, range[1:])):
        if math.isnan(val) or val == None:
            return f'missing data'
        
        if lower <= val < upper:
            if percentage:
                return f'{lower}%-{upper}%'
            return f'{lower}-{upper}'
        
    if percentage:
        return f'{range[-1]}%+'
    return f'{range[-1]}+'

def create_animated_map(df, measure, categories, ranges, colors, assign_color, date_col):

        # Perc parameter set based on the measure
        perc = False
        if "pct" in measure:
            perc = True

        # Create the colormap
        color_map = dict(zip(categories, colors))

        # Assign the category to each value in the measure column
        df["category"] = df[measure].apply(assign_color, **{'range': ranges, 'percentage': perc})
        df[date_col] = df[date_col].astype(str)

        # Add the distinct categories to each time frame
        # (verbose operation needed only for visualization purposes)
        catg = df['category'].unique()
        dts = df[date_col].unique()
        data = [{date_col: tf, 'category': i} for tf in dts for i in catg]

        # Concatenate the list of dictionaries into a DataFrame
        df = pd.concat([df, pd.DataFrame(data)], ignore_index=True)

        # Plot the animated map
        fig = plx.choropleth(
            data_frame=df,
            locations="Country_code",
            locationmode="ISO-3",
            color="category",
            animation_frame=date_col,
            title="Covid-19 Evolution Map",
            category_orders={measure: categories},
            color_discrete_map=color_map,
        )
        
        return fig
    

# Define the categories and the range of values for the categories
ranges_dict = {
    'New_cases_per_million': (["missing data", "0-10", "10-50", "50-100", "100-200", "200-500", "500-1000", "1000-2000", "2000-5000", "5000+"],
                              [0, 10, 50, 100, 200, 500, 1000, 2000, 5000],
                              ['#D3D3D3', '#ffcccc', '#ff9999', '#ff6666', '#ff3333', '#cc0000', '#990000', '#660000', '#440000', '#220000']),
    
    'Cumulative_cases_per_million': (["missing data", "0-100", "100-300", "300-1000", "1000-3000", "3000-10000", "10000-30000", "30000-100000", "100000-300000", "300000-1000000"],
                                     [0, 100, 300, 1000, 3000, 10000, 30000, 100000, 300000, 1000000],
                                     ['#D3D3D3', '#ffffff','#ffeeee', '#ffcccc', '#ff9999', '#ff6666', '#ff3333', '#cc0000', '#990000', '#660000', '#330000'])
,
    'Weekly_pct_growth_cases': (["missing data", "-200%--100%", "-100%--50%", "-50%--20%", "-20%-0%", "0%-20%", "20%-50%", "50%-100%", "100%-200%", "200%+"],
                                      [-200, -100, -50, -20, 0, 20, 50, 50, 100, 200],
                                      ['#D3D3D3', '#1a5276', '#336e9d', '#6699cc', '#c0d9e7', '#ffcccc', '#ff6666', '#ff3333', '#990000', '#660000'])
,
    'Cumulative_deaths_per_million': (["missing data", "0-10", "10-1000", "0-1000", "1000-2000", "2000-3000", "3000-4000", "4000-5000", "5000+"],
                                      [0, 10, 1000, 2000, 3000, 4000, 5000],
                                      ['#D3D3D3', '#ffffff','#ffcccc', '#ff6666', '#ff0000', '#cc0000', '#990000', '#660000'])
,
    
    'New_deaths_per_million': (["missing data", "0-0.1", "0.1-0.2", "0.2-0.5", "0.5-1", "1-2", "2-5", "5-10", "10-20", "20-50", "50+"],
                               [0, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50],
                               ['#D3D3D3', '#ffffff','#ffeeee', '#ffcccc', '#ff9999', '#ff6666', '#ff3333', '#cc0000', '#990000', '#660000', '#330000'])
,
    'Weekly_pct_growth_deaths': (["missing data", "-50%--25%", "-25%--10%", "-10%-0%", "0%-10%", "10%-25%", "25%-50%", "50%-100%", "100%+"],
                                 [-50, -25, -10, 0, 10, 25, 50, 100],
                                 ['#D3D3D3', '#1a5276', '#336e9d', '#6699cc', '#c0d9e7', '#ffcccc', '#ff6666', '#ff3333', '#990000', '#660000'])
,
    'new_vaccine_doses_administered_per_hundred': (["missing data", "0-0.1", "0.1-0.2", "0.2-0.3", "0.3-0.4", "0.4-0.5", "0.5-0.6", "0.6-0.7", "0.7-0.8", "0.8-0.9", "0.9-1", "1+"],
                                                   [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1],
                                                   ['#D3D3D3', '#eaffea', '#c7f2c7', '#a4d9a4', '#81c181', '#5ea75e', '#3b8c3b', '#187218', '#006600', '#005500', '#004400', '#003300'])
,
    'cumulative_vaccine_doses_administered_per_hundred': (["missing data", "0-50", "50-100", "100-150", "150-200", "200-250", "250-300", "300+"],
                                                          [0, 50, 100, 150, 200, 250, 300],
                                                          ['#D3D3D3', '#f0fff0', '#cae8ca', '#a3d0a3', '#7cb87c', '#559f55', '#2e872e', '#007000'])

    
}

st.write("""## 1. How did COVID-19 spread globally, and which countries were most severely affected in terms of the number of cases and deaths?""")


st.write("""### Covid-19 Confirmed Cases""")  
st.write("""Choose the metric you prefer to visualize the evolution of the pandemic""")  
    
measure_cases = st.selectbox("Metric", ["New_cases_per_million", 
                                        "Cumulative_cases_per_million", 
                                        "Weekly_pct_growth_cases"])

# Read the data
cases_deaths_df = pd.read_csv('C:/Users/super/Desktop/DATA_ENGINEERING/Project_git/FODE-23-22/dags/files_wrangled/cases_deaths.csv')

# Create a copy of the dataframe
df = cases_deaths_df.copy()

# Select the range of values for the measure
categories, ranges, colors = ranges_dict[measure_cases]

# Create an animated map
fig = create_animated_map(df, measure_cases, categories, ranges, colors, assign_color, date_col='Date_reported')

st.plotly_chart(fig)


'''DEATHS ANIMATED MAP'''


st.write("""### Covid-19 Confirmed Deaths""")  
st.write("""Choose the metric you prefer to visualize the evolution of the pandemic""")  
  

measure_deaths = st.selectbox("Metric", ["Cumulative_deaths_per_million", 
                                         "New_deaths_per_million",
                                         "Weekly_pct_growth_deaths"])


# Create a copy of the dataframe
df = cases_deaths_df.copy()

# Select the range of values for the measure
categories, ranges, colors = ranges_dict[measure_deaths]

# Create an animated map
fig = create_animated_map(df, measure_deaths, categories, ranges, colors, assign_color, date_col='Date_reported')

st.plotly_chart(fig)


st.write("""### Animated Charts""")
st.write("""Choose the countries you want and explore the evolution of confirmed cases and deaths""")
st.write("""It's very slow to load, please be patient!""")

# List of all countries
all_countries = df['Country_name'].dropna().unique()
all_countries.sort()

# Display the filtered countries in the multiselect widget
selected_countries = st.multiselect('Select countries (max 5):', options=all_countries, max_selections=5)
        
# Select the metric to visualize
measure_chart = st.selectbox("Chart Metric", ["New_cases_per_million", 
                                        "Cumulative_cases_per_million", 
                                        "Weekly_pct_growth_cases",
                                        "Cumulative_deaths_per_million",
                                        "New_deaths_per_million",
                                        "Weekly_pct_growth_deaths"])


countries = df[df["Country_name"].isin(selected_countries)]['Country_code'].unique()
colors = ['blue', 'green', 'red', 'purple', 'orange', 'yellow', 'cyan', 'magenta']

#df = cases_deaths_df[cases_deaths_df['Country_code'] == country]
df = cases_deaths_df.copy()

# Create figure
fig = go.Figure()

# Add traces (initial empty trace)
for i, country in enumerate(countries):
    dff = df.loc[df["Country_code"] == countries[i]]
    fig.add_trace(go.Scatter(x=dff['Date_reported'], 
                         y=dff[measure_chart], 
                         mode='lines', name=countries[i], line=dict(color=colors[i])))



fig.update_xaxes(ticks="outside", tickwidth=2, tickcolor='white', ticklen=10)
fig.update_yaxes(ticks="outside", tickwidth=2, tickcolor='white', ticklen=1)
fig.update_layout(yaxis_tickformat=',')
fig.update_layout(legend=dict(x=0, y=1.1), legend_orientation="h")


# Update layout
fig.update_layout(title='Animated Line Chart',
                  xaxis=dict(range=[df['Date_reported'].min(), df['Date_reported'].max()]),
                  yaxis=dict(range=[0, df.loc[df["Country_code"].isin(countries), measure_chart].max()]),
                  updatemenus=[dict(type='buttons',
                                    buttons=[dict(label='Play',
                                                  method='animate',
                                                  args=[None,
                                                        dict(frame=dict(duration=300, redraw=True), 
                                                             fromcurrent=True,
                                                             mode='immediate')
                                                        ]
                                                  )
                                             ]
                                   )
                               ]
                  )

# Animation
fig.update(frames=[
    go.Frame(
        data=[
            go.Scatter(x=df.loc[df["Country_code"] == countries[i], 'Date_reported'].iloc[:k],
                       y=df.loc[df["Country_code"] == countries[i], measure_chart].iloc[:k])
            for i in range(0, len(countries))]
    )
    for k in range(1, len(df[df['Country_code'] == 'USA']) + 1)])

# Show the animation
st.plotly_chart(fig)


st.write("""## 2. How did the vaccination campaign evolve globally?""")


measure_vaccine = st.selectbox("Metric", ["new_vaccine_doses_administered_per_hundred", 
                                          "cumulative_vaccine_doses_administered_per_hundred"])

# Read the data
vaccinations_df = pd.read_csv('C:/Users/super/Desktop/DATA_ENGINEERING/Project_git/FODE-23-22/Production_exploration/files_wrangled/vaccinations.csv')

# Create a copy of the dataframe
df = vaccinations_df.copy()

# Select the range of values for the measure
categories, ranges, colors = ranges_dict[measure_vaccine]

# Create an animated map
fig = create_animated_map(df, measure_vaccine, categories, ranges, colors, assign_color, date_col='date')

# Plot figure
st.plotly_chart(fig)


st.write("""### Correlation between vaccination and Covid-19 cases and deaths""")

countries = ['USA', 'FRA', 'ITA', 'DEU', 'GBR', 'RUS', 'BRA', 'IND']
colors = ['blue', 'green', 'red', 'purple', 'orange', 'yellow', 'cyan', 'magenta']

# Create two main figures with subplots
fig = make_subplots(rows=3, cols=1)

df = cases_deaths_df.copy()
measure = 'New_deaths_per_million'
# Add traces for each line in the first subplot
for i, country in enumerate(countries):
    mask = df["Country_code"] == country
    
    fig.add_trace(go.Scatter(x=df.loc[mask, 'Date_reported'],
                             y=df.loc[mask, measure],
                             mode='lines', name=country, line=dict(color=colors[i])), row=1, col=1)
    
    fig.add_trace(go.Scatter(x=df.loc[mask, 'Date_reported'],
                                y=df.loc[mask, 'New_cases_per_million'],
                                mode='lines', name=country, line=dict(color=colors[i])), row=3, col=1)

df = vaccinations_df.copy()
measure = 'cumulative_vaccine_doses_administered_per_hundred'

for i, country in enumerate(countries):
    mask = df["Country_code"] == country
    
    start_date = cases_deaths_df["Date_reported"].min()  # Start date of the desired time range
    end_date = cases_deaths_df["Date_reported"].max()    # End date of the desired time range
    date_r = pd.date_range(start=start_date, end=end_date)
    time_range_df = pd.DataFrame({'date': date_r})
    time_range_df['date'] = time_range_df['date'].astype(str)
    
    slice = pd.merge(df.loc[mask], time_range_df, how='right', on='date')
    #print(slice['date'])
    fig.add_trace(go.Scatter(x=slice['date'],
                             y=slice[measure],
                             mode='lines', name=country, line=dict(color=colors[i])), row=2, col=1)
    

# Show the legend for the first subplot
fig.update_traces(showlegend=True, row=1, col=1)


# Show the plot
st.plotly_chart(fig)
