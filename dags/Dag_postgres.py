import airflow
import datetime
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import pandas as pd
import wbgapi as wb
import numpy as np

location_csv = 'https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv'
population_data='https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv'
cases_deaths='https://covid19.who.int/WHO-COVID-19-global-data.csv'
vaccinations='https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv'
government_measures='https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_national_v1.csv'


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=10),
    'catchup': False,
    "depends_on_past": False,
}

dag = DAG('covid_data_dag_postgres', start_date=airflow.utils.dates.days_ago(0), default_args=default_args, schedule_interval='@daily')

def create_time_csv():
    start_date = f"{2019}-01-01"
    end_date = f"{2023}-12-31"
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    data = {
        'Date': date_range,
        'Week': date_range.isocalendar().week,
        'Month': date_range.month,
        'Trimester': (date_range.month - 1) // 3 + 1,
        'Semester': (date_range.month <= 6).astype(int) + 1,
        'Year': date_range.year
        }
    
    df = pd.DataFrame(data)
    df.to_csv('/opt/airflow/dags/postgres/time_table.csv', index=False)

def download_location_table():
    response = requests.get(location_csv)
    with open('/opt/airflow/dags/postgres/location.csv', 'wb') as f:
        f.write(response.content)
    
def download_cases_deaths():
    response = requests.get(cases_deaths)
    with open('/opt/airflow/dags/postgres/cases_deaths.csv', 'wb') as f:
        f.write(response.content)

def download_vaccinations():
    response = requests.get(vaccinations)
    with open('/opt/airflow/dags/postgres/vaccinations.csv', 'wb') as f:
        f.write(response.content)

def download_government_measures():
    response = requests.get(government_measures)
    with open('/opt/airflow/dags/postgres/government_measures.csv', 'wb') as f:
        f.write(response.content)
        
# Population data from the World Bank API, used to calculate the per capita metrics 
# for every table (e.g. total_vaccinations_per_hundred, people_vaccinated_per_hundred, etc.)         
def download_population_data():
    data = wb.data.DataFrame('SP.POP.TOTL', labels=True, time=range(2019, 2023))
    
    #Manually add the population for 2023 and set it equal to the population of 2022
    data["YR2023"] = data["YR2022"]
    
    data_reshaped = pd.melt(data, id_vars=['Country'], var_name='Year', value_name='population')
    data_reshaped['Year'] = data_reshaped['Year'].apply(lambda year: int(year[2:]))
    
    data_reshaped.to_csv('/opt/airflow/dags/postgres/population_data.csv', index=False)

def format_date(df: pd.DataFrame) -> pd.DataFrame:
    print("Formatting date…")
    df["Date_reported"] = pd.to_datetime(df["Date_reported"], format="%Y-%m-%d")
    return df

def discard_rows(df):
    print("Discarding rows…")
    # For all rows where new_cases or new_deaths is negative, we keep the cumulative value but set
    # the daily change to NA. This also sets the 7-day rolling average to NA for the next 7 days.
    df.loc[df.New_cases < 0, "New_cases"] = np.nan
    df.loc[df.New_deaths < 0, "New_deaths"] = np.nan
    return df

def _inject_growth(df, prefix, periods):
    cases_colname = "%s_cases" % prefix
    deaths_colname = "%s_deaths" % prefix
    cases_growth_colname = "%s_pct_growth_cases" % prefix
    deaths_growth_colname = "%s_pct_growth_deaths" % prefix

    df[[cases_colname, deaths_colname]] = (
        df[["Country", "New_cases", "New_deaths"]]
        .groupby("Country")[["New_cases", "New_deaths"]]
        .rolling(window=periods, min_periods=periods - 1, center=False)
        .sum()
        .reset_index(level=0, drop=True)
    )
    df[[cases_growth_colname, deaths_growth_colname]] = (
        df[["Country", cases_colname, deaths_colname]]
        .groupby("Country")[[cases_colname, deaths_colname]]
        .pct_change(periods=periods, fill_method=None)
        .round(3)
        .replace([np.inf, -np.inf], pd.NA)
        * 100
    )
    
    return df
    
def _inject_population(df):
    'dags\postgres\population_data.csv'
    df_population = pd.read_csv('/opt/airflow/dags/postgres/population_data.csv')
    # Extract year from Date_reported column and create a new column Year
    df['Year'] = pd.DatetimeIndex(df['Date_reported']).year
    # 
    # Merge the two dataframes based on Country and Year columns
    df_merged = pd.merge(df, df_population, how='left', on=['Country', 'Year'])
    df_merged.drop('Year', axis=1, inplace=True)
    
    return df_merged

def _per_capita(df, measures):
    for measure in measures:
        pop_measure = measure + "_per_million"
        series = df[measure] / (df["population"] / 1e6)
        df[pop_measure] = series.round(decimals=3)
    #df = drop_population(df)
    return df

def wrangle_cases_deaths():
    df = pd.read_csv('/opt/airflow/dags/postgres/cases_deaths.csv')
    
    df = format_date(df)
    df = discard_rows(df)
    df = _inject_growth(df, 'Weekly', 7)   
    df = _inject_population(df)
    df = _per_capita(df, ["New_cases", "New_deaths", "Cumulative_cases", 
                          "Cumulative_deaths", "Weekly_cases", "Weekly_deaths",])
    
    
    df.to_csv('/opt/airflow/dags/postgres/cases_deaths_wrangled.csv', index=False)

def wrangle_vaccinations():
    df = pd.read_csv('/opt/airflow/dags/postgres/vaccinations.csv')
    # Apply data wrangling here
    # ...
    df.to_csv('/opt/airflow/dags/postgres/vaccinations_wrangled.csv', index=False)

def wrangle_government_measures():
    df = pd.read_csv('/opt/airflow/dags/postgres/government_measures.csv')
    # Apply data wrangling here
    
    # Mantain only the columns of interest
    #df = df[['CountryName', 'CountryCode', 'RegionName', 'RegionCode', 'Jurisdiction', 'Date', 'StringencyIndex_Average', 'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average', 'EconomicSupportIndex']]
    
    
    #df.to_csv('/opt/airflow/dags/postgres/government_measures_wrangled.csv', index=False)

def wrangle_population_data():
    df = pd.read_csv('/opt/airflow/dags/postgres/population_data.csv')
    # Apply data wrangling here
    # ...
    #df.to_csv('/opt/airflow/dags/postgres/population_data_wrangled.csv', index=False)
    
def wrangle_location_data():
    df = pd.read_csv('/opt/airflow/dags/postgres/location.csv')
    # Apply data wrangling here
    # ...
    #df.to_csv('/opt/airflow/dags/postgres/location_wrangled.csv', index=False)

# I KEPT (COMMENTED) THE FOLLOWING FUNCTIONS (bla_bla_query()) AS A REFERENCE, BUT 
# THEY SHOULD BE MODIFIED BASED ON THE SHAPE OF THE DATA AFTER THE WRANGLING
'''
def _create_cases_deaths_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv('/opt/airflow/dags/postgres/cases_deaths.csv')
    with open("/opt/airflow/dags/postgres/cases_deaths_inserts.sql", "w") as f:
        df_iterable = df.iterrows()

        f.write(
            "DROP TABLE IF EXISTS cases_deaths;\n"
            "CREATE TABLE cases_deaths (\n"
            "id SERIAL PRIMARY KEY,\n"
            "date_reported DATE,\n"
            "country_code VARCHAR(10),\n"
            "country VARCHAR(100),\n"
            "who_region VARCHAR(100),\n"
            "new_cases INTEGER,\n"
            "cumulative_cases INTEGER,\n"
            "new_deaths INTEGER,\n"
            "cumulative_deaths INTEGER\n"
            ");\n"
        )
        
        
        for index, row in df_iterable:
            id = index
            date_reported = row['Date_reported']
            country_code = row['Country_code']
            # If the country name contains a single quote, replace it with two single quotes 
            # (the apostrophe is a reserved character in SQL)
            country = row['Country'].replace("'", "''")
            who_region = row['WHO_region']
            new_cases = row['New_cases']
            cumulative_cases = row['Cumulative_cases']
            new_deaths = row['New_deaths']
            cumulative_deaths = row['Cumulative_deaths']

            f.write(
                "INSERT INTO cases_deaths VALUES ("
                f"'{id}', '{date_reported}', '{country_code}', '{country}', '{who_region}', {new_cases}, {cumulative_cases}, {new_deaths}, {cumulative_deaths}"
                ");\n"
            )
            
            # Just for debugging purposes, I limit the number of records to 100
            if index == 100:
                break

        f.close()
        
def _create_government_measures_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv('/opt/airflow/dags/postgres/government_measures.csv')
    # Mantain only the columns of interest
    df = df[['CountryName', 'CountryCode', 'RegionName', 'RegionCode', 'Jurisdiction', 'Date', 'StringencyIndex_Average', 'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average', 'EconomicSupportIndex']]
    
    with open("/opt/airflow/dags/postgres/government_measures_inserts.sql", "w") as f:
        df_iterable = df.iterrows()

        f.write(
            "DROP TABLE IF EXISTS government_measures;\n"
            "CREATE TABLE government_measures (\n"
            "id SERIAL PRIMARY KEY,\n"
            "country_name VARCHAR(100),\n"
            "country_code VARCHAR(10),\n"
            "region_name VARCHAR(100),\n"
            "region_code VARCHAR(10),\n"
            "jurisdiction VARCHAR(100),\n"
            "date DATE,\n"
            "stringency_index_average FLOAT,\n"
            "government_response_index_average FLOAT,\n"
            "containment_health_index_average FLOAT,\n"
            "economic_support_index FLOAT\n"
            ");\n"
        )
        
        for index, row in df_iterable:
            id = index
            country_name = row['CountryName'].replace("'", "''")
            country_code = row['CountryCode']
            region_name = row['RegionName'] if pd.notnull(row['RegionName']) else "null"
            region_code = row['RegionCode'] if pd.notnull(row['RegionCode']) else "null"
            jurisdiction = row['Jurisdiction'].replace("'", "''") if pd.notnull(row['Jurisdiction']) else "null"
            date = row['Date']
            stringency_index_average = row['StringencyIndex_Average'] if pd.notnull(row['StringencyIndex_Average']) else "null"
            government_response_index_average = row['GovernmentResponseIndex_Average'] if pd.notnull(row['GovernmentResponseIndex_Average']) else "null"
            containment_health_index_average = row['ContainmentHealthIndex_Average'] if pd.notnull(row['ContainmentHealthIndex_Average']) else "null"
            economic_support_index = row['EconomicSupportIndex'] if pd.notnull(row['EconomicSupportIndex']) else "null"

            f.write(
                "INSERT INTO government_measures VALUES ("
                f"'{id}', '{country_name}', '{country_code}', '{region_name}', '{region_code}', '{jurisdiction}', '{date}', {stringency_index_average}, {government_response_index_average}, {containment_health_index_average}, {economic_support_index}"
                ");\n"
            )
            
            # Just for debugging purposes, I limit the number of records to 100
            if index == 100:
                break

        f.close()
        
def _create_vaccinations_query(previous_epoch: int, output_folder: str):
    df = pd.read_csv('/opt/airflow/dags/postgres/vaccinations.csv')
    df.fillna("null", inplace=True)
    with open("/opt/airflow/dags/postgres/vaccinations_inserts.sql", "w") as f:
        df_iterable = df.iterrows()
        
        f.write(
            "DROP TABLE IF EXISTS vaccinations;\n"
            "CREATE TABLE vaccinations (\n"
            "id SERIAL PRIMARY KEY,\n"
            "date_ DATE,\n"
            "location_ VARCHAR(100),\n"
            "iso_code VARCHAR(10),\n"
            "total_vaccinations INTEGER,\n"
            "people_vaccinated INTEGER,\n"
            "people_fully_vaccinated INTEGER,\n"
            "daily_vaccinations_raw INTEGER,\n"
            "daily_vaccinations INTEGER,\n"
            "total_vaccinations_per_hundred FLOAT,\n"
            "people_vaccinated_per_hundred FLOAT,\n"
            "people_fully_vaccinated_per_hundred FLOAT,\n"
            "daily_vaccinations_per_million INTEGER,\n"
            "daily_people_vaccinated INTEGER,\n"
            "daily_people_vaccinated_per_hundred FLOAT\n"
            ");\n"
        )
        
        for index, row in df_iterable:
            id = index
            date = row['date']
            location = row['location']
            iso_code = row['iso_code']
            total_vaccinations = row['total_vaccinations']
            people_vaccinated = row['people_vaccinated']
            people_fully_vaccinated = row['people_fully_vaccinated']
            daily_vaccinations_raw = row['daily_vaccinations_raw']
            daily_vaccinations = row['daily_vaccinations']
            total_vaccinations_per_hundred = row['total_vaccinations_per_hundred']
            people_vaccinated_per_hundred = row['people_vaccinated_per_hundred']
            people_fully_vaccinated_per_hundred = row['people_fully_vaccinated_per_hundred']
            daily_vaccinations_per_million = row['daily_vaccinations_per_million']
            daily_people_vaccinated = row['daily_people_vaccinated']
            daily_people_vaccinated_per_hundred = row['daily_people_vaccinated_per_hundred']

            f.write(
                "INSERT INTO vaccinations VALUES ("
                f"'{id}', '{date}', '{location}', '{iso_code}', {total_vaccinations}, {people_vaccinated}, {people_fully_vaccinated}, {daily_vaccinations_raw}, {daily_vaccinations}, {total_vaccinations_per_hundred}, {people_vaccinated_per_hundred}, {people_fully_vaccinated_per_hundred}, {daily_vaccinations_per_million}, {daily_people_vaccinated}, {daily_people_vaccinated_per_hundred}"
                ");\n"
            )
            
            if index == 100:
                break

        f.close()
'''    

# Download the cases_deaths.csv file from the WHO website
download_cases_deaths = PythonOperator(
    task_id='download_cases_deaths',
    dag=dag,
    python_callable=download_cases_deaths,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

# Downlaod the vaccinations.csv file from the OWID GitHub repository
download_vaccinations = PythonOperator(
    task_id='download_vaccinations',
    dag=dag,
    python_callable=download_vaccinations,
    op_kwargs={},
    trigger_rule='all_success',
    depends_on_past=False,
)

# Download the government_measures.csv file from the OxCGRT GitHub repository
download_government_measures = PythonOperator(
    task_id='download_government_measures',
    python_callable=download_government_measures,
    dag=dag
)

# Download the population_data.csv file from the World Bank API
download_population_data_task = PythonOperator(
    task_id='download_population_data',
    python_callable=download_population_data,
    dag=dag,
)

# Download the location.csv file from the GitHub repository
download_location_data_task = PythonOperator(
    task_id='download_location_data',
    python_callable=download_location_table,
    dag=dag,
)

# Create the time_table.csv file with the time dimensions
create_time_csv = PythonOperator(
    task_id='create_time_csv',
    python_callable=create_time_csv,
    dag=dag,
)

wrangle_cases_deaths_task = PythonOperator(
    task_id='wrangle_cases_deaths',
    python_callable=wrangle_cases_deaths,
    dag=dag,
)

wrangle_vaccinations_task = PythonOperator(
    task_id='wrangle_vaccinations',
    python_callable=wrangle_vaccinations,
    dag=dag,
)

wrangle_government_measures_task = PythonOperator(
    task_id='wrangle_government_measures',
    python_callable=wrangle_government_measures,
    dag=dag,
)

# Define the task to wrangle the population data
wrangle_population_data_task = PythonOperator(
    task_id='wrangle_population_data',
    python_callable=wrangle_population_data,
    dag=dag,
)


wrangle_location_data_task = PythonOperator(
    task_id='wrangle_location_data',
    python_callable=wrangle_location_data,
    dag=dag,
)

# I KEPT THE FOLLOWING OPERATORS AS A REFERENCE, BUT THEY SHOULD BE 
# MODIFIED BASED ON THE SHAPE OF THE DATA AFTER THE WRANGLING
'''
# Create the cases_deaths_inserts.sql file with the SQL query to insert the data into the database
create_cases_deaths_query_operator = PythonOperator(
    task_id='create_cases_deaths_query_operator',
    dag=dag,
    python_callable=_create_cases_deaths_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# Create the vaccinations_inserts.sql file with the SQL query to insert the data into the database
create_vaccinations_query_operator = PythonOperator(
    task_id='create_vaccinations_query_operator',
    dag=dag,
    python_callable=_create_vaccinations_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# Create the government_measures_inserts.sql file with the SQL query to insert the data into the database
create_government_measures_query_operator = PythonOperator(
    task_id='create_government_measures_query_operator',
    dag=dag,
    python_callable=_create_government_measures_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# Create the cases_deaths table in the database and insert the data
create_cases_deaths_table = PostgresOperator(
    task_id='create_cases_deaths_table',
    dag=dag,
    postgres_conn_id='postgres_default',
    sql='/postgres/cases_deaths_inserts.sql',
)

# Create the vaccinations table in the database and insert the data
create_vaccinations_table = PostgresOperator(
    task_id='create_vaccinations_table',
    postgres_conn_id='postgres_default',
    sql='/postgres/vaccinations_inserts.sql',
    dag=dag
)

# Create the government_measures table in the database and insert the data
create_government_measures_table = PostgresOperator(
    task_id='create_government_measures_table',
    postgres_conn_id='postgres_default',
    sql='/postgres/government_measures_inserts.sql',
    dag=dag
)
'''

[download_cases_deaths, download_population_data_task] >> wrangle_cases_deaths_task #>> create_cases_deaths_query_operator >> create_cases_deaths_table >> print_vaccinations_operator
[download_vaccinations, download_population_data_task] >> wrangle_vaccinations_task #>> create_vaccinations_query_operator >> create_vaccinations_table
download_government_measures >> wrangle_government_measures_task #>> create_government_measures_query_operator >> create_government_measures_table
download_population_data_task >> wrangle_population_data_task
download_location_data_task >> wrangle_location_data_task #>> create_location_table
create_time_csv #>> create_time_table
#[wrangle_cases_deaths_task, wrangle_vaccinations_task, wrangle_government_measures_task, wrangle_location_data_task] >> [join_wrangled_data_task]
#join_wrangled_data_task >> create_big_table


 