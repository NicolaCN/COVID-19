# Imports
import airflow
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

from pymongo import MongoClient
import datetime


# Data structures
## Datasets URLs
datasets = {
    "location": "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.json", # JSON
    "population_data": "https://api.worldbank.org/v2/en/indicator/SP.POP.TOTL?downloadformat=csv", # CSV
    "cases_deaths": "https://covid19.who.int/WHO-COVID-19-global-data.csv", # CSV
    "vaccinations": "https://storage.googleapis.com/covid19-open-data/v3/vaccinations.csv", # CSV
    #"vaccinations": "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv"
    "government_measures": "https://raw.githubusercontent.com/OxCGRT/covid-policy-dataset/main/data/OxCGRT_compact_national_v1.csv", # CSV
    "geojson": "https://datahub.io/core/geo-countries/r/countries.geojson", # GEOJSON
    "gdp_growth": "https://data.worldbank.org/indicator/NY.GDP.MKTP.KD.ZG?downloadformat=csv", # CSV
}

# DBs connectors
mongoClient = MongoClient("mongodb://mongo:27017/")
mongoDatabase = mongoClient["covid"]

# Airflow default arguments
default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "concurrency": 1,
    "schedule_interval": None,
    "retries": 1,
    "retry_delay": datetime.timedelta(seconds=10),
    "catchup": False,
    "depends_on_past": False,
}

dag = DAG("dag", default_args=default_args, schedule_interval="@daily")

# Functions
def _create_time_csv():
    import pandas as pd
    start_date = f"{2019}-01-01"
    end_date = f"{2023}-12-31"
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    data = {
        "Date": date_range,
        "Week": date_range.isocalendar().week,
        "Month": date_range.month,
        "Trimester": (date_range.month - 1) // 3 + 1,
        "Semester": (date_range.month <= 6).astype(int) + 1,
        "Year": date_range.year,
    }

    df = pd.DataFrame(data)
    df.to_csv("/opt/airflow/dags/postgres/time_table.csv", index=False)

def csv_to_json(filename, header=None):
    import pandas as pd
    data = pd.read_csv(filename, header=header)
    return data.to_dict("records")

def _download_location_table():
    import requests
    response = requests.get(datasets["location"])
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception("Error while downloading file")


def _download_cases_deaths():
    import requests
    import io 
    
    response = requests.get(datasets["cases_deaths"])
    if response.status_code == 200:
        return io.StringIO(response.content.decode())
    else:
        raise Exception("Error while downloading file")


def _download_vaccinations():
    import pandas as pd
    import urllib.request
    with urllib.request.urlopen(datasets["vaccinations"]) as file:
        with open("/opt/airflow/dags/files/vaccinations.csv", "wb") as new_file:
            new_file.write(file.read())

def _download_government_measures():
    import requests
    import io
    
    response = requests.get(datasets["government_measures"])
    if response.status_code == 200:
        return io.StringIO(response.content.decode())
    else:
        raise Exception("Error while downloading file")


def _store_location_csv(ti):
    # Code to store location.csv in MongoDB
    buffer = ti.xcom_pull(key='return_value', task_ids='ingestion.download_location_data')

    mongoDatabase.drop_collection("location")

    collection = mongoDatabase["location"]
    collection.insert_many(buffer)

def _store_population_data():
    import wbgapi as wb
    data = wb.data.DataFrame("SP.POP.TOTL", labels=True, time=range(2019, 2023))

    mongoDatabase.drop_collection("population_data")

    collection = mongoDatabase["population_data"]
    collection.insert_many(data.to_dict("records"))


# For big files it might be better to first store
def _store_cases_deaths(ti):
    import pandas as pd
    buffer = ti.xcom_pull(key='return_value', task_ids='ingestion.download_cases_deaths')

    df = pd.read_csv(buffer)
    json = df.to_dict("records")

    mongoDatabase.drop_collection("cases_deaths")

    collection = mongoDatabase["cases_deaths"]
    collection.insert_many(json)


def _store_vaccinations(ti):
    import pandas as pd

    df = pd.read_csv("/opt/airflow/dags/files/vaccinations.csv", header=0).iloc[:, :9]
    df = rollup(df)
    json = df.to_dict("records")

    mongoDatabase.drop_collection("vaccinations")

    collection = mongoDatabase["vaccinations"]
    collection.insert_many(json)


def _store_government_measures(ti):
    import pandas as pd
    buffer = ti.xcom_pull(key='return_value', task_ids='ingestion.download_government_measures')

    df = pd.read_csv(buffer)
    df = df[['CountryName', 'CountryCode', 'Jurisdiction', 'Date', 'StringencyIndex_Average', 'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average', 'EconomicSupportIndex']]
    json = df.to_dict("records")

    mongoDatabase.drop_collection("government_measures")

    collection = mongoDatabase["government_measures"]
    collection.insert_many(json)

def _store_gdp_growth():
    import wbgapi as wb
    data = wb.data.DataFrame("NY.GDP.MKTP.KD.ZG", labels=True, time=range(2019, 2023))

    mongoDatabase.drop_collection("gdp_growth")

    collection = mongoDatabase["gdp_growth"]
    collection.insert_many(data.to_dict("records"))


def _pull_location_csv():
    import pandas as pd
    # Code to pull location.csv from MongoDB
    collection = mongoDatabase["location"]
    data = pd.DataFrame(list(collection.find())).drop("_id", axis=1)
    data.to_csv("/opt/airflow/dags/files/location.csv", index=False)


def _pull_population_data():
    import pandas as pd
    collection = mongoDatabase["population_data"]
    data = pd.DataFrame(list(collection.find())).drop("_id", axis=1)
    data.to_csv("/opt/airflow/dags/files/population_data.csv", index=False)


def _pull_cases_deaths():
    import pandas as pd
    collection = mongoDatabase["cases_deaths"]
    data = pd.DataFrame(list(collection.find())).drop("_id", axis=1)
    data.to_csv("/opt/airflow/dags/files/cases_deaths.csv", index=False)


def _pull_vaccinations():
    import pandas as pd
    collection = mongoDatabase["vaccinations"]
    data = pd.DataFrame(list(collection.find())).drop("_id", axis=1)
    data.to_csv("/opt/airflow/dags/files/vaccinations.csv", index=False)


def _pull_government_measures():
    import pandas as pd
    collection = mongoDatabase  ["government_measures"]
    data = pd.DataFrame(list(collection.find())).drop("_id", axis=1)
    data.to_csv("/opt/airflow/dags/files/government_measures.csv", index=False)

def format_date(df, date_col):
    import pandas as pd
    
    print("Formatting date…")
    df[date_col] = pd.to_datetime(df[date_col], format="%Y-%m-%d")
    return df

def discard_rows(df, columns):
    import numpy as np
    
    print("Discarding rows…")
    # For all rows where new_cases or new_deaths is negative, we keep the cumulative value but set
    # the daily change to NA. This also sets the 7-day rolling average to NA for the next 7 days.
    for col in columns:
        df.loc[df[col] < 0, col] = np.nan
    
    return df

def _inject_growth(df, prefix, periods):
    import pandas as pd
    import numpy as np
    
    print("assigning variables…")
    cases_colname = "%s_cases" % prefix
    deaths_colname = "%s_deaths" % prefix
    cases_growth_colname = "%s_pct_growth_cases" % prefix
    deaths_growth_colname = "%s_pct_growth_deaths" % prefix
    
    print("Calculating growth…")
    df[[cases_colname, deaths_colname]] = (
        df[["Country", "New_cases", "New_deaths"]]
        .groupby("Country")[["New_cases", "New_deaths"]]
        .rolling(window=periods, min_periods=periods - 1, center=False)
        .sum()
        .reset_index(level=0, drop=True)
    )
    
    print("Calculating pct growth…")
    '''df[[cases_growth_colname, deaths_growth_colname]] = (
        df[["Country_code", cases_colname, deaths_colname]]
        .groupby("Country_code")[[cases_colname, deaths_colname]]
        .pct_change(periods=periods, fill_method=None)
        .round(3)
        .replace([np.inf, -np.inf], pd.NA)
        * 100
    )'''
    
    dff = df[["Country_code", cases_colname, deaths_colname]]
    dff = dff.groupby("Country_code")[[cases_colname, deaths_colname]]
    dff = dff.pct_change(periods=periods, fill_method=None)
    dff = dff.round(3)
    mask = np.isinf(dff) | np.isneginf(dff)
    dff = dff.where(~mask, pd.NA) * 100
    df[[cases_growth_colname, deaths_growth_colname]] = dff
    print("Done")
    return df
    
def _inject_population(df, date_col, country_col='Country_code'):
    import pandas as pd
    
    df_population = pd.read_csv("/opt/airflow/dags/files_wrangled/population_data.csv")
    
    # Make sure date_col is in datetime format
    df[date_col] = pd.to_datetime(df[date_col], format="%Y-%m-%d")
    
    # Extract year from Date_reported column and create a new column Year
    df['Year'] = df[date_col].dt.year

    # Merge the two dataframes based on Country and Year columns
    df_merged = pd.merge(df, df_population, how='left', left_on=[country_col, 'Year'], right_on=['Country', 'Year'])
    df_merged.drop('Year', axis=1, inplace=True)
    
    return df_merged

def _per_capita(df, measures, denom=1e6, denom_name='_per_million'):
    import pandas as pd
    
    for measure in measures:
        pop_measure = measure + denom_name
        series = df[measure] / (df["population"] / denom)
        df[pop_measure] = series.round(decimals=3)
    #df = drop_population(df)
    return df

def convert_iso_code(df, iso_col, country_col):
    import pandas as pd
    
    converting_table = pd.read_csv("/opt/airflow/dags/files_wrangled/location.csv")
    converting_table = converting_table[['alpha-2', 'alpha-3', 'name']]
    merged = pd.merge(df, converting_table, how='left', left_on=[iso_col], right_on=['alpha-2'])
    
    merged.drop([iso_col, 'alpha-2'], axis=1, inplace=True)
    merged.rename(columns={'alpha-3': 'Country_code'}, inplace=True)
    
    if country_col is None:
        merged.rename(columns={'name': 'Country_name'}, inplace=True)
    else:
        merged.drop(country_col, axis=1, inplace=True)
        merged.rename(columns={'name': 'Country_name'}, inplace=True)
            
    return merged

def _inject_growth_vacc(df, prefix, periods):
    import pandas as pd
    import numpy as np
    
    vaccinated_colname = "%s_persons_vaccinated" % prefix
    fully_vaccinated_colname = "%s_persons_fully_vaccinated" % prefix
    vaccine_doses_colname = "%s_vaccine_doses_administered" % prefix
    vaccinated_growth_colname = "%s_pct_growth_persons_vaccinated" % prefix
    fully_vaccinated_growth_colname = "%s_pct_growth_persons_fully_vaccinated" % prefix
    vaccine_doses_growth_colname = "%s_pct_growth_vaccine_doses_administered" % prefix

    df[[vaccinated_colname, fully_vaccinated_colname, vaccine_doses_colname]] = (
        df[["Country_code", "new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered"]]
        .groupby("Country_code")[["new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered"]]
        .rolling(window=periods, min_periods=periods - 1, center=False)
        .sum()
        .reset_index(level=0, drop=True)
    )

    dff = df[["Country_code", vaccinated_colname, fully_vaccinated_colname, vaccine_doses_colname]]
    dff = dff.groupby("Country_code")[[vaccinated_colname, fully_vaccinated_colname, vaccine_doses_colname]]
    dff = dff.pct_change(periods=periods, fill_method=None)
    dff = dff.round(3)
    mask = np.isinf(dff) | np.isneginf(dff)
    dff = dff.where(~mask, pd.NA) * 100
    df[[vaccinated_growth_colname, fully_vaccinated_growth_colname, vaccine_doses_growth_colname]] = dff
    
    return df

def rollup(df):
    import pandas as pd
    
    # Discard rows where location_key is null
    df.dropna(subset=["location_key"], inplace=True)
    
    # Discard rows where location_key contains "_", i.e. regional/province data
    df.drop(df[df["location_key"].str.contains("_")].index, inplace=True)
    
    return df

def allign_time_ranges(df):
    import pandas as pd
    
    # Step 1: Create a DataFrame with the desired time range
    start_date = "2020-12-02"  # Start date of the desired time range
    end_date = df["date"].max()    # End date of the desired time range
    date_r = pd.date_range(start=start_date, end=end_date)

    # Create a new DataFrame with 'country' and 'date' columns
    countries = pd.Series(df['Country_code'].unique(), name='country')
    time_range_df = pd.DataFrame({'date': date_r})
    time_range_df['date'] = time_range_df['date'].astype(str)

    dfm = time_range_df.merge(countries, how='cross').sort_values(by=['country', 'date']).reset_index(drop=True)

    m = df.merge(dfm, how='right', left_on=['Country_code', 'date'], right_on=['country', 'date']).sort_values(by=['country', 'date']).reset_index(drop=True)
    
    m.drop(columns=['Country_code'], inplace=True)
    m.rename(columns={'country': 'Country_code'}, inplace=True)
    
    return m

def _wrangle_cases_deaths(source_path="/opt/airflow/dags/files/cases_deaths.csv"):
    import pandas as pd
    
    df = pd.read_csv(source_path)
    
    df = format_date(df, date_col="Date_reported")
    df = discard_rows(df, ["New_cases", "New_deaths"])
    print("Injecting growth…")
    df = _inject_growth(df, 'Weekly', 7)
    print("Converting ISO code…")
    df = convert_iso_code(df, iso_col='Country_code', country_col='Country')
    print("Injecting population…")   
    df = _inject_population(df, date_col="Date_reported")
    print("Calculating per capita…")
    df = _per_capita(df, ["New_cases", "New_deaths", "Cumulative_cases", 
                          "Cumulative_deaths", "Weekly_cases", "Weekly_deaths",])
    
    
    
    df.to_csv("/opt/airflow/dags/files_wrangled/cases_deaths.csv", index=False)

def _wrangle_vaccinations(source_path="/opt/airflow/dags/files/vaccinations.csv"):
    import pandas as pd
    
    df = pd.read_csv(source_path)
    df = df.iloc[:, :8]     
    print("rolling up…")
    df = rollup(df) 
    df = convert_iso_code(df, iso_col='location_key', country_col=None)
    df = allign_time_ranges(df)
    df = format_date(df, date_col="date")
    df = discard_rows(df, ["new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered"])
    print("Injecting growth…")
    df = _inject_growth_vacc(df, 'Weekly', 7)
    print("Injecting population…")
    df = _inject_population(df, date_col="date", country_col='Country_code')
    df = _per_capita(df, ["new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered", 
                          "Weekly_persons_vaccinated", "Weekly_persons_fully_vaccinated", "Weekly_vaccine_doses_administered",
                          "cumulative_vaccine_doses_administered"],
                     denom=1e2,
                     denom_name='_per_hundred')
    
    df.to_csv("/opt/airflow/dags/files_wrangled/vaccinations.csv", index=False)

def _wrangle_government_measures(source_path="/opt/airflow/dags/files/government_measures.csv"):
    import pandas as pd
    
    df = pd.read_csv(source_path)
    # Apply data wrangling here
    
    # Mantain only the columns of interest
    df = df[['CountryName', 'CountryCode', 'Jurisdiction', 'Date', 'StringencyIndex_Average', 'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average', 'EconomicSupportIndex']]
    
    
    df.to_csv('/opt/airflow/dags/files_wrangled/government_measures.csv', index=False)

def _wrangle_population_data():
    import wbgapi as wb
    import pandas as pd
    
    data = wb.data.DataFrame('SP.POP.TOTL', labels=True, time=range(2019, 2023))
    
    data["YR2023"] = data["YR2022"]
    data = data.reset_index()

    data_reshaped = pd.melt(data, id_vars=['economy', 'Country'], var_name='Year', value_name='population')
    data_reshaped.drop('Country', axis=1, inplace=True)
    data_reshaped['Year'] = data_reshaped['Year'].apply(lambda year: int(year[2:]))
    data_reshaped.rename(columns={'economy': 'Country'}, inplace=True)
    
    data_reshaped.to_csv('/opt/airflow/dags/files_wrangled/population_data.csv', index=False)
    
def _create_time_csv(dest_path='/opt/airflow/dags/files_wrangled/time.csv'):
    import pandas as pd
    
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
    df.to_csv(dest_path, index=False)
    

def _wrangle_location_data():
    import pandas as pd
    df = pd.read_csv("/opt/airflow/dags/files/location.csv")
    # Apply data wrangling here
    # ...
    df.to_csv('/opt/airflow/dags/files_wrangled/location.csv', index=False)



def _upload_cases_deaths():
    import pandas as pd
    from sqlalchemy import create_engine
    
    conn_string = "postgresql://airflow:airflow@postgres:5432/postgres"
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv("/opt/airflow/dags/files_wrangled/cases_deaths.csv")
    df.to_sql("cases_deaths", con=conn, if_exists="replace", index=False)
    


def _upload_vaccinations():
    import pandas as pd
    from sqlalchemy import create_engine
    
    conn_string = "postgresql://airflow:airflow@postgres:5432/postgres"
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv("/opt/airflow/dags/files_wrangled/vaccinations.csv")
    df.to_sql("vaccinations", con=conn, if_exists="replace", index=False)


def _upload_government_measures():
    import pandas as pd
    from sqlalchemy import create_engine
    
    conn_string = "postgresql://airflow:airflow@postgres:5432/postgres"
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv("/opt/airflow/dags/files_wrangled/government_measures.csv")
    df.to_sql("government_measures", con=conn, if_exists="replace", index=False)


def _upload_time():
    import pandas as pd
    from sqlalchemy import create_engine
    
    conn_string = "postgresql://airflow:airflow@postgres:5432/postgres"
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv("/opt/airflow/dags/files_wrangled/time.csv")
    df.to_sql("time_table", con=conn, if_exists="replace", index=False)


def _upload_location():
    import pandas as pd
    from sqlalchemy import create_engine
    
    conn_string = "postgresql://airflow:airflow@postgres:5432/postgres"
    db = create_engine(conn_string)
    conn = db.connect()
    df = pd.read_csv("/opt/airflow/dags/files_wrangled/location.csv")
    df.to_sql("location", con=conn, if_exists="replace", index=False)

def _pull_cases_deaths_sql():
    import csv 
    import pandas as pd
    import psycopg2
    
    # Define the function implementation here
    connection = psycopg2.connect(user="airflow",
                              password="airflow",
                              host="postgres",
                              port="5432",
                              database="postgres")
    
    # Execute the select query
    query = """SELECT cd."Date_reported", cd."Country_code", cd."Country_name",
                  cd."New_cases_per_million", cd."Cumulative_cases_per_million", 
                  cd."Weekly_pct_growth_cases", cd."Cumulative_deaths_per_million",
                  cd."New_deaths_per_million", cd."Weekly_pct_growth_deaths" 
                      FROM cases_deaths as cd"""
                                                
    # Fetch all the rows
    sql_query = pd.read_sql_query(query, con=connection)
    df = pd.DataFrame(sql_query, columns = ["Date_reported", "Country_code", "Country_name",
                  "New_cases_per_million", "Cumulative_cases_per_million", 
                  "Weekly_pct_growth_cases", "Cumulative_deaths_per_million",
                  "New_deaths_per_million", "Weekly_pct_growth_deaths"])


    
    # Define the path for the output CSV file
    output_path = "/opt/airflow/dags/files_production/cases_deaths.csv"

    df.to_csv(output_path)


def _pull_vaccinations_sql():
    import csv 
    import pandas as pd
    import psycopg2
    
    # Define the function implementation here
    connection = psycopg2.connect(user="airflow",
                              password="airflow",
                              host="postgres",
                              port="5432",
                              database="postgres")
    
    # Execute the select query
    query = """SELECT vc."date", vc."Country_code", vc."Country_name",
                      vc."new_vaccine_doses_administered_per_hundred", vc."cumulative_vaccine_doses_administered_per_hundred"
               FROM vaccinations as vc"""
                                                
    # Fetch all the rows
    sql_query = pd.read_sql_query(query, con=connection)
    df = pd.DataFrame(sql_query, columns = ["date", "Country_code", "Country_name",
                                            "new_vaccine_doses_administered_per_hundred", 
                                            "cumulative_vaccine_doses_administered_per_hundred"])


    
    # Define the path for the output CSV file
    output_path = "/opt/airflow/dags/files_production/vaccinations.csv"

    df.to_csv(output_path)

# TaskGroups
with TaskGroup("ingestion", dag=dag) as ingestion:
    ingestion_start = DummyOperator(
        task_id="ingestion_start",
        dag=dag,
    )

    # Download the cases_deaths.csv file from the WHO website
    download_cases_deaths = PythonOperator(
        task_id="download_cases_deaths",
        dag=dag,
        python_callable=_download_cases_deaths,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Downlaod the vaccinations.csv file from the OWID GitHub repository
    download_vaccinations = PythonOperator(
        task_id="download_vaccinations",
        dag=dag,
        python_callable=_download_vaccinations,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )
    
    '''download_vaccinations = BashOperator(
        task_id="download_vaccinations",
        bash_command=f"wget -O /opt/airflow/dags/postgres/vaccinations.csv {datasets['vaccinations']}",
        dag=dag,
    )'''

    # Download the government_measures.csv file from the OxCGRT GitHub repository
    download_government_measures = PythonOperator(
        task_id="download_government_measures",
        dag=dag,
        python_callable=_download_government_measures,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Download the location.csv file from the GitHub repository
    download_location_data = PythonOperator(
        task_id="download_location_data",
        dag=dag,
        python_callable=_download_location_table,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )
    
    ingestion_middle = DummyOperator(
        task_id="ingestion_middle",
        dag=dag,
    )

    # Store the location_csv in MongoDB
    store_location_csv = PythonOperator(
        task_id="store_location_csv",
        dag=dag,
        python_callable=_store_location_csv,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Store the population_data in MongoDB
    store_population_data = PythonOperator(
        task_id="store_population_data",
        dag=dag,
        python_callable=_store_population_data,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Store the cases_deaths in MongoDB
    store_cases_deaths = PythonOperator(
        task_id="store_cases_deaths",
        dag=dag,
        python_callable=_store_cases_deaths,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Store the vaccinations in MongoDB
    store_vaccinations = PythonOperator(
        task_id="store_vaccinations",
        dag=dag,
        python_callable=_store_vaccinations,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Store the government_measures in MongoDB
    store_government_measures = PythonOperator(
        task_id="store_government_measures",
        dag=dag,
        python_callable=_store_government_measures,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    store_gdp_growth = PythonOperator(
        task_id="store_gdp_growth",
        dag=dag,
        python_callable=_store_gdp_growth,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    ingestion_end = DummyOperator(
        task_id="ingestion_end", dag=dag, trigger_rule="all_success"
    )

with TaskGroup("staging", dag=dag) as staging:
    staging_start = DummyOperator(
        task_id="staging_start",
        dag=dag,
    )

    staging_end = DummyOperator(
        task_id="staging_end", dag=dag, trigger_rule="all_success"
    )

    # Pull data from Mongo for location_csv
    pull_location_csv = PythonOperator(
        task_id="pull_location_csv",
        dag=dag,
        python_callable=_pull_location_csv,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Pull data from Mongo for population_data
    pull_population_data = PythonOperator(
        task_id="pull_population_data",
        dag=dag,
        python_callable=_pull_population_data,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Pull data from Mongo for cases_deaths
    pull_cases_deaths = PythonOperator(
        task_id="pull_cases_deaths",
        dag=dag,
        python_callable=_pull_cases_deaths,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Pull data from Mongo for vaccinations
    pull_vaccinations = PythonOperator(
        task_id="pull_vaccinations",
        dag=dag,
        python_callable=_pull_vaccinations,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Pull data from Mongo for government_measures
    pull_government_measures = PythonOperator(
        task_id="pull_government_measures",
        dag=dag,
        python_callable=_pull_government_measures,
        op_kwargs={},
        trigger_rule="all_success",
        depends_on_past=False,
    )

    # Create the time_table.csv file with the time dimensions
    create_time_csv = PythonOperator(
        task_id="create_time_csv",
        python_callable=_create_time_csv,
        dag=dag,
    )

    wrangle_cases_deaths_task = PythonOperator(
        task_id="wrangle_cases_deaths",
        python_callable=_wrangle_cases_deaths,
        dag=dag,
    )

    wrangle_vaccinations_task = PythonOperator(
        task_id="wrangle_vaccinations",
        python_callable=_wrangle_vaccinations,
        dag=dag,
    )

    wrangle_government_measures_task = PythonOperator(
        task_id="wrangle_government_measures",
        python_callable=_wrangle_government_measures,
        dag=dag,
    )

    wrangle_population_data_task = PythonOperator(
        task_id="wrangle_population_data",
        python_callable=_wrangle_population_data,
        dag=dag,
    )

    wrangle_location_data_task = PythonOperator(
        task_id="wrangle_location_data",
        python_callable=_wrangle_location_data,
        dag=dag,
    )

    '''create_time_table = PostgresOperator(
        task_id="create_time_table",
        dag=dag,
        postgres_conn_id="postgres_default",
        sql="sql/create_time_table.sql",
        trigger_rule="all_success",
    )

    create_location_table = PostgresOperator(
        task_id="create_location_table",
        dag=dag,
        postgres_conn_id="postgres_default",
        sql="sql/create_location_table.sql",
        trigger_rule="all_success",
    )

    create_cases_deaths_table = PostgresOperator(
        task_id="create_cases_deaths_table",
        dag=dag,
        postgres_conn_id="postgres_default",
        sql="sql/create_cases_deaths_table.sql",
    )

    create_vaccinations_table = PostgresOperator(
        task_id="create_vaccinations_table",
        postgres_conn_id="postgres_default",
        sql="sql/create_vaccinations_table.sql",
        dag=dag,
    )

    create_government_measures_table = PostgresOperator(
        task_id="create_government_measures_table",
        postgres_conn_id="postgres_default",
        sql="sql/create_government_measures_table.sql",
        dag=dag,
    )'''

    upload_cases_deaths = PythonOperator(
        task_id="upload_cases_deaths",
        python_callable=_upload_cases_deaths,
        dag=dag,
    )

    upload_vaccinations = PythonOperator(
        task_id="upload_vaccinations",
        python_callable=_upload_vaccinations,
        dag=dag,
    )

    upload_government_measures = PythonOperator(
        task_id="upload_government_measures",
        python_callable=_upload_government_measures,
        dag=dag,
    )

    upload_time = PythonOperator(
        task_id="upload_time",
        python_callable=_upload_time,
        dag=dag,
    )

    upload_location = PythonOperator(
        task_id="upload_location",
        python_callable=_upload_location,
        dag=dag,
    )
    

with TaskGroup("production", dag=dag) as production:
    
    production_start = DummyOperator(
        task_id="production_start",
        dag=dag,
    )
    
    pull_cases_deaths_sql = PythonOperator(
        task_id="pull_cases_deaths_sql",
        python_callable=_pull_cases_deaths_sql,
        dag=dag,
    )
    
    pull_vaccinations_sql = PythonOperator(
        task_id="pull_vaccinations_sql",
        python_callable=_pull_vaccinations_sql,
        dag=dag,
    )
    
    production_end = DummyOperator(
        task_id="production_end",
        dag=dag,
    )


# DAGs
ingestion_start >> [
    download_cases_deaths,
    download_location_data,
    download_government_measures,
    download_vaccinations,
    store_population_data,
    store_gdp_growth
] 

download_cases_deaths >> store_cases_deaths
download_location_data >> store_location_csv
download_government_measures >> store_government_measures
download_location_data >> store_location_csv
download_vaccinations >> store_vaccinations

[
    store_cases_deaths, 
    store_location_csv, 
    store_government_measures, 
    store_vaccinations, 
    store_population_data,
    store_gdp_growth,
] >> ingestion_end

ingestion_end >> staging_start

staging_start >> [
    pull_location_csv,
    pull_population_data,
    pull_cases_deaths,
    pull_vaccinations,
    pull_government_measures,
]
(
    [pull_cases_deaths, wrangle_population_data_task, wrangle_location_data_task]
    >> wrangle_cases_deaths_task
    #>> create_cases_deaths_table
    >> upload_cases_deaths
)
(
    [pull_vaccinations, wrangle_population_data_task, wrangle_location_data_task]
    >> wrangle_vaccinations_task
    #>> create_vaccinations_table
    >> upload_vaccinations
)
(
    pull_government_measures
    >> wrangle_government_measures_task
    #>> create_government_measures_table
    >> upload_government_measures
)
(
    pull_location_csv
    >> wrangle_location_data_task
    #>> create_location_table
    >> upload_location
)
pull_population_data >> wrangle_population_data_task
#staging_start >> create_time_csv >> create_time_table >> upload_time
staging_start >> create_time_csv >> upload_time

[
    upload_cases_deaths,
    upload_vaccinations,
    upload_government_measures,
    upload_location,
    upload_time,
] >> staging_end

staging_end >> production_start

production_start >> [  
    pull_cases_deaths_sql,
    pull_vaccinations_sql,
] >> production_end
