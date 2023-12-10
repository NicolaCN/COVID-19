import pandas as pd
import numpy as np
import wbgapi as wb


def format_date(df: pd.DataFrame, date_col) -> pd.DataFrame:
    print("Formatting date…")
    df[date_col] = pd.to_datetime(df[date_col], format="%Y-%m-%d")
    return df

def discard_rows(df, columns):
    print("Discarding rows…")
    # For all rows where new_cases or new_deaths is negative, we keep the cumulative value but set
    # the daily change to NA. This also sets the 7-day rolling average to NA for the next 7 days.
    for col in columns:
        df.loc[df[col] < 0, col] = np.nan
    
    return df

def _inject_growth(df, prefix, periods):
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
    
def _inject_population(df, date_col, country_col='Country'):
    df_population = pd.read_csv('./files_wrangled/population_data.csv')
    # Extract year from Date_reported column and create a new column Year
    df['Year'] = df[date_col].dt.year

    # Merge the two dataframes based on Country and Year columns
    df_merged = pd.merge(df, df_population, how='left', left_on=[country_col, 'Year'], right_on=['Country', 'Year'])
    df_merged.drop('Year', axis=1, inplace=True)
    
    return df_merged

def _per_capita(df, measures):
    for measure in measures:
        pop_measure = measure + "_per_million"
        series = df[measure] / (df["population"] / 1e6)
        df[pop_measure] = series.round(decimals=3)
    #df = drop_population(df)
    return df

def convert_iso_code(df, iso_col):
    converting_table = pd.read_csv('./files_wrangled/location.csv')
    converting_table = converting_table[['alpha-2', 'alpha-3', 'name']]
    merged = pd.merge(df, converting_table, how='left', left_on=[iso_col], right_on=['alpha-2'])
    
    merged.drop([iso_col, 'alpha-2'], axis=1, inplace=True)
    merged.rename(columns={'alpha-3': 'Country_code'}, inplace=True)
    
    if iso_col == 'location_key':
        merged.rename(columns={'name': iso_col}, inplace=True)
    
    return merged

def _inject_growth_vacc(df, prefix, periods):
    vaccinated_colname = "%s_persons_vaccinated" % prefix
    fully_vaccinated_colname = "%s_persons_fully_vaccinated" % prefix
    vaccine_doses_colname = "%s_vaccine_doses_administered" % prefix
    vaccinated_growth_colname = "%s_pct_growth_persons_vaccinated" % prefix
    fully_vaccinated_growth_colname = "%s_pct_growth_persons_fully_vaccinated" % prefix
    vaccine_doses_growth_colname = "%s_pct_growth_vaccine_doses_administered" % prefix

    df[[vaccinated_colname, fully_vaccinated_colname, vaccine_doses_colname]] = (
        df[["location_key", "new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered"]]
        .groupby("location_key")[["new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered"]]
        .rolling(window=periods, min_periods=periods - 1, center=False)
        .sum()
        .reset_index(level=0, drop=True)
    )

    dff = df[["location_key", vaccinated_colname, fully_vaccinated_colname, vaccine_doses_colname]]
    dff = dff.groupby("location_key")[[vaccinated_colname, fully_vaccinated_colname, vaccine_doses_colname]]
    dff = dff.pct_change(periods=periods, fill_method=None)
    dff = dff.round(3)
    mask = np.isinf(dff) | np.isneginf(dff)
    dff = dff.where(~mask, pd.NA) * 100
    df[[vaccinated_growth_colname, fully_vaccinated_growth_colname, vaccine_doses_growth_colname]] = dff
    
    return df

def rollup(df):
    # Discard rows where location_key is null
    df.dropna(subset=["location_key"], inplace=True)
    
    # Discard rows where location_key contains "_", i.e. regional/province data
    df.drop(df[df["location_key"].str.contains("_")].index, inplace=True)
    
    return df

def _wrangle_cases_deaths(source_path='./files/cases_deaths.csv'):
    df = pd.read_csv(source_path)
    
    df = format_date(df, date_col="Date_reported")
    df = discard_rows(df, ["New_cases", "New_deaths"])
    print("Injecting growth…")
    df = _inject_growth(df, 'Weekly', 7)
    print("Injecting population…")   
    df = _inject_population(df, date_col="Date_reported")
    print("Calculating per capita…")
    df = _per_capita(df, ["New_cases", "New_deaths", "Cumulative_cases", 
                          "Cumulative_deaths", "Weekly_cases", "Weekly_deaths",])
    df = convert_iso_code(df, iso_col='Country_code')
    
    
    df.to_csv('./files_wrangled/cases_deaths.csv', index=False)
    return df

def _wrangle_vaccinations(source_path='./files/vaccinations.csv'):
    df = pd.read_csv(source_path)
    df = df.iloc[:, :8]     
    df = format_date(df, date_col="date")
    df = discard_rows(df, ["new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered"])
    df = convert_iso_code(df, iso_col='location_key')
    print("rolling up…")
    df = rollup(df) 
    print("Injecting growth…")
    df = _inject_growth_vacc(df, 'Weekly', 7)
    print("Injecting population…")
    df = _inject_population(df, date_col="date", country_col='location_key')
    df = _per_capita(df, ["new_persons_vaccinated", "new_persons_fully_vaccinated", "new_vaccine_doses_administered", 
                          "Weekly_persons_vaccinated", "Weekly_persons_fully_vaccinated", "Weekly_vaccine_doses_administered"])
    
    df.to_csv('./files_wrangled/vaccinations.csv', index=False)
    return df

def _wrangle_government_measures(source_path='./files/government_measures.csv'):
    df = pd.read_csv(source_path)
    # Apply data wrangling here
    
    # Mantain only the columns of interest
    df = df[['CountryName', 'CountryCode', 'Jurisdiction', 'Date', 'StringencyIndex_Average', 'GovernmentResponseIndex_Average', 'ContainmentHealthIndex_Average', 'EconomicSupportIndex']]
    
    
    df.to_csv('./files_wrangled/government_measures.csv', index=False)
    return df

def _wrangle_population_data():
    data = wb.data.DataFrame('SP.POP.TOTL', labels=True, time=range(2019, 2023))
    
    data["YR2023"] = data["YR2022"]
    
    data_reshaped = pd.melt(data, id_vars=['Country'], var_name='Year', value_name='population')
    data_reshaped['Year'] = data_reshaped['Year'].apply(lambda year: int(year[2:]))
    
    data_reshaped.to_csv('./files_wrangled/population_data.csv', index=False)
    #df.to_csv('/opt/airflow/dags/postgres/population_data_wrangled.csv', index=False)
    return data_reshaped
    
def _create_time_csv(dest_path='./files_wrangled/time_table.csv'):
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
    df.to_csv(path, index=False)
    return df