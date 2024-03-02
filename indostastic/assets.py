import datetime
import json
import os
import io
import re
import numpy as np
import requests
from dagster import asset, AssetExecutionContext
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from constants import *


@asset
def init():
    cur_time = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
    os.makedirs(f"data/{cur_time}/csv_tables", exist_ok=True)
    with open("data/info.txt", "w") as f:
        f.write(str({"cur_time": cur_time}))
        f.close()


@asset(deps=[init])
def extract_gdp_html(context: AssetExecutionContext) -> None:
    info = {}
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    headers = {'Accept-Encoding': 'utf8'}
    html = requests.get(GDP_URL, headers=headers).text
    year = re.findall(r'id="([0-9]{4})_data"', html, re.IGNORECASE)[0]
    soup = BeautifulSoup(html, "html.parser")
    html_table = soup.find_all('table', {'class': 'wikitable'})[0]
    dirty = html_table.find_all_next("caption", limit=1)
    if dirty:
        dirty[0].extract(_self_index=dirty[0].parent.index(dirty[0]))
    dirty = html_table.find_all_next("tr", limit=1)
    if dirty and dirty[0].text.index("exchange"):
        dirty[0].extract(_self_index=dirty[0].parent.index(dirty[0]))
    dirty = html_table.find_all_next("sup")
    if dirty:
        for a_dirty in dirty:
            a_dirty.extract(_self_index=a_dirty.parent.index(a_dirty))
    with open(f"data/{cur_time}/gdp_extract_{year}.htm", "w") as f:
        f.write(str(html_table))
        f.close()
    with open("data/info.txt", "w") as f:
        info["year"] = year
        f.write(str(info))
        f.close()


@asset(deps=[extract_gdp_html])
def transform_gdp_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time, year = info['cur_time'], info['year']
        f.close()
    with open(f"data/{cur_time}/gdp_extract_{year}.htm", "r") as f:
        html_gdp = f.read()
        f.close()
        df = pd.read_html(io.StringIO(html_gdp), index_col=None, header=0)[0]
        df = df[df['Rank'].str.isdigit()]
        df.drop(inplace=True, columns=['Rank', 'GDP PPP'])
        df.rename(inplace=True, columns={
            'GDP (in billion Rp)': 'GDP Nominal (billion RP)',
            'GDP Nominal': 'GDP Nominal (billion USD)',
        })
        ids = range(1, len(df['Province']) + 1)
        df.set_index('Province', drop=False, inplace=True)
        df.sort_index(inplace=True)
        df.insert(loc=0, column="Id", value=ids)
        df.set_index("Id", drop=False, inplace=True)
        df.to_csv(f"data/{cur_time}/gdp_transform_{year}.csv", index=False)


@asset(deps=[transform_gdp_data])
def load_gdp_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time, year = info['cur_time'], info['year']
        f.close()
    df = pd.read_csv(f"data/{cur_time}/gdp_transform_{year}.csv", )
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    count = df.to_sql(name='gdp', con=engine, index=False, if_exists='replace')
    context.log.info(f"!!!!!!!!!!!!!! Inserted {count} GDP rows")
    df.to_csv(f"data/{cur_time}/csv_tables/gdp.csv", index=False)

    df.drop(columns=["GDP Nominal (billion RP)", "GDP Nominal (billion USD)"], inplace=True)
    df.to_sql(name='provinces', con=engine, index=False, if_exists='replace')
    df.to_csv(f"data/{cur_time}/csv_tables/provinces.csv", index=False)

    regions = df["Region"].unique().tolist()
    reg_ids = range(1, len(regions) + 1)
    df = pd.DataFrame(zip(reg_ids, regions), index=None, columns=["Region Id", "Region"])
    df.to_sql(name='regions', con=engine, index=False, if_exists='replace')
    df.to_csv(f"data/{cur_time}/csv_tables/regions.csv", index=False)


@asset(deps=[init])
def extract_grp_html() -> None:
    headers = {'Accept-Encoding': 'utf8'}
    html = requests.get(GRP_URL, headers=headers).text
    info = {}
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    year = re.findall(r'id="([0-9]{4})_Data"', html, re.IGNORECASE)[0]
    soup = BeautifulSoup(html, "html.parser")
    html_table = soup.find_all('table', {'class': 'wikitable'})[0]
    dirty = html_table.find_all_next("caption", limit=1)
    if dirty:
        dirty[0].extract(_self_index=dirty[0].parent.index(dirty[0]))
    dirty = html_table.find_all_next("tr", limit=1)
    if dirty and dirty[0].text.index("exchange"):
        dirty[0].extract(_self_index=dirty[0].parent.index(dirty[0]))
    dirty = html_table.find_all_next("sup")
    if dirty:
        for a_dirty in dirty:
            a_dirty.extract(_self_index=a_dirty.parent.index(a_dirty))
    with open(f"data/{cur_time}/grp_extract_{year}.htm", "w") as f:
        f.write(str(html_table))
        f.close()
    with open("data/info.txt", "w") as f:
        info["year"] = year
        f.write(str(info))
        f.close()


@asset(deps=[extract_grp_html])
def transform_grp_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time, year = info['cur_time'], info['year']
        f.close()
    with open(f"data/{cur_time}/grp_extract_{year}.htm", "r") as f:
        html_grp = f.read()
        f.close()
        df = pd.read_html(io.StringIO(html_grp), index_col=None, header=0)[0]
        df = df[df['Rank'].str.isdigit()]
        df.drop(inplace=True, columns=['Rank', 'Per capita.1'])
        df.rename(inplace=True, columns={
            'Per capita (in thousand Rp)': 'Per capita (thousand RP)',
            'Per capita': 'Per capita Nominal (USD)',
        })
        ids = range(1, len(df['Province']) + 1)
        df.set_index('Province', drop=False, inplace=True)
        df.sort_index(inplace=True)
        df.insert(loc=0, column="Id", value=ids)
        df.set_index("Id", drop=False, inplace=True)
        df.to_csv(f"data/{cur_time}/grp_transform_{year}.csv", index=False)


@asset(deps=[transform_grp_data])
def load_grp_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time, year = info['cur_time'], info['year']
        f.close()
    df = pd.read_csv(f"data/{cur_time}/grp_transform_{year}.csv", )
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    count = df.to_sql(name='grp', con=engine, index=False, if_exists='replace')
    context.log.info(f"!!!!!!!!!!!!!! Inserted {count} GRP rows")
    df.to_csv(f"data/{cur_time}/csv_tables/grp.csv", index=False)


@asset(deps=[init])
def extract_hdi_html(context: AssetExecutionContext) -> None:
    headers = {'Accept-Encoding': 'utf8'}
    html = requests.get(HDI_URL, headers=headers).text
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    soup = BeautifulSoup(html, "html.parser")
    hdi_22_23 = soup.find(attrs={'id': 'By_Statistics_Indonesia_in_2023'}).find_next('table')
    with open(f"data/{cur_time}/hdi_extract_2022_2023.htm", "w") as f:
        f.write(str(hdi_22_23))
        f.close()
    hdi_10_21 = soup.find(attrs={'id': 'Trends_by_Statistics_Indonesia'}).find_next('table')
    with open(f"data/{cur_time}/hdi_extract_2010_2021.htm", "w") as f:
        f.write(str(hdi_10_21))
        f.close()


@asset(deps=[extract_hdi_html])
def transform_hdi_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    full_df = pd.DataFrame()
    with open(f"data/{cur_time}/hdi_extract_2010_2021.htm", "r") as f:
        html_hdi = f.read()
        f.close()
        df = pd.read_html(io.StringIO(html_hdi), index_col=None, header=0)[0]
        df = df[df['Province'] != df['HDI 2010']]
        df = df[df['Province'] != 'Indonesia']
        df.rename(columns=lambda col: re.sub('(HDI )', '', col), inplace=True)
        df.replace('Part of East Kalimantan', 0, inplace=True)
        full_df = df

    with (open(f"data/{cur_time}/hdi_extract_2022_2023.htm", "r") as f):
        html_hdi = f.read()
        f.close()
        df = pd.read_html(io.StringIO(html_hdi), index_col=None, header=0)[0]
        df = df[df['Rank'].str.isdigit()]
        df = df[df['Rank'] != '2023']
        df.drop(axis='columns', columns=['Rank', 'Rank.1'], inplace=True)
        df.rename(columns={'HDI.1': '2022', 'HDI': '2023'}, inplace=True)
        df['Province'] = df['Province'].apply(lambda p: re.sub(r'(\[[a-z]+])', '', p))
        df = df.astype({'2022': 'double', '2023': 'double'})
        full_df['2022'] = 0.0
        full_df['2023'] = 0.0
        df.set_index('Province', inplace=True)
        full_df.set_index('Province', drop=False, inplace=True)
        for prv in full_df.index:
            full_df.loc[prv, '2022'] = str.ljust(str(df.loc[prv, '2023'].astype(float) - df.loc[prv, '2022'].astype(float)), 6, '0')[:6]
            full_df.loc[prv, '2023'] = str.ljust(str(df.loc[prv, '2023']), 6, '0')[:6]

        full_df.loc["Southwest Papua"] = [full_df.loc["West Papua", col] for col in full_df.columns]
        full_df.loc["Southwest Papua", "Province"] = "Southwest Papua"
        for other_papua in ["South Papua", "Central Papua", "Highland Papua"]:
            full_df.loc[other_papua] = [full_df.loc["Papua", col] for col in full_df.columns]
            full_df.loc[other_papua, "Province"] = other_papua

        full_df.sort_index(inplace=True)
        ids = range(1, full_df.count()["Province"] + 1)
        full_df.insert(loc=0, column="Id", value=ids)
        full_df.set_index("Id", drop=False, inplace=True)
        full_df.to_csv(f"data/{cur_time}/hdi_transform_2010_2023.csv", index=False)

        summary_years = [year for year in full_df.columns if year not in ["Id", "Province"]]
        summary_averages = [full_df[year].astype(float).mean().round(4) for year in summary_years]
        full_df = pd.DataFrame(data=[summary_years, summary_averages], index=None, columns=None)
        full_df.T.to_csv(f"data/{cur_time}/hdi_transform_2010_2023_summary.csv", header=["Year", "Average"], index=False)


@asset(deps=[transform_hdi_data])
def load_hdi_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df = pd.read_csv(f"data/{cur_time}/hdi_transform_2010_2023.csv", )
    count = df.to_sql(name='hdi', con=engine, index=False, if_exists='replace')
    context.log.info(f"!!!!!!!!!!!!!! Inserted {count} HDI rows")
    df.to_csv(f"data/{cur_time}/csv_tables/hdi.csv", index=False)

    df = pd.read_csv(f"data/{cur_time}/hdi_transform_2010_2023_summary.csv", )
    df.to_sql(name='hdi_year_summary', con=engine, index=False, if_exists='replace')
    df.to_csv(f"data/{cur_time}/csv_tables/hdi_year_summary.csv", index=False)


# @asset(deps=[extract_hdi_html])
def transform_hdi_data_transpose(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    full_df = pd.DataFrame()
    with open(f"data/{cur_time}/hdi_extract_2010_2021.htm", "r") as f:
        html_hdi = f.read()
        f.close()
        df = pd.read_html(io.StringIO(html_hdi), index_col=None, header=0)[0]
        df = df[df['Province'] != df['HDI 2010']]
        df = df[df['Province'] != 'Indonesia']
        df.rename(columns=lambda col: re.sub('(HDI )', '', col), inplace=True)
        df.replace('Part of East Kalimantan', 0, inplace=True)
        full_df = df

    with (open(f"data/{cur_time}/hdi_extract_2022_2023.htm", "r") as f):
        html_hdi = f.read()
        f.close()
        df = pd.read_html(io.StringIO(html_hdi), index_col=None, header=0)[0]
        df = df[df['Rank'].str.isdigit()]
        df = df[df['Rank'] != '2023']
        df.drop(axis='columns', columns=['Rank', 'Rank.1'], inplace=True)
        df.rename(columns={'HDI.1': '2022', 'HDI': '2023'}, inplace=True)
        df['Province'] = df['Province'].apply(lambda p: re.sub(r'(\[[a-z]+])', '', p))
        df = df.astype({'2022': 'double', '2023': 'double'})
        full_df['2022'] = 0.0
        full_df['2023'] = 0.0
        df.set_index('Province', inplace=True)
        full_df.rename(columns={'Province': 'Year'}, inplace=True)
        full_df.set_index('Year', drop=False, inplace=True)
        for prv in full_df.index:
            full_df.loc[prv, '2022'] = str.ljust(str(df.loc[prv, '2023'].astype(float) - df.loc[prv, '2022'].astype(float)), 6, '0')[:6]
            full_df.loc[prv, '2023'] = str.ljust(str(df.loc[prv, '2023']), 6, '0')[:6]
        full_df.sort_index(inplace=True)
        full_df = np.concatenate(([full_df.columns.to_numpy()], full_df.to_numpy()), axis=0)
        full_df = full_df.transpose()
        full_df = np.insert(full_df, 0, values=["Id", *range(1, full_df.shape[0])], axis=1)
        pd.DataFrame(full_df).to_csv(f"data/{cur_time}/hdi_transform_transpose_2010_2023.csv", header=False, index=False)


# @asset(deps=[transform_hdi_data_transpose])
def load_hdi_data_transpose(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    df = pd.read_csv(f"data/{cur_time}/hdi_transform_transpose_2010_2023.csv", )
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    count = df.to_sql(name='indo_hdi_transpose', con=engine, index=False, if_exists='replace')
    context.log.info(f"!!!!!!!!!!!!!! Inserted {count} HDI transpose rows")
