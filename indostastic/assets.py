import datetime
import json
import os
import io
import re
import requests
from dagster import asset, AssetExecutionContext
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
from constants import *


@asset
def init():
    cur_time = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
    os.makedirs(f"data/{cur_time}", exist_ok=True)
    with open("data/info.txt", "w") as f:
        f.write(str({"cur_time": cur_time}))
        f.close()


@asset(deps=[init])
def extract_table_gdp(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    headers = {'Accept-Encoding': 'utf8'}
    html = requests.get(GDP_URL, headers=headers).text
    year = re.findall(r'id="([0-9]{4})_data"', html, re.IGNORECASE)[0]
    soup = BeautifulSoup(html, "html.parser")
    table_gdp = soup.find_all('table', {'class': 'wikitable'})[0]
    with open(f"data/{cur_time}/gdp_extract_{year}.htm", "w") as f:
        f.write(str(table_gdp))
        f.close()
    with open("data/info.txt", "w") as f:
        f.write(str({"cur_time": cur_time, "year": year}))
        f.close()


@asset(deps=[extract_table_gdp])
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
            'GDP[8] (in billion Rp)': 'GDP Nominal (billion RP)',
            'GDP Nominal': 'GDP Nominal (billion USD)',
        })
        df.set_index('Province', inplace=True)
        df.sort_index(inplace=True)
        df.to_csv(f"data/{cur_time}/gdp_transform_{year}.csv", index=True)


@asset(deps=[transform_gdp_data])
def load_gdp_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time, year = info['cur_time'], info['year']
        f.close()
    df = pd.read_csv(f"data/{cur_time}/gdp_transform_{year}.csv", )
    engine = create_engine("postgresql://postgres:123456@localhost:5432/indo_stastic")
    count = df.to_sql(name='indo_gdp', con=engine, index=True, if_exists='replace')
    context.log.info(f"!!!!!!!!!!!!!! Inserted {count} GDP rows")


@asset(deps=[init])
def extract_table_grp() -> None:
    headers = {'Accept-Encoding': 'utf8'}
    html = requests.get(GRP_URL, headers=headers).text
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    year = re.findall(r'id="([0-9]{4})_Per_Capita"', html, re.IGNORECASE)[0]
    soup = BeautifulSoup(html, "html.parser")
    table_grp = soup.find_all('table', {'class': 'wikitable'})[0]
    with open(f"data/{cur_time}/grp_extract_{year}.htm", "w") as f:
        f.write(str(table_grp))
        f.close()
    with open("data/info.txt", "w") as f:
        f.write(str({"cur_time": cur_time, "year": year}))
        f.close()


@asset(deps=[extract_table_grp])
def transform_grp_data() -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time, year = info['cur_time'], info['year']
        f.close()
    with open(f"data/{cur_time}/grp_extract_{year}.htm", "r") as f:
        html_grp = f.read()
        f.close()
        df = pd.read_html(io.StringIO(html_grp), index_col=None, header=0)[0]
        df = df[df['Rank'].str.isdigit()]
        df.drop(inplace=True, columns=['Rank', 'Per capita PPP'])
        df.rename(inplace=True, columns={
            'Per capita[9] (in thousand Rp)': 'Per capita (thousand RP)',
            'Per capita Nominal': 'Per capita Nominal (USD)',
        })
        df.set_index('Province', inplace=True)
        df.sort_index(inplace=True)
        df.to_csv(f"data/{cur_time}/grp_transform_{year}.csv", index=True)


@asset(deps=[transform_grp_data])
def load_grp_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time, year = info['cur_time'], info['year']
        f.close()
    df = pd.read_csv(f"data/{cur_time}/grp_transform_{year}.csv", )
    engine = create_engine("postgresql://postgres:123456@localhost:5432/indo_stastic")
    count = df.to_sql(name='indo_grp', con=engine, index=True, if_exists='replace')
    context.log.info(f"!!!!!!!!!!!!!! Inserted {count} GRP rows")


@asset(deps=[init])
def extract_table_hdi(context: AssetExecutionContext) -> None:
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


@asset(deps=[extract_table_hdi])
def transform_hdi_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    combine_hdi = pd.DataFrame()
    with open(f"data/{cur_time}/hdi_extract_2010_2021.htm", "r") as f:
        html_hdi = f.read()
        f.close()
        df = pd.read_html(io.StringIO(html_hdi), index_col=None, header=0)[0]
        df = df[df['Province'] != df['HDI 2010']]
        df = df[df['Province'] != 'Indonesia']
        df.rename(columns=lambda col: re.sub('(HDI )', '', col), inplace=True)
        df.replace('Part of East Kalimantan', 0, inplace=True)
        combine_hdi = df

    with open(f"data/{cur_time}/hdi_extract_2022_2023.htm", "r") as f:
        html_hdi = f.read()
        f.close()
        df = pd.read_html(io.StringIO(html_hdi), index_col=None, header=0)[0]
        df = df[df['Rank'].str.isdigit()]
        df = df[df['Rank'] != '2023']
        df.drop(axis='columns', columns=['Rank', 'Rank.1'], inplace=True)
        df.rename(columns={'HDI.1': '2022', 'HDI': '2023'}, inplace=True)
        df['Province'] = df['Province'].apply(lambda p: re.sub(r'(\[[a-z]+])', '', p))
        df = df.astype({'2022': 'double', '2023': 'double'})
        combine_hdi['2022'] = 0.0
        combine_hdi['2023'] = 0.0
        df.set_index('Province', inplace=True)
        combine_hdi.set_index('Province', inplace=True)
        for prv in combine_hdi.index:
            combine_hdi.loc[prv, '2022'] = str.ljust(str(df.loc[prv, '2023'].astype(float) - df.loc[prv, '2022'].astype(float)), 6, '0')[:6]
            combine_hdi.loc[prv, '2023'] = str.ljust(str(df.loc[prv, '2023']), 6, '0')[:6]
        combine_hdi.sort_index(inplace=True)
        combine_hdi.reset_index(inplace=True)
        combine_hdi.rename(columns={'Province': 'Year_Province'}, inplace=True)
        combine_hdi.T.to_csv(f"data/{cur_time}/hdi_transform_2010_2023.csv", header=None, index=True)


@asset(deps=[transform_hdi_data])
def load_hdi_data(context: AssetExecutionContext) -> None:
    with open("data/info.txt", "r") as f:
        info = json.JSONDecoder().decode(f.read().replace("'", "\""))
        cur_time = info['cur_time']
        f.close()
    df = pd.read_csv(f"data/{cur_time}/hdi_transform_2010_2023.csv", )
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    count = df.to_sql(name='indo_hdi', con=engine, index=True, if_exists='replace')
    context.log.info(f"!!!!!!!!!!!!!! Inserted {count} HDI rows")
