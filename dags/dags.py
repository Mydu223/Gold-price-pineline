from datetime import datetime, timedelta
from airflow import DAG
import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, Float, TIMESTAMP
import json

# 1 Extract data
def extract_gold_prices(**context):
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0'}
    
    url = 'https://giavang.org/'
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    try:
        tables = soup.findAll('table', class_ = 'table table-bordered table-hover table-striped text-right')
        tael_price = []
        chi_price = []
        current_region = None
        region_id = 1
        regions=[]
        region_id_map = {}
        
        for table_id,table in enumerate(tables):
            rows = table.findAll('tr')
            time = table.find('i').text.strip().split(' ')[3:]
            for row_id,row in enumerate(rows):
                if row_id == 0 or row_id == len(rows) - 1:
                    continue
                cols = row.findAll(['th', 'td'])
                if cols:
                    if cols[0].name == 'th':
                        current_region = cols[0].text.strip()
                        if current_region not in region_id_map:
                            regions.append({'region_id': region_id, 'region': current_region})
                            region_id_map[current_region] = region_id
                            region_id += 1
                        sys, buy_price, sell_price = [ele.text.strip() for ele in cols[1:]]
                    else:   
                        if current_region is None:
                            continue  
                        sys, buy_price, sell_price = [ele.text.strip() for ele in cols]
                    if table_id == 0:
                        tael_price.append({
                            'region': region_id_map[current_region],
                            'sys': sys,
                            'buy_price': buy_price,
                            'sell_price': sell_price,
                            'time': ' '.join(time)[:19] 
                        })
                    else:
                        chi_price.append({
                            'region': region_id_map[current_region],
                            'sys': sys,
                            'buy_price': buy_price,
                            'sell_price': sell_price,
                            'time': ' '.join(time)[:19] 
                        })
        
        context['task_instance'].xcom_push(key='regions_data', value=regions)
        context['task_instance'].xcom_push(key='tael_price_data', value=tael_price)
        context['task_instance'].xcom_push(key='chi_price_data', value=chi_price)
        
    except Exception as e:
        print(f"Error extracting data: {e}")
        raise  
        
# 2) Clean data (Transform)
def transform_gold_data(**context):
    regions_data = context['task_instance'].xcom_pull(task_ids='fetch_gold_data', key='regions_data')
    tael_price_data = context['task_instance'].xcom_pull(task_ids='fetch_gold_data', key='tael_price_data')
    chi_price_data = context['task_instance'].xcom_pull(task_ids='fetch_gold_data', key='chi_price_data')
    
    regions_df = pd.DataFrame(regions_data)
    tael_price_df = pd.DataFrame(tael_price_data)
    chi_price_df = pd.DataFrame(chi_price_data)
    
    tael_price_df['time'] = pd.to_datetime(tael_price_df['time'], format='%H:%M:%S %d/%m/%Y')
    chi_price_df['time'] = pd.to_datetime(chi_price_df['time'], format='%H:%M:%S %d/%m/%Y')
    
    tael_price_df['buy_price'] = pd.to_numeric(tael_price_df['buy_price'], errors='coerce')
    tael_price_df['sell_price'] = pd.to_numeric(tael_price_df['sell_price'], errors='coerce')
    chi_price_df['buy_price'] = pd.to_numeric(chi_price_df['buy_price'], errors='coerce')
    chi_price_df['sell_price'] = pd.to_numeric(chi_price_df['sell_price'], errors='coerce')
    
    tael_price_df['time'] = tael_price_df['time'].astype(str)
    chi_price_df['time'] = chi_price_df['time'].astype(str)
    
    context['task_instance'].xcom_push(key='transformed_regions_df', value=regions_df.to_dict('records'))
    context['task_instance'].xcom_push(key='transformed_tael_price_df', value=tael_price_df.to_dict('records'))
    context['task_instance'].xcom_push(key='transformed_chi_price_df', value=chi_price_df.to_dict('records'))
    
    print(f"Transformed data: {len(regions_df)} regions, {len(tael_price_df)} tael prices, {len(chi_price_df)} chi prices")

def upsert_regions_do_nothing(df, engine):
    if df.empty:
        return
    insert_query = """
        INSERT INTO regions (region_id, region)
        VALUES (%s, %s)
        ON CONFLICT (region_id) DO NOTHING
    """
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(insert_query, (int(row['region_id']), str(row['region'])))

# 3) Store data in Postgres (Load)
def load_gold_data(**context):
    regions_records = context['task_instance'].xcom_pull(task_ids='transform_gold_data', key='transformed_regions_df')
    tael_price_records = context['task_instance'].xcom_pull(task_ids='transform_gold_data', key='transformed_tael_price_df')
    chi_price_records = context['task_instance'].xcom_pull(task_ids='transform_gold_data', key='transformed_chi_price_df')
    
    transformed_regions_df = pd.DataFrame(regions_records)
    transformed_tael_price_df = pd.DataFrame(tael_price_records)
    transformed_chi_price_df = pd.DataFrame(chi_price_records)
    
    if not transformed_tael_price_df.empty:
        transformed_tael_price_df['region'] = transformed_tael_price_df['region'].astype(int)
        transformed_tael_price_df['buy_price'] = pd.to_numeric(transformed_tael_price_df['buy_price'], errors='coerce')
        transformed_tael_price_df['sell_price'] = pd.to_numeric(transformed_tael_price_df['sell_price'], errors='coerce')
        transformed_tael_price_df['time'] = pd.to_datetime(transformed_tael_price_df['time'], errors='coerce')

    if not transformed_chi_price_df.empty:
        transformed_chi_price_df['region'] = transformed_chi_price_df['region'].astype(int)
        transformed_chi_price_df['buy_price'] = pd.to_numeric(transformed_chi_price_df['buy_price'], errors='coerce')
        transformed_chi_price_df['sell_price'] = pd.to_numeric(transformed_chi_price_df['sell_price'], errors='coerce')
        transformed_chi_price_df['time'] = pd.to_datetime(transformed_chi_price_df['time'], errors='coerce')
    
    if not transformed_regions_df.empty:
        transformed_regions_df['region_id'] = transformed_regions_df['region_id'].astype(int)
        transformed_regions_df['region'] = transformed_regions_df['region'].astype(str)
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id='gold_price_connection')
        engine = create_engine(postgres_hook.get_uri())
        
        upsert_regions_do_nothing(transformed_regions_df, engine)
        
        transformed_tael_price_df.to_sql('tael_price', engine, if_exists='append', index=False, dtype={
            'region': Integer(),
            'sys': Text(),
            'buy_price': Float(),
            'sell_price': Float(),
            'time': TIMESTAMP()
        })
        
        transformed_chi_price_df.to_sql('chi_price', engine, if_exists='append', index=False, dtype={
            'region': Integer(),
            'sys': Text(),
            'buy_price': Float(),
            'sell_price': Float(),
            'time': TIMESTAMP()
        })
        
        print(f"Successfully loaded data to database")
        
    except Exception as e:
        print(f"Error loading data to database: {e}")
        raise  

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 27),  
    'retry_delay': timedelta(minutes=5),
    'retries': 1,
}

dag = DAG(
    'fetch_and_store_gold_prices',
    default_args=default_args,
    description='None',
    schedule='0 8,11,14,17,21,23 * * *',
    catchup=False,
)

fetch_gold_data_task = PythonOperator(
    task_id='fetch_gold_data',
    python_callable=extract_gold_prices,
    dag=dag,
)

transform_gold_data_task = PythonOperator(
    task_id='transform_gold_data',
    python_callable=transform_gold_data,
    dag=dag,
)

load_gold_data_task = PythonOperator(
    task_id='load_gold_data',
    python_callable=load_gold_data,
    dag=dag,
)

fetch_gold_data_task >> transform_gold_data_task >> load_gold_data_task