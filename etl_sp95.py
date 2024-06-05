from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "email": ["jp.huynh@audencia.com"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'trigger_rule': 'all_success'
}

dag = DAG(
    'etl_sp95',
    default_args=default_args,
    description='ETL to BigQuery gas prices in France',
    start_date=datetime(2024, 5, 28),
    schedule_interval="0 3 * * *",
    catchup=False
)

def extract_data(**kwargs):
    url = 'https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-des-carburants-en-france-flux-instantane-v2/exports/json?lang=fr&timezone=Europe%2FParis'
    response = requests.get(url)
    data = response.json()
    
    return data

def transform_data(**kwargs):
    raw_json_data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    
    # Convertir la chaîne JSON en DataFrame
    df = pd.DataFrame(raw_json_data)

    df = df.rename(columns={'cp':'code_postal'})

    # Ajouter un zéro en tête des codes postaux de 4 chiffres
    df['code_postal'] = df['code_postal'].astype(str).str.zfill(5)

    # Diviser les colonnes longitude et latitude par 100000
    df['longitude'] = df['longitude'].astype(float) / 100000
    df['latitude'] = df['latitude'].astype(float) / 100000

    # Supprimer les colonnes inutiles
    columns_to_drop = ['geom','pop', 'horaires', 'services', 'prix', 'rupture', 'horaires_automate_24_24', 
    'services_service', 'code_region', 'horaires_jour', 'e10_rupture_debut', 'e10_rupture_type', 'sp98_rupture_debut',
    'sp98_rupture_type', 'sp95_rupture_debut', 'sp95_rupture_type','e85_rupture_debut', 'e85_rupture_type', 
    'gplc_rupture_debut', 'gplc_rupture_type', 'gazole_rupture_debut', 'gazole_rupture_type', 'carburants_disponibles', 
    'carburants_indisponibles']
    df = df.drop(columns=columns_to_drop)

    return df

def load_to_bigquery(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='transform_data')

    credentials = service_account.Credentials.from_service_account_file('Documents/Clés API/Airflow/lofty-generator-398311-36986f298381.json')
    client = bigquery.Client(credentials=credentials, project='lofty-generator-398311')
    project_id = 'lofty-generator-398311'
    dataset_id = 'fr_carburant'
    table_id = 'lofty-generator-398311.fr_carburant.fr_carburant'

    table_ref = f"{project_id}.{dataset_id}.fr_carburant"
    

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Pour écraser les données existantes
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Attendre la fin du chargement
    
    print(f"Loaded {len(df)} rows into {table_id}.")

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_to_bigquery = PythonOperator(
    task_id='load_to_bigquery',
    python_callable=load_to_bigquery,
    dag=dag,
)

extract_data >> transform_data >> load_to_bigquery
