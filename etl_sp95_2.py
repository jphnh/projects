from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import json
import requests
import zipfile
import io
import xml.etree.ElementTree as ET

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
    start_date=datetime(2024, 5, 13),
    schedule_interval="0 3 * * *"
)

def extract_data(**kwargs):
    url = 'https://donnees.roulez-eco.fr/opendata/jour'
    response = requests.get(url)
    
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        xml_filename = z.namelist()[0]
        with z.open(xml_filename) as xml_file:
            tree = ET.parse(xml_file)
            root = tree.getroot()
    
    data = []
    for pdv in root.findall('pdv'):
        entry = {}
        entry['id'] = pdv.get('id')
        entry['latitude'] = pdv.get('latitude')
        entry['longitude'] = pdv.get('longitude')
        entry['cp'] = pdv.get('cp')
        entry['pop'] = pdv.get('pop')
        entry['adresse'] = pdv.find('adresse').text if pdv.find('adresse') is not None else None
        entry['ville'] = pdv.find('ville').text if pdv.find('ville') is not None else None
        
        # Récupérer les horaires
        horaires = {}
        horaires_elem = pdv.find('horaires')
        if horaires_elem is not None:
            for jour in horaires_elem:
                horaires[jour.get('nom')] = jour.get('ferme')
        entry['horaires'] = horaires
        
        # Récupérer les services
        services_elem = pdv.find('services')
        services = [service.text for service in services_elem] if services_elem is not None else []
        entry['services'] = services
        
        # Récupérer les prix
        prix = {prix.get('nom'): prix.get('valeur') for prix in pdv.findall('prix')}
        entry['prix'] = prix
        
        # Récupérer les ruptures
        ruptures = {rupture.get('nom'): rupture.get('debut') for rupture in pdv.findall('rupture')}
        entry['ruptures'] = ruptures
        
        data.append(entry)

    # Convertir les données en JSON
    json_data = json.dumps(data)
    
    return json_data

def transform_data(**kwargs):
    raw_json_data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    
    # Convertir la chaîne JSON en DataFrame
    df = pd.read_json(raw_json_data)
    
    # Nettoyer les données
    df = df.applymap(lambda x: '' if x is None else x)

    df = df.rename(columns={'cp':'code_postal'})

    # Ajouter le '0' devant les codes postaux à 4 chiffres et convertir en chaînes de caractères
    df['code_postal'] = df['code_postal'].apply(lambda x: str(x).zfill(5))

    # Créer la colonne 'code_departement'
    df['code_departement'] = df['code_postal'].apply(lambda x: x[:2])

    # Convertir les colonnes longitude et latitude en float et diviser par 100000
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce') / 100000
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce') / 100000

    # Séparer les prix en différentes colonnes

    prix_df = pd.json_normalize(df['prix'])
    prix_df.columns = ['prix_' + col.lower() for col in prix_df.columns]

    df = pd.concat([df, prix_df], axis=1)

    ruptures_df = pd.json_normalize(df['ruptures'])
    ruptures_df.columns = ['ruptures_' + col.lower() for col in ruptures_df.columns]

    df = pd.concat([df, ruptures_df], axis=1)

    df = df.drop(columns=['pop', 'services', 'horaires', 'prix', 'ruptures'])
    df = df.rename(columns={'prix_E10':'prix_e10', 'prix_E85':'prix_e85', 'prix_GPLc':'prix_gplc','prix_Gazole':'prix_gazole','prix_SP95':'prix_sp95','prix_SP98':'prix_sp98',
        'ruptures_E10':'ruptures_e10','ruptures_GPLc':'ruptures_gplc','ruptures_Gazole':'ruptures_gazole','ruptures_SP95':'ruptures_sp95','ruptures_SP98':'ruptures_sp98'})
    
    df['prix_e10'] = pd.to_numeric(df['prix_e10'])
    df['prix_e85'] = pd.to_numeric(df['prix_e85'])
    df['prix_gplc'] = pd.to_numeric(df['prix_gplc'])
    df['prix_gazole'] = pd.to_numeric(df['prix_gazole'])
    df['prix_sp95'] = pd.to_numeric(df['prix_sp95'])
    df['prix_sp98'] = pd.to_numeric(df['prix_sp98'])

    df['ruptures_e10'] = pd.to_datetime(df['ruptures_e10'], format='ISO8601', errors='coerce')
    df['ruptures_e85'] = pd.to_datetime(df['ruptures_e85'], format='ISO8601', errors='coerce')
    df['ruptures_gplc'] = pd.to_datetime(df['ruptures_gplc'], format='ISO8601', errors='coerce')
    df['ruptures_gazole'] = pd.to_datetime(df['ruptures_gazole'], format='ISO8601', errors='coerce')
    df['ruptures_sp95'] = pd.to_datetime(df['ruptures_sp95'], format='ISO8601', errors='coerce')
    df['ruptures_sp98'] = pd.to_datetime(df['ruptures_sp98'], format='ISO8601', errors='coerce')

    return df

def load_to_bigquery(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='transform_data')

    credentials = service_account.Credentials.from_service_account_file('Documents/Clés API/Airflow/lofty-generator-398311-36986f298381.json')
    client = bigquery.Client(credentials=credentials, project='lofty-generator-398311')
    project_id = 'lofty-generator-398311'
    dataset_id = 'fr_carburant'
    table_id = 'lofty-generator-398311.fr_carburant.fr_carburant_airflow'

    table_ref = f"{project_id}.{dataset_id}.fr_carburant_airflow"
    
    schema = [
        bigquery.SchemaField('id', bigquery.enums.SqlTypeNames.INTEGER),
        bigquery.SchemaField('latitude', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('longitude', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('code_postal', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('adresse', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('ville', bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField('prix_e10', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('prix_e85', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('prix_gplc', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('prix_gazole', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('prix_sp95', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('prix_sp98', bigquery.enums.SqlTypeNames.FLOAT),
        bigquery.SchemaField('ruptures_e10', bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField('ruptures_e85', bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField('ruptures_gplc', bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField('ruptures_gazole', bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField('ruptures_sp95', bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField('ruptures_sp98', bigquery.enums.SqlTypeNames.DATETIME),
        bigquery.SchemaField('code_departement', bigquery.enums.SqlTypeNames.STRING)
    ]

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Pour écraser les données existantes
        schema=schema
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
