from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv
import os

# Importar las funciones necesarias
from config import REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DBNAME, REDSHIFT_USER, YEARS
from utils import setup_logging, load_credentials
from spotify import authenticate_spotify, get_tracks_data, create_dataframe
from redshift import connect_redshift, create_table, insert_data, drop_table  # truncate_table

# Cargar las variables del archivo .env
load_dotenv()

def setup_logging_task():
    setup_logging()

def load_credentials_task():
    client_secret = os.getenv("SPOTIFY_CLIENT_ID")
    client_id = os.getenv("SPOTIFY_SECRET_FILE")
    password = os.getenv("REDSHIFT_PASSWORD_FILE")
    return client_secret, client_id, password

def run_spotify_to_redshift_task(client_secret, client_id, password):
    # Autenticación con Spotify
    sp = authenticate_spotify(client_secret, client_id)
    
    # Conectar a Redshift
    conn = connect_redshift(REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DBNAME, REDSHIFT_USER, password)
    
    # Crear un cursor
    cur = conn.cursor()
    
    # dropear y Crear la tabla si no existe y truncarla
    drop_table(cur)
    create_table(cur)
    # truncate_table(cur)
    
    # Obtener datos de pistas de Spotify
    data = get_tracks_data(sp, YEARS)
    
    # Crear DataFrame y limpiar datos
    df = create_dataframe(data)
    
    # Insertar datos en Redshift
    insert_data(cur, df)
    
    # Cerrar el cursor y la conexión
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    'spotify_to_redshift_dag',
    default_args=default_args,
    description='A DAG to transfer Spotify data to Redshift',
    schedule_interval=None,  # Puedes ajustar esto según tus necesidades
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    setup_logging_task = PythonOperator(
        task_id='setup_logging',
        python_callable=setup_logging_task,
    )
    
    load_credentials_task = PythonOperator(
        task_id='load_credentials',
        python_callable=load_credentials_task,
    )
    
    run_spotify_to_redshift_task = PythonOperator(
        task_id='run_spotify_to_redshift',
        python_callable=run_spotify_to_redshift_task,
        op_kwargs={
            'client_secret': '{{ task_instance.xcom_pull(task_ids="load_credentials")[0] }}',
            'client_id': '{{ task_instance.xcom_pull(task_ids="load_credentials")[1] }}',
            'password': '{{ task_instance.xcom_pull(task_ids="load_credentials")[2] }}',
        }
    )

    setup_logging_task >> load_credentials_task >> run_spotify_to_redshift_task

