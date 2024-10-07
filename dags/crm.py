import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import timedelta, datetime
import pandas as pd
import io
import psycopg2

# Define paths constants:
BASE_FILE = '/home/elieba/airflow/files/CRM_20240917.csv'
SUBSCRIBER_FILE = '/home/elieba/airflow/files/subscribers.csv'
SERVICES_FILE = '/home/elieba/airflow/files/services.csv'
CLEANED_FILE = '/home/elieba/airflow/files/clean_data.csv'

# Define default arguments for the DAG
default_args = {
    'owner': 'elieba',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(seconds=30),
}

# Initialize DAG
dag = DAG(
    'crm_dag',
    default_args=default_args,
    description='A CRM Loading Flow',
    schedule=timedelta(days=1),
    catchup=False,
)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname='RAFM_DAT',
    user='postgres',
    password='13058',
    host='localhost',  # or your server address
    port='5432'  # default PostgreSQL port
)

# Function to read subscribers records
def read_subscribers_records(**kwargs):
    crm_subscribers = (row for row in open(BASE_FILE) if 'CRM_Subscriber' in row)
    cols = ['MSISDN', 'IMSI', 'SUBS_STATUS', 'SUBS_TYPE', 'RECORD_TYPE']
    filtered_data = ''.join(crm_subscribers)
    subscribers = pd.read_csv(io.StringIO(filtered_data), sep='|', names=cols)
    
    # pushing the data of subscribers df to the transform task using Xcom:
    ti = kwargs['ti']
    subscribers_dict = subscribers.to_dict(orient='records')
    subscribers_json = json.dumps(subscribers_dict)
    ti.xcom_push(key='subscribers_json', value=subscribers_json)
    # Save the subscribers DataFrame to CSV for future use in transformation
    # subscribers.to_csv(SUBSCRIBER_FILE, index=False)

# Function to read services records
def read_services_records(**kwargs):
    crm_services = (row for row in open(BASE_FILE) if 'CRM_SERVICES' in row)
    filtered_data = ''.join(crm_services)
    names = ['MSISDN', 'PRICE_PLAN_NAME', 'PROD_SPEC_NAME', 'SUBS_STATUS', 'SERVICE_ID', 'RECORD_TYPE']
    services = pd.read_csv(io.StringIO(filtered_data), sep='|', names=names)
    
    # pushing the data of services df to the transform task using Xcom:
    ti = kwargs['ti']
    services_dict = services.to_dict(orient='records')
    services_json = json.dumps(services_dict)
    ti.xcom_push(key='services_json', value=services_json)
    # Save the services DataFrame to CSV for future use in transformation
    # services.to_csv(SERVICES_FILE, index=False)


def format_services(services):
    if isinstance(services, list):  # Check if services is a list
        return '{' + ', '.join(map(str, services)) + '}'
    return services  # Return as is if not a list


# Function to transform data
def transform_data(**kwargs):
    # ## Read subscribers and services from CSV
    # subscribers = pd.read_csv(SUBSCRIBER_FILE)
    # services = pd.read_csv(SERVICES_FILE)

    # pulling the data of subscribers df from read_subscribers task using Xcom:
    ti = kwargs['ti']
    subscribers_json = ti.xcom_pull(task_ids='read_subscribers', key='subscribers_json')
    subscribers_dict = json.loads(subscribers_json)
    subscribers = pd.DataFrame(subscribers_dict)

    # pulling the data of services df from read_services task using Xcom:
    services_json = ti.xcom_pull(task_ids='read_services', key='services_json')
    services_dict = json.loads(services_json)
    services = pd.DataFrame(services_dict)

    # Pulling lookups df from load_lookups task using Xcom:
    
    status_json = ti.xcom_pull(task_ids= 'load_lookups', key='status_json')
    status_dict = json.loads(status_json)
    subs_status = pd.DataFrame(status_dict)

    types_json = ti.xcom_pull(task_ids = 'load_lookups', key='types_json')
    types_dict = json.loads(types_json)
    subs_types = pd.DataFrame(types_dict)

    services_lookup_json = ti.xcom_pull(task_ids = 'load_lookups', key='services_lookup_json')
    services_lookup_dict = json.loads(services_lookup_json)
    services_lookup = pd.DataFrame(services_lookup_dict)
    
    packages_json = ti.xcom_pull(task_ids = 'load_lookups', key='packages_json')
    packages_dict = json.loads(packages_json)
    packages = pd.DataFrame(packages_dict)

    est_rev_json = ti.xcom_pull(task_ids = 'load_lookups', key='rev_json')
    est_rev_dict = json.loads(est_rev_json)
    estimated_revenue = pd.DataFrame(est_rev_dict)

    # Merge and transform
    merge_df = pd.merge(subscribers, services, on='MSISDN', how='inner')
    merge_df = pd.merge(merge_df, subs_types, left_on='SUBS_TYPE', right_on='subs_type_code', how='left')
    print(f"df columns are:------------------: {merge_df.columns}")
    merge_df = pd.merge(merge_df, subs_status, left_on='SUBS_STATUS_x', right_on='subs_status_code', how='left')
    print(f"from merge_df:-------:{merge_df[['SUBS_STATUS_x', 'subs_status_id']].head()}")
    print(f"from subs_status:---------------:{subs_status[['subs_status_code', 'subs_status_id']]}")

   
    service_df = merge_df.groupby('MSISDN').agg(
        IMSI=('IMSI', 'first'),
        PRICE_PLAN_NAME=('PRICE_PLAN_NAME', 'first'),
        PROD_SPEC_NAME=('PROD_SPEC_NAME', 'first'),
        SERVICES=('SERVICE_ID', list),
        SUBS_TYPE_ID=('subs_type_id', 'first'),
        SUBS_STATUS_ID=('subs_status_id', 'first') # Aggregate SERVICE_IDs into a list
    ).reset_index()

    service_df['SERVICES'] = service_df['SERVICES'].apply(format_services)

    print(f"services_df:\n {service_df.head()}")
    # Save the transformed DataFrame to CSV for future use
    print(merge_df.head())
    service_df.to_csv(CLEANED_FILE, index=False)

def load_lookups(**kwargs):
    status_query = 'select * from rd_r_crm_subs_status;'
    type_query = 'select * from rd_r_crm_subs_type;'
    services_query = 'select * from rd_r_crm_srv;'
    estimated_rev_query = 'select * from rd_r_estimated_revenue;'
    subs_packages_query = 'select * from rd_r_subs_packages;'

    subs_status = pd.read_sql_query(status_query,conn)
    subs_type =  pd.read_sql_query(type_query, conn)
    services_lookup = pd.read_sql_query(services_query, conn)
    estimated_rev = pd.read_sql_query(estimated_rev_query, conn)
    subs_packages = pd.read_sql_query(subs_packages_query,conn)

    conn.close()

    ti = kwargs['ti']
    status_dict = subs_status.to_dict(orient='records')
    status_json = json.dumps(status_dict)
    ti.xcom_push(key='status_json', value=status_json)

    types_dict = subs_type.to_dict(orient='records')
    types_json = json.dumps(types_dict)
    ti.xcom_push(key='types_json', value=types_json)

    services_lookup_dict = services_lookup.to_dict(orient='records')
    services_lookup_json = json.dumps(services_lookup_dict)
    ti.xcom_push(key='services_lookup_json', value=services_lookup_json)

    rev_dict = estimated_rev.to_dict(orient='records')
    rev_json = json.dumps(rev_dict)
    ti.xcom_push(key='rev_json', value = rev_json)

    packages_dict = subs_packages.to_dict(orient='records')
    packages_json = json.dumps(packages_dict)
    ti.xcom_push(key='packages_json', value =packages_json)

# Task to unzip the file
task1 = BashOperator(
    task_id='gunzip_file',
    bash_command='gunzip /home/elieba/airflow/files/*.gz',
    dag=dag
)

# Task to read subscribers
task2 = PythonOperator(
    task_id='read_subscribers',
    python_callable=read_subscribers_records,
    dag=dag
)

# Task to read services
task3 = PythonOperator(
    task_id='read_services',
    python_callable=read_services_records,
    dag=dag
)

# Task to transform data
task4 = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag
)
# Task to change file permissions

task5 = BashOperator(
    task_id = 'change_permissions',
    bash_command = f"chown postgres:postgres {CLEANED_FILE} && chmod 644 {CLEANED_FILE}",
    dag=dag
)
#Task to dump the data into PostgreSQL
task6 = SQLExecuteQueryOperator(
    task_id='load_data_to_db',
    conn_id='postgres_hp_centos9',  # Set your Postgres connection ID here
    sql=f"""
        COPY crm_data (MSISDN, IMSI, PRICE_PLAN_NAME, PROD_SPEC_NAME, SERVICES, SUBS_TYPE_ID, SUBS_STATUS_ID)
        FROM '{CLEANED_FILE}'
        DELIMITER ',' 
        CSV HEADER;
    """,
    dag=dag,
)

task7 = PythonOperator(
    task_id = 'load_lookups',
    python_callable = load_lookups, 
    dag = dag
)

# Define task dependencies
task1 >> task2 >> task3 >> task7 >> task4 >> task5 >> task6
