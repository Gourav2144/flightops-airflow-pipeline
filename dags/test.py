from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pymongo
import os

def test_mongodb_connection():
    # आपके docker-compose एनवायरनमेंट वेरिएबल्स से क्रेडेंशियल्स लेना
    mongo_host = os.getenv("MONGO_HOST", "mongodb")
    mongo_user = os.getenv("MONGO_USER", "mongo_user")
    mongo_pass = os.getenv("MONGO_PASSWORD", "mongo_pass")
    
    # कनेक्शन स्ट्रिंग बनाना
    client = pymongo.MongoClient(f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/")
    
    # कनेक्शन चेक करना (पिंग कमांड)
    db = client.admin
    response = db.command('ping')
    print("MongoDB Connection Status:", response)

default_args = {
    'owner': 'flightops',
    'start_date': datetime(2026, 1, 1),
}

with DAG('test_mongodb_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    
    connect_task = PythonOperator(
        task_id='check_mongo_connectivity',
        python_callable=test_mongodb_connection
    )
