from airflow import DAG 
from airflow.operators.python import PythonOperator # Fixed Import
from datetime import datetime
import pandas as pd
import os

def process_data():
    path = "/opt/airflow/output/alerts"
    
    # Ensure the directory exists to avoid FileNotFoundError
    if not os.path.exists(path):
        print(f"Directory {path} not found.")
        return

    files = [f for f in os.listdir(path) if f.endswith(".parquet")]

    if not files:
        print("No parquet files found to process.")
        return

    df_list = []
    for f in files:
        df_list.append(pd.read_parquet(os.path.join(path, f)))

    df = pd.concat(df_list)

    # Logic to satisfy Scenario 3: Daily User Segmentation or Top Products 
    top_products = df.groupby("product_id")["view_count"].sum().reset_index()
    top_products = top_products.sort_values(by="view_count", ascending=False).head(5)

    # Save final analytic report
    output_path = "/opt/airflow/output/top_products.csv"
    top_products.to_csv(output_path, index=False)

    print(f"Report generated at {output_path}")
    print(top_products)

dag = DAG(
    'ecommerce_batch_pipeline',
    start_date=datetime(2026, 4, 28),
    schedule_interval='@daily',
    catchup=False
)

task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag
)