import logging
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2

# =========================================================
# LOGGING
# =========================================================

logger = logging.getLogger("airflow.task")

# =========================================================
# POSTGRES CONFIG
# =========================================================

POSTGRES_CONFIG = {
    "host": "postgres",
    "database": "ecommerce_analytics",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}

# =========================================================
# DAG DEFAULT ARGUMENTS
# =========================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# =========================================================
# DAG DEFINITION
# =========================================================

dag = DAG(
    dag_id="daily_clickstream_segmentation_report",
    default_args=default_args,
    description="Daily user segmentation + product analytics report",
    schedule_interval="*/2 * * * *",
    catchup=False
)

# =========================================================
# STEP 1: FETCH DATA FROM POSTGRES
# =========================================================

def fetch_clickstream_data():

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    query = """
        SELECT user_id, product_id, event_type, event_timestamp
        FROM clickstream_events
        WHERE event_timestamp >= NOW() - INTERVAL '1 day'
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    logger.info(f"Fetched {len(rows)} rows from database")

    return rows

# =========================================================
# STEP 2: USER SEGMENTATION LOGIC
# =========================================================

def segment_users(**context):

    rows = fetch_clickstream_data()

    df = pd.DataFrame(rows, columns=[
        "user_id", "product_id", "event_type", "event_timestamp"
    ])

    # Group user behavior
    user_stats = df.groupby("user_id").agg(
        total_views=("event_type", lambda x: (x == "view").sum()),
        total_cart=("event_type", lambda x: (x == "add_to_cart").sum()),
        total_purchases=("event_type", lambda x: (x == "purchase").sum())
    ).reset_index()

    # Segmentation rules
    def segment(row):
        if row["total_purchases"] > 0:
            return "Buyer"
        elif row["total_cart"] > 0:
            return "Potential Buyer"
        else:
            return "Window Shopper"

    user_stats["segment"] = user_stats.apply(segment, axis=1)
    user_stats["segmentation_date"] = datetime.now().date()

    logger.info("User segmentation completed")

    return user_stats.to_dict(orient="records")

# =========================================================
# STEP 3: SAVE SEGMENTATION TO POSTGRES
# =========================================================

def save_segmentation(**context):

    data = context["task_instance"].xcom_pull(task_ids="segment_users")

    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = conn.cursor()

    for row in data:

        cursor.execute("""
            INSERT INTO user_segmentation
            (user_id, total_views, total_add_to_cart,
             total_purchases, user_segment, segmentation_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row["user_id"],
            row["total_views"],
            row["total_cart"],
            row["total_purchases"],
            row["segment"],
            row["segmentation_date"]
        ))

    conn.commit()
    cursor.close()
    conn.close()

    logger.info("User segmentation saved to PostgreSQL")

# =========================================================
# STEP 4: TOP PRODUCTS REPORT
# =========================================================

def generate_top_products_report():

    conn = psycopg2.connect(**POSTGRES_CONFIG)

    query = """
        SELECT product_id,
               COUNT(*) FILTER (WHERE event_type='view') AS views,
               COUNT(*) FILTER (WHERE event_type='purchase') AS purchases
        FROM clickstream_events
        WHERE event_timestamp >= NOW() - INTERVAL '1 day'
        GROUP BY product_id
        ORDER BY views DESC
        LIMIT 5
    """

    df = pd.read_sql(query, conn)

    conn.close()

    logger.info("Top products extracted")

    return df

# =========================================================
# STEP 5: WRITE REPORT FILE
# =========================================================

def write_report(**context):

    df = generate_top_products_report()

    file_path = "/opt/airflow/reports/daily_report.txt"

    with open(file_path, "w") as f:

        f.write("DAILY E-COMMERCE CLICKSTREAM REPORT\n")
        f.write("=" * 50 + "\n\n")

        f.write("TOP 5 MOST VIEWED PRODUCTS\n\n")

        for index, row in df.iterrows():
            f.write(
                f"{index+1}. Product: {row['product_id']} | "
                f"Views: {row['views']} | "
                f"Purchases: {row['purchases']}\n"
            )

    logger.info(f"Report generated at {file_path}")

# =========================================================
# AIRFLOW TASKS
# =========================================================

task_segment_users = PythonOperator(
    task_id="segment_users",
    python_callable=segment_users,
    provide_context=True,
    dag=dag
)

task_save_segmentation = PythonOperator(
    task_id="save_segmentation",
    python_callable=save_segmentation,
    provide_context=True,
    dag=dag
)

task_generate_report = PythonOperator(
    task_id="generate_report",
    python_callable=write_report,
    provide_context=True,
    dag=dag
)

# =========================================================
# TASK DEPENDENCIES
# =========================================================

task_segment_users >> task_save_segmentation >> task_generate_report