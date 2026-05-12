import logging
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("airflow.task")

# =========================================================
# PRODUCT CATALOG
# Simulated product master data
# =========================================================

PRODUCT_CATALOG = {

    "iphone_15": {
        "name": "Apple iPhone 15",
        "category": "Smartphones",
        "price": 999
    },

    "samsung_s24": {
        "name": "Samsung Galaxy S24",
        "category": "Smartphones",
        "price": 899
    },

    "airpods_pro": {
        "name": "Apple AirPods Pro",
        "category": "Audio",
        "price": 249
    },

    "gaming_mouse": {
        "name": "Gaming Mouse RGB",
        "category": "Accessories",
        "price": 79
    },

    "mechanical_keyboard": {
        "name": "Mechanical Keyboard",
        "category": "Accessories",
        "price": 129
    },

    "macbook_air_m3": {
        "name": "MacBook Air M3",
        "category": "Laptops",
        "price": 1499
    },

    "ipad_pro": {
        "name": "iPad Pro",
        "category": "Tablets",
        "price": 1099
    },

    "smart_watch": {
        "name": "Smart Watch X",
        "category": "Wearables",
        "price": 299
    },

    "monitor_4k": {
        "name": "4K Ultra HD Monitor",
        "category": "Monitors",
        "price": 499
    },

    "gaming_laptop": {
        "name": "Gaming Laptop RTX",
        "category": "Laptops",
        "price": 1999
    }
}

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
    "retry_delay": timedelta(minutes=1)
}

# =========================================================
# DAG DEFINITION
# =========================================================

dag = DAG(
    dag_id="realtime_clickstream_segmentation_report",
    default_args=default_args,
    description="Realtime clickstream analytics report",
    schedule_interval="*/2 * * * *",
    catchup=False
)

# =========================================================
# FETCH CLICKSTREAM DATA
# =========================================================

def fetch_clickstream_data():

    conn = psycopg2.connect(**POSTGRES_CONFIG)

    cursor = conn.cursor()

    query = """
        SELECT
            user_id,
            product_id,
            event_type,
            event_timestamp
        FROM clickstream_events
        WHERE event_timestamp >= NOW() - INTERVAL '2 minutes'
    """

    cursor.execute(query)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    logger.info(f"Fetched {len(rows)} rows")

    return rows

# =========================================================
# USER SEGMENTATION
# =========================================================

def segment_users(**context):

    rows = fetch_clickstream_data()

    if len(rows) == 0:

        logger.warning("No recent data found")

        return []

    df = pd.DataFrame(rows, columns=[
        "user_id",
        "product_id",
        "event_type",
        "event_timestamp"
    ])

    user_stats = df.groupby("user_id").agg(

        total_views=(
            "event_type",
            lambda x: (x == "view").sum()
        ),

        total_cart=(
            "event_type",
            lambda x: (x == "add_to_cart").sum()
        ),

        total_purchases=(
            "event_type",
            lambda x: (x == "purchase").sum()
        )

    ).reset_index()

    def segment(row):

        if row["total_purchases"] > 0:
            return "Buyer"

        elif row["total_cart"] > 0:
            return "Potential Buyer"

        else:
            return "Window Shopper"

    user_stats["segment"] = user_stats.apply(
        segment,
        axis=1
    )

    user_stats["segmentation_timestamp"] = datetime.now()

    logger.info("User segmentation completed")

    return user_stats.to_dict(orient="records")

# =========================================================
# SAVE SEGMENTATION
# =========================================================

def save_segmentation(**context):

    data = context["task_instance"].xcom_pull(
        task_ids="segment_users"
    )

    if not data:

        logger.warning("No segmentation data")

        return

    conn = psycopg2.connect(**POSTGRES_CONFIG)

    cursor = conn.cursor()

    for row in data:

        cursor.execute("""
            INSERT INTO user_segmentation (
                user_id,
                total_views,
                total_add_to_cart,
                total_purchases,
                user_segment,
                segmentation_date
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (

            row["user_id"],
            row["total_views"],
            row["total_cart"],
            row["total_purchases"],
            row["segment"],
            row["segmentation_timestamp"]

        ))

    conn.commit()

    cursor.close()
    conn.close()

    logger.info("Segmentation saved successfully")

# =========================================================
# TOP PRODUCTS ANALYTICS
# =========================================================

def generate_top_products_report():

    conn = psycopg2.connect(**POSTGRES_CONFIG)

    query = """
        SELECT

            product_id,

            COUNT(*) FILTER (
                WHERE event_type='view'
            ) AS views,

            COUNT(*) FILTER (
                WHERE event_type='add_to_cart'
            ) AS add_to_cart,

            COUNT(*) FILTER (
                WHERE event_type='purchase'
            ) AS purchases

        FROM clickstream_events

        WHERE event_timestamp >= NOW() - INTERVAL '2 minutes'

        GROUP BY product_id

        ORDER BY views DESC

        LIMIT 5
    """

    df = pd.read_sql(query, conn)

    conn.close()

    # =====================================================
    # ENRICH WITH PRODUCT DETAILS
    # =====================================================

    product_names = []
    categories = []
    prices = []
    conversion_rates = []

    for _, row in df.iterrows():

        product_id = row["product_id"]

        product_info = PRODUCT_CATALOG.get(
            product_id,
            {
                "name": "Unknown Product",
                "category": "Unknown",
                "price": 0
            }
        )

        product_names.append(product_info["name"])
        categories.append(product_info["category"])
        prices.append(product_info["price"])

        views = row["views"]
        purchases = row["purchases"]

        if views > 0:
            conversion = round(
                (purchases / views) * 100,
                2
            )
        else:
            conversion = 0

        conversion_rates.append(conversion)

    df["product_name"] = product_names
    df["category"] = categories
    df["price_usd"] = prices
    df["conversion_rate"] = conversion_rates

    logger.info("Top product analytics generated")

    return df

# =========================================================
# WRITE REPORT
# =========================================================

def write_report(**context):

    df = generate_top_products_report()

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    file_path = (
        f"/opt/airflow/reports/"
        f"analytics_report_{timestamp}.txt"
    )

    with open(file_path, "w") as f:

        f.write(
            "E-COMMERCE REALTIME ANALYTICS REPORT\n"
        )

        f.write("=" * 80 + "\n\n")

        f.write(
            f"Generated At: {datetime.now()}\n"
        )

        f.write(
            "Analysis Window: Last 2 Minutes\n\n"
        )

        # =================================================
        # TOP PRODUCTS
        # =================================================

        f.write(
            "TOP 5 MOST VIEWED PRODUCTS\n"
        )

        f.write("-" * 80 + "\n\n")

        if df.empty:

            f.write("No product activity found\n")

        else:

            for index, row in df.iterrows():

                f.write(
                    f"Rank #{index+1}\n"
                )

                f.write(
                    f"Product ID       : {row['product_id']}\n"
                )

                f.write(
                    f"Product Name     : {row['product_name']}\n"
                )

                f.write(
                    f"Category         : {row['category']}\n"
                )

                f.write(
                    f"Price (USD)      : ${row['price_usd']}\n"
                )

                f.write(
                    f"Total Views      : {row['views']}\n"
                )

                f.write(
                    f"Add To Cart      : {row['add_to_cart']}\n"
                )

                f.write(
                    f"Purchases        : {row['purchases']}\n"
                )

                f.write(
                    f"Conversion Rate  : "
                    f"{row['conversion_rate']}%\n"
                )

                # =========================================
                # BUSINESS INSIGHT
                # =========================================

                if (
                    row["views"] > 50 and
                    row["purchases"] < 5
                ):

                    f.write(
                        "Insight          : "
                        "High interest but low conversion. "
                        "Flash sale recommended.\n"
                    )

                else:

                    f.write(
                        "Insight          : "
                        "Healthy customer engagement.\n"
                    )

                f.write("\n")
                f.write("-" * 80 + "\n\n")

        # =================================================
        # SUMMARY SECTION
        # =================================================

        total_views = df["views"].sum()
        total_purchases = df["purchases"].sum()

        if total_views > 0:
            overall_conversion = round(
                (total_purchases / total_views) * 100,
                2
            )
        else:
            overall_conversion = 0

        f.write("\nSUMMARY\n")

        f.write("-" * 80 + "\n")

        f.write(
            f"Total Product Views      : {total_views}\n"
        )

        f.write(
            f"Total Purchases          : {total_purchases}\n"
        )

        f.write(
            f"Overall Conversion Rate  : "
            f"{overall_conversion}%\n"
        )

    logger.info(f"Analytics report created: {file_path}")

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