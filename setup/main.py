import pandas as pd
import psycopg2
import mysql.connector
from google.cloud import storage
from io import BytesIO
from enumlist import POSTGRES_DB,POSTGRES_USER,MYSQL_ROOT_PASSWORD,MYSQL_HOSTNAME,MYSQL_DATABASE,POSTGRES_PASSWORD
# 

# Function to fetch data from PostgreSQL
def fetch_data_postgresql():
    conn = psycopg2.connect(
        host=MYSQL_HOSTNAME,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    query = """
        SELECT user_id
        FROM mindtickle_users
        WHERE active_status = 'active';
    """
    data = pd.read_sql_query(query, conn)
    conn.close()
    return data

# Function to fetch data from MySQL
def fetch_data_mysql():
    conn = mysql.connector.connect(
        host=MYSQL_HOSTNAME,
        user=POSTGRES_USER,
        password=MYSQL_ROOT_PASSWORD,
        database=MYSQL_DATABASE
    )
    query = """
        SELECT * FROM lesson_completion;
    """
    data = pd.read_sql_query(query, conn)
    conn.close()
    return data


def merge_and_process_data(df_postgresql, df_mysql):
    # Merge data from both databases as needed
    merged_data = pd.merge(df_postgresql, df_mysql, on='user_id', how='inner')
    return merged_data

# Function to save the report to GCS
def save_report_to_gcs(report_df, bucket_name, file_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Convert the DataFrame to CSV format and save to GCS
    csv_data = report_df.to_csv(index=False)
    blob.upload_from_string(csv_data)

def main():
    df_postgresql = fetch_data_postgresql()
    df_mysql = fetch_data_mysql()
    merged_data = merge_and_process_data(df_postgresql, df_mysql)
    
    # Filter data for the last 30 days
    merged_data['lesson_date'] = pd.to_datetime(merged_data['lesson_date'])
    last_30_days = merged_data['lesson_date'] >= pd.Timestamp('today') - pd.DateOffset(days=30)
    report_df = merged_data[last_30_days]
    
    # Save the report to GCS
    bucket_name = 'your-gcs-bucket-name'
    file_name = 'user_activity_report.csv'
    save_report_to_gcs(report_df, bucket_name, file_name)

if __name__ == "__main__":
    main()
