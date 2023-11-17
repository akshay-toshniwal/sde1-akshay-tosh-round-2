import logging
import pandas as pd
from datetime import datetime, timedelta
from connections import MySQLConnection, PostgresConnection
from service import upload_to_gcs

REPORT_FILE = "customer_report.csv"
STORAGE_BUCKET = "BUCKET_NAME"

class CustomerReportGenerator:
    """Generates a report on customer activities."""

    def __init__(self):
        """
        Initializes the CustomerReportGenerator.

        Attributes:
        - postgres_conn: PostgreSQL database connection.
        - mysql_conn: MySQL database connection.
        """
        logging.basicConfig(
            filename='report.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.postgres_conn = PostgresConnection.get_postgres_connection()
        self.mysql_conn = MySQLConnection.get_mysql_connection()

    def fetch_active_users_from_postgres(self):
        """
        Fetches active users from PostgreSQL.

        Returns:
        - DataFrame: Active users' data.
        """
        try:
            logging.info("Fetching active users from PostgreSQL...")
            query = "SELECT user_id, user_name FROM mindtickle_users WHERE active_status = 'active';"
            df_active_users = pd.read_sql(query, self.postgres_conn)
            return df_active_users
        except Exception as e:
            logging.error(f"Error fetching active users: {str(e)}")
            return pd.DataFrame()

    def fetch_lessons_completed_for_active_users(self, active_users, start_date):
        """
        Fetches lessons completed by active users from MySQL.

        Args:
        - active_users (DataFrame): DataFrame containing active users' data.
        - start_date (datetime): Start date for fetching lessons completed.

        Returns:
        - DataFrame: Lessons completed by active users.
        """
        try:
            active_users_list = tuple(str(uid) for uid in active_users['user_id'].tolist())
            logging.info(f"Active users: {active_users_list}")

            logging.info("Fetching lessons completed from MySQL...")
            query = f"""
            SELECT user_id, COUNT(lesson_id) as lessons_completed, completion_date
            FROM lesson_completion WHERE user_id IN {active_users_list}  
            AND completion_date >= '{start_date.strftime('%Y-%m-%d')}'
            GROUP BY user_id, completion_date 
            ORDER BY user_id, completion_date;
            """
            df_lessons_completed = pd.read_sql(query, self.mysql_conn)
            return df_lessons_completed
        except Exception as e:
            logging.error(f"Error fetching lessons completed: {str(e)}")
            return pd.DataFrame()

    def generate_customer_report(self):
        """
        Generates a customer report based on active users' lessons completion data.
        """
        try:
            active_users = self.fetch_active_users_from_postgres()
            start_date = datetime.now() - timedelta(days=120)
            lessons_completed = self.fetch_lessons_completed_for_active_users(active_users, start_date)
            
            final_df = pd.merge(active_users, lessons_completed, on='user_id', how="left")
            final_df.to_csv(REPORT_FILE, index=False)
            logging.info(f"Customer report is generated {REPORT_FILE}")

            # Upload the CSV file to Google Cloud Storage
            # bucket_name = STORAGE_BUCKET  # Replace with your GCS bucket name
            # destination_blob_name = REPORT_FILE
            # result = upload_to_gcs(REPORT_FILE, bucket_name, destination_blob_name)
            # if result:
            #     logging.info(f"File {REPORT_FILE} uploaded to {bucket_name} as {destination_blob_name}")

        except Exception as e:
            logging.error(f"Error generating report: {str(e)}")
            return pd.DataFrame()

# Generate report
report_generator = CustomerReportGenerator()
report_generator.generate_customer_report()
