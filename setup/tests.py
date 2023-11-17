import os
import unittest
from unittest.mock import Mock, patch

from datetime import datetime, timedelta
import pandas as pd

from main import CustomerReportGenerator

class TestCustomerReportGenerator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.report_generator = CustomerReportGenerator()

    def test_fetch_active_users_from_postgres(self):
        with patch('main.PostgresConnection.get_postgres_connection') as mock_postgres_connection:
            mock_cursor = Mock()
            mock_cursor.execute.return_value = None  # Mocking the execute method

            # Mocking the Postgres connection and its cursor
            mock_postgres_connection.return_value.cursor.return_value = mock_cursor

            # Assuming report_generator is an instance of the class where fetch_active_users_from_postgres is defined
            report_generator = CustomerReportGenerator()  # Creating an instance of ReportGenerator
            result = report_generator.fetch_active_users_from_postgres()

        self.assertIsInstance(result, pd.DataFrame)

    def test_fetch_lessons_completed_for_active_users(self):
        with patch('main.MySQLConnection.get_mysql_connection') as mock_mysql_connection:
            mock_cursor = Mock()
            mock_cursor.execute.return_value = None  # Mocking the execute method

            # Mocking the MySQL connection and its cursor
            mock_mysql_connection.return_value.cursor.return_value = mock_cursor

            active_users_data = pd.DataFrame({'user_id': [1, 2, 3], 'user_name': ['Alice', 'Bob', 'Charlie']})
            start_date = datetime.now() - timedelta(days=120)

            # Assuming report_generator is an instance of the class where fetch_lessons_completed_for_active_users is defined
            report_generator = CustomerReportGenerator()  # Creating an instance of ReportGenerator
            result = report_generator.fetch_lessons_completed_for_active_users(active_users_data, start_date)

        self.assertIsInstance(result, pd.DataFrame)


    def test_generate_customer_report(self):
        # Mocking fetch_active_users_from_postgres and fetch_lessons_completed_for_active_users
        with patch.object(self.report_generator, 'fetch_active_users_from_postgres') as mock_fetch_active_users, \
                patch.object(self.report_generator, 'fetch_lessons_completed_for_active_users') as mock_fetch_lessons_completed:
            mock_fetch_active_users.return_value = pd.DataFrame({'user_id': [1, 2], 'user_name': ['Alice', 'Bob']})
            mock_fetch_lessons_completed.return_value = pd.DataFrame({'user_id': [1, 2], 'lessons_completed': [5, 10]})

            self.report_generator.generate_customer_report()

            # Check if the CSV file was generated
            file_path = 'customer_report.csv'
            self.assertTrue(os.path.exists(file_path), f"CSV file '{file_path}' was not generated.")

            # Optionally, you can add assertions to check the content or structure of the generated CSV file
            # For example, checking if the file is not empty:
            self.assertGreater(os.path.getsize(file_path), 0, "CSV file is empty.")

if __name__ == '__main__':
    unittest.main()
