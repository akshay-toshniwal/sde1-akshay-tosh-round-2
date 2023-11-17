from dotenv import load_dotenv
import logging
import mysql.connector
import os
import psycopg2

load_dotenv()  # take environment variables from .env.


class PostgresConnection:
    """
    A class to manage PostgreSQL database connections.
    """

    _postgres_instance = None

    @staticmethod
    def get_postgres_connection():
        """
        Establishes a connection to PostgreSQL if not already connected.

        Returns:
        - psycopg2 connection: PostgreSQL connection object.
        """
        try:
            if PostgresConnection._postgres_instance is None:
                # PostgreSQL connection details from environment variables or configuration
                PostgresConnection._postgres_instance = psycopg2.connect(
                    dbname=os.getenv('POSTGRES_DB'),
                    user=os.getenv('POSTGRES_USER'),
                    password=os.getenv('POSTGRES_PASSWORD'),
                    host='mt-postgres',
                    port='5432'
                )
            return PostgresConnection._postgres_instance
        except Exception as e:
            logging.error(f"Error establishing PostgreSQL connection: {str(e)}")
            return None
    

class MySQLConnection:
    """
    A class to manage MySQL database connections.
    """

    _mysql_instance = None

    @staticmethod
    def get_mysql_connection():
        """
        Establishes a connection to MySQL if not already connected.

        Returns:
        - mysql.connector connection: MySQL connection object.
        """
        try:
            if MySQLConnection._mysql_instance is None:
                # MySQL connection details from environment variables or configuration
                MySQLConnection._mysql_instance = mysql.connector.connect(
                    user=os.getenv('MYSQL_user'),
                    password=os.getenv('MYSQL_ROOT_PASSWORD'),
                    host='mt-mysql',
                    database=os.getenv('MYSQL_DATABASE'),
                )
            return MySQLConnection._mysql_instance
        except Exception as e:
            logging.error(f"Error establishing MySQL connection: {str(e)}")
            return None
