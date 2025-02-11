from flask import jsonify
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os

# Load environment variables from .env
load_dotenv()

# Retrieve DB credentials from environment variables
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = int(os.getenv('DB_PORT', 3307))
DB_USER = os.getenv('DB_USER', 'value_voyage_backend')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'your_password_here')
DB_NAME = os.getenv('DB_NAME', 'historical_prices')

def test_db_connection():
    try:
        # Connect to MySQL using credentials from the .env file
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM test;")
            row = cursor.fetchone()
            if row:
                # Assuming the table 'test' has one column, return its value
                result = row[0]
                return jsonify({"result": result})
            else:
                return jsonify({"error": "No data found in test table"}), 404
    except Error as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == '__main__':
    app.run(debug=True)
