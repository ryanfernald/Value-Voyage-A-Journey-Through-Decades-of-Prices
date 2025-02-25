import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv
import os
import matplotlib.colors as mcolors

# Load environment variables from .env
load_dotenv()

# Retrieve DB credentials from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT'))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Specify the data source value to exclude
EXCLUDE_DATA_SOURCE = "data_source_to_exclude"


def get_distinct_years():
    """Fetch distinct years from the goods_prices table with the given filter."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        query = "SELECT DISTINCT YEAR(date) as year FROM goods_prices WHERE data_source != %s;"
        cursor = connection.cursor()
        cursor.execute(query, (EXCLUDE_DATA_SOURCE,))
        years = [row[0] for row in cursor.fetchall()]
        return [y for y in range(min(years), max(years)+1)]
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return []
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def get_distinct_goods():
    """Fetch distinct goods (names) from the goods_prices table with the given filter."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        query = "SELECT DISTINCT name as good_name FROM goods_prices WHERE data_source != %s;"
        cursor = connection.cursor()
        cursor.execute(query, (EXCLUDE_DATA_SOURCE,))
        goods = [row[0] for row in cursor.fetchall()]
        return sorted(goods)
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return []
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def get_july2_records():
    """Fetch records for July 2 (month=7, day=2) with the given filter."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        query = """
            SELECT YEAR(date) as year, name as good_name
            FROM goods_prices
            WHERE data_source != %s
              AND MONTH(date) = 7
              AND DAY(date) = 2;
        """
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, (EXCLUDE_DATA_SOURCE,))
        records = cursor.fetchall()
        return pd.DataFrame(records)
    except mysql.connector.Error as e:
        print(f"Error: {e}")
        return pd.DataFrame()
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()


def create_binary_matrix(years, goods, july2_df):
    """
    Create a DataFrame with years as rows and goods as columns.
    Value 1 indicates that a record exists for July 2 for that good in that year, 0 otherwise.
    """
    # Initialize the matrix with zeros.
    matrix = pd.DataFrame(0, index=years, columns=goods)

    # Mark cells with a record (set to 1) if found in the july2_df.
    for _, row in july2_df.iterrows():
        year = row['year']
        good = row['good_name']
        if year in matrix.index and good in matrix.columns:
            matrix.at[year, good] = 1
    return matrix


def plot_heatmap(matrix):
    """
    Plot the heatmap and save it as a PNG file.
    The axes are reversed: X-axis shows Years and Y-axis shows Good Names.
    """
    # Transpose the matrix so that Years become the X-axis and Good Names the Y-axis.
    matrix_transposed = matrix.T

    # Increase figure size for better visibility.
    plt.figure(figsize=(30, 60))
    cmap = mcolors.ListedColormap(['red', 'green'])

    sns.heatmap(matrix_transposed, cmap=cmap, linewidths=0.5, linecolor="gray",
                cbar=False, square=True, annot=False, vmin=0, vmax=1)
    plt.xlabel("Year")
    plt.ylabel("Good Name")
    plt.title("July 2 Data Entry Heatmap (Green: Entry exists, Red: No entry) - Reversed Axes")

    # Rotate X-axis labels if necessary
    plt.xticks(rotation=90, ha="center")
    plt.yticks(rotation=0)

    output_filename = "heatmap.png"
    plt.savefig(output_filename, dpi=300, bbox_inches="tight")
    print(f"Heatmap saved as {output_filename}")


if __name__ == "__main__":
    years = get_distinct_years()
    goods = get_distinct_goods()
    july2_df = get_july2_records()

    # Create a binary matrix of entries: 1 if record exists, 0 otherwise.
    matrix = create_binary_matrix(years, goods, july2_df)
    plot_heatmap(matrix)
