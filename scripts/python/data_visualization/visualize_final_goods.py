from matplotlib import pyplot as plt
from src.functions.db import get_connection
import mysql.connector

def get_final_goods_affordable_quantity(final_goods, start_year, end_year, income_interval='annual', income_data_source="BEA", output_format='json'):
    """
    Calculates how many units of each final good can be afforded for each year in the given range
    based on the average income of each year.

    Parameters:
        final_goods (list): A list of good names (strings) for which prices will be queried.
        start_year (int): The starting year for the analysis.
        end_year (int): The ending year for the analysis.
        income_interval (str): Specifies the salary frequency. Use 'annual' for the full annual salary
                               (default) or 'monthly' to use the monthly salary (annual salary divided by 12).
        output_format (str): Format of the output. Use 'json' (default) for a dictionary output or 'df'
                             for a pandas DataFrame in long format.

    Returns:
        dict or DataFrame: If output_format is 'json', returns a dictionary mapping each good to another
                           dictionary mapping each year to a dictionary containing:
                             - "quantity": The affordable quantity (average_income / price) as an integer.
                             - "unit": The unit extracted from the good_unit field.
                           If output_format is 'df', returns a pandas DataFrame in long format with columns:
                           ['year', 'good', 'quantity', 'unit'].
    """
    goods_affordable = {}
    try:
        # Establish database connection
        connection = get_connection()
        cursor = connection.cursor()

        # Fetch incomes for all years in the range.
        income_query = """
            SELECT year, average_income_unadjusted
            FROM incomes
            WHERE year BETWEEN %s AND %s
            AND source_name=%s
            AND region='united states';
        """
        cursor.execute(income_query, (start_year, end_year, income_data_source))
        incomes_data = cursor.fetchall()
        # Build a mapping: year -> adjusted average income
        incomes = {}
        for row in incomes_data:
            yr, income = row
            if income is None:
                continue
            if income_interval.lower() == 'monthly':
                income = income / 12
            elif income_interval.lower() != 'annual':
                raise ValueError("Invalid income_interval. Use 'annual' or 'monthly'.")
            incomes[yr] = income

        # Prepare placeholders for final_goods list in the IN clause.
        goods_placeholders = ','.join(['%s'] * len(final_goods))
        price_query = f"""
            SELECT name, YEAR(date) AS yr, price, good_unit
            FROM goods_prices
            WHERE name IN ({goods_placeholders})
              AND MONTH(date) = 7
              AND DAY(date) = 2
              AND YEAR(date) BETWEEN %s AND %s;
        """
        params = tuple(final_goods) + (start_year, end_year)
        cursor.execute(price_query, params)
        prices_data = cursor.fetchall()

        # Build a mapping: (good, year) -> (price, good_unit)
        prices = {}
        for row in prices_data:
            name, yr, price, good_unit = row
            prices[(name, yr)] = (price, good_unit)

        # Build result dictionary.
        # For each good, for each year in the range, calculate affordable quantity if data exists.
        for good in final_goods:
            goods_affordable[good] = {}
            for yr in range(start_year, end_year + 1):
                if yr not in incomes:
                    goods_affordable[good][yr] = None
                    continue

                income_for_year = incomes[yr]
                key = (good, yr)
                if key not in prices:
                    goods_affordable[good][yr] = None
                    continue

                price, good_unit = prices[key]
                if price is None or price <= 0:
                    goods_affordable[good][yr] = None
                    continue

                affordable_quantity = int(income_for_year / price)
                # Extract the unit after '/' if available, otherwise use the entire string.
                if isinstance(good_unit, str) and '/' in good_unit:
                    parts = good_unit.split('/')
                    unit = parts[1].strip() if len(parts) > 1 else good_unit
                else:
                    unit = good_unit

                goods_affordable[good][yr] = {
                    "quantity": affordable_quantity,
                    "unit": unit
                }

    except mysql.connector.Error as e:
        print("Database error:", e)
        return None
    except Exception as e:
        print("Error:", e)
        return None
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

    # Return output in desired format.
    if output_format.lower() == 'df':
        import pandas as pd
        # Create a DataFrame in long format.
        rows = []
        for good, year_data in goods_affordable.items():
            for yr in range(start_year, end_year + 1):
                entry = year_data.get(yr)
                if entry is None:
                    rows.append({'year': yr, 'good': good, 'quantity': None, 'unit': None})
                else:
                    rows.append({
                        'year': yr,
                        'good': good,
                        'quantity': entry.get("quantity"),
                        'unit': entry.get("unit")
                    })
        df = pd.DataFrame(rows)
        return df

    return goods_affordable


def plot_incomes_inf_final_goods():
    df = get_final_goods_affordable_quantity(
        # ['bacon', 'butter', 'coffee', 'milk', 'pork chop', 'round steak'],
        ['bacon', 'bread', 'butter', 'coffee', 'eggs', 'flour', 'milk', 'pork chop', 'round steak', 'sugar'],
        1929, 2024,
        "monthly",
        income_data_source='FRED',
        output_format='df'
    )
    print(df)

    plt.figure(figsize=(20, 10))

    # If the DataFrame contains data for only one year, use a bar chart.
    if df['year'].nunique() == 1:
        plt.bar(df['good'], df['quantity'])
        plt.xlabel("Good")
        plt.ylabel("Affordable Quantity")
        plt.title(f"Affordable Quantity in {df['year'].iloc[0]}")
    else:
        # For multiple years, plot a line for each good.
        for good, group in df.groupby('good'):
            plt.plot(group['year'], group['quantity'], marker='o', label=good)
        plt.xlabel("Year")
        plt.ylabel("Affordable Quantity")
        plt.title("Affordable Quantity Over Years FRED")
        plt.legend()

    # Save the graph as a PNG file.
    plt.savefig("../../../doc/figures/affordable_quantity_1929_2024_fred_incomes_sample.png")

    plt.show()


if __name__ == "__main__":
    plot_incomes_inf_final_goods()