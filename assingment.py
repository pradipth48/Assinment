import pandas as pd
import json


def read_files(file1, file2):
    try:
        data1 = pd.read_csv(file1)
        data2 = pd.read_csv(file2)
        data = pd.concat([data1, data2], ignore_index=True)

        return data
    except FileExistsError:
        print(f"the file not found {data} was not found")
        return None

#read the csv file as provided on asssingment

csv_data = "D:\\csv_file\\order_region_a.csv"
csv_data1 = "D:\\csv_file\\order_region_b.csv"

output = read_files(csv_data, csv_data1)

if output is not None:
    print(output)
else:
    print("file not found")
# Performing the manual operation using pandas

output["total_sales"] = output["ItemPrice"] * output["QuantityOrdered"]
df = output.drop_duplicates("ItemPrice", keep="first")

df["region"]= df["total_sales"].apply(lambda x : "A" if x >= 500  else "B")

df['amount'] = output['PromotionDiscount'].apply(
    lambda x: float(json.loads(x).get("Amount", 0)) if isinstance(x, str) else 0)
print(df)

"""Now Create the connection with Mysql """

from sqlalchemy import create_engine,text
def database_setup():
    conn =create_engine('mysql+mysqlconnector://root:root@localhost/Assigment_data')

    if conn.connect():
        print("Connect to the Database")

    df.to_sql("sales_file", conn, if_exists="replace", index=False)

    print("data load succesfully")

    with conn.connect() as result:
        query_count = result.execute(text("select count(*) from sales_file"))
        query_data = query_count.fetchone()[0]

        total_sales=result.execute(text("select region ,sum(total_sales) from sales_file group by region"))
        sales_data=total_sales.fetchall()

        result_avg_per_traction = result.execute(text(
            "SELECT AVG(total_sales) AS avg_sales_amount FROM sales_data"
        )).scalar()

    print(f"total umber of data record :{query_data, sales_data,result_avg_per_traction}")

database_out=database_setup()
print(database_out)