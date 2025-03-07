# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Transforms

# COMMAND ----------

# Access configuration
container = "ecom-data"
storage_account = "ecomdatastorage"
application_id = "fb7ba273-59ce-42eb-91d7-4d054efd9277"
directory_id = "9304b1b4-1646-4e1f-8059-6f38efeb3f40"
service_credential = "VOS8Q~yupQPSEhDOmqQYE_flu3TJkuQFAINAWdn9"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the Data from Azure Data Lake Storage Gen2

# COMMAND ----------

# specify directory
path_to_directory = "bronze/"

# List files in the directory
files = dbutils.fs.ls(f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path_to_directory}")

# Create a dictionary to hold DataFrames for each file
dataframes = {}

# Iterate through the files
for file in files:
    if file.name.endswith('.csv'):  # Check if the file is a CSV
        # Create a DataFrame for each CSV file
        df_name = file.name.replace('.csv', '')  # Create a name for the DataFrame (remove .csv)
        df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(file.path)  # Load the CSV file
        
        # Store the DataFrame in the dictionary
        dataframes[df_name] = df

# Now you can access each DataFrame by its name
for name, df in dataframes.items():
    print(f"DataFrame for {name}:")
    #df.show()  # Display the contents of the DataFrame


# COMMAND ----------

# MAGIC %md
# MAGIC ## Read a file from MongoDB
# MAGIC ### Product Category Names

# COMMAND ----------

from pymongo import MongoClient
import pandas as pd

# importing module
hostname = "7nbzb.h.filess.io"
database = "ecomprojectNoSQL_ispaidsend"
port = "27018"
username = "ecomprojectNoSQL_ispaidsend"
password = "7b9ab8c326127ed01b37ae949d8872b93801a745"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]

collection = mydatabase["product_categories"]

# Convert to pandas DataFrame
mongo_data = pd.DataFrame(list(collection.find()))

# Drop column not required
mongo_data.drop("_id", axis=1, inplace=True)

# Convert to Spark DataFrame
mongo_spark_df = spark.createDataFrame(mongo_data)
display(mongo_spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning the data

# COMMAND ----------

from pyspark.sql.functions import col, to_date, datediff, current_date, month, year, when

# COMMAND ----------

def clean_dataframe(df,name):
    print(f"Cleaning " + name)
    # Drop duplicates
    cleaned_df = df.dropDuplicates().na.drop("all")
    #display(cleaned_df)
    return cleaned_df

# COMMAND ----------


# Create a new dictionary to hold cleaned DataFrames
cleaned_dataframes = {}

# Iterate through the dictionary of DataFrames and apply the cleaning function
for name, df in dataframes.items():
    cleaned_name = f"{name}_cleaned"  # Create a new name with suffix
    cleaned_df = clean_dataframe(df, name)  # Clean the DataFrame
    cleaned_dataframes[cleaned_name] = cleaned_df  # Store the cleaned DataFrame in the new dictionary

# COMMAND ----------

cleaned_dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Customers

# COMMAND ----------

customers_df = cleaned_dataframes["olist_customers_dataset_cleaned"]
# Display the first 5 rows in a cleaner format
display(customers_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Geolocation

# COMMAND ----------

geolocation_df = cleaned_dataframes["olist_geolocation_dataset_cleaned"]
# Display the first 5 rows in a cleaner format
display(geolocation_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Order items

# COMMAND ----------

items_df = cleaned_dataframes["olist_order_items_dataset_cleaned"]
# Display the first 5 rows in a cleaner format
display(items_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Order Payments

# COMMAND ----------

order_payments_df = cleaned_dataframes["olist_order_payments_dataset_cleaned"]
# Display the first 5 rows in a cleaner format
display(order_payments_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Order Reviews

# COMMAND ----------

order_reviews_df = cleaned_dataframes["olist_order_reviews_dataset_cleaned"]
# Display the first 5 rows in a cleaner format
display(order_reviews_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Orders

# COMMAND ----------

orders_df = cleaned_dataframes["olist_orders_dataset_cleaned"]

# COMMAND ----------

### Convert Date Columns
orders_df = orders_df \
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date"))) \
    .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))    

# COMMAND ----------

### Calculate Delivery and Time Delays
orders_df = orders_df \
    .withColumn("delivery_lead_time", datediff("order_delivered_customer_date", "order_purchase_timestamp")) \
    .withColumn("estimated_delivery_time", datediff("order_estimated_delivery_date", "order_purchase_timestamp")) \
    .withColumn("delay_days", (col("delivery_lead_time") - col("estimated_delivery_time")))

# COMMAND ----------

# Display the first 5 rows in a cleaner format
display(orders_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Products

# COMMAND ----------

products_df = cleaned_dataframes["olist_products_dataset_cleaned"]
# Display the first 5 rows in a cleaner format
display(products_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Sellers

# COMMAND ----------

sellers_df = cleaned_dataframes["olist_sellers_dataset_cleaned"]
# Display the first 5 rows in a cleaner format
display(sellers_df .limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark Transformations Vs Actions
# MAGIC
# MAGIC ### Transformations
# MAGIC
# MAGIC Transformations are operations on DataFrames that return a new DataFrame. They are lazily evaluated, meaning they do not execute immediately but build a logical plan that is executed when an action is performed.
# MAGIC
# MAGIC 𝟏. 𝐁𝐚𝐬𝐢𝐜 𝐓𝐫𝐚𝐧𝐬𝐟𝐨𝐫𝐦𝐚𝐭𝐢𝐨𝐧𝐬:
# MAGIC 𝐬𝐞𝐥𝐞𝐜𝐭():  Select specific columns.
# MAGIC 𝐟𝐢𝐥𝐭𝐞𝐫(): Filter rows based on a condition.
# MAGIC 𝐰𝐢𝐭𝐡𝐂𝐨𝐥𝐮𝐦𝐧():Add or replace a column.
# MAGIC 𝐝𝐫𝐨𝐩():  Remove columns.
# MAGIC 𝐰𝐡𝐞𝐫𝐞(𝐜𝐨𝐧𝐝𝐢𝐭𝐢𝐨𝐧): Equivalent to filter(condition).
# MAGIC 𝐝𝐫𝐨𝐩(*𝐜𝐨𝐥𝐬): Returns a new DataFrame with columns dropped.
# MAGIC 𝐝𝐢𝐬𝐭𝐢𝐧𝐜𝐭():Remove duplicate rows.
# MAGIC 𝐬𝐨𝐫𝐭(): Sort the DataFrame by columns.
# MAGIC 𝐨𝐫𝐝𝐞𝐫𝐁𝐲(): Order the DataFrame by columns.
# MAGIC
# MAGIC 𝟐. 𝐀𝐠𝐠𝐫𝐞𝐠𝐚𝐭𝐢𝐨𝐧 𝐚𝐧𝐝 𝐆𝐫𝐨𝐮𝐩𝐢𝐧𝐠:
# MAGIC 𝐠𝐫𝐨𝐮𝐩𝐁𝐲(): Group rows by column values.
# MAGIC 𝐚𝐠𝐠(): Aggregate data using functions.
# MAGIC 𝐜𝐨𝐮𝐧𝐭():  Count rows.
# MAGIC 𝐬𝐮𝐦(*𝐜𝐨𝐥𝐬):Computes the sum for each numeric column.
# MAGIC 𝐚𝐯𝐠(*𝐜𝐨𝐥𝐬): Computes the average for each numeric column.
# MAGIC 𝐦𝐢𝐧(*𝐜𝐨𝐥𝐬):Computes the minimum value for each column.
# MAGIC 𝐦𝐚𝐱(*𝐜𝐨𝐥𝐬): Computes the maximum value for each column.
# MAGIC
# MAGIC 𝟑. 𝐉𝐨𝐢𝐧𝐢𝐧𝐠 𝐃𝐚𝐭𝐚𝐅𝐫𝐚𝐦𝐞𝐬:
# MAGIC 𝐣𝐨𝐢𝐧(𝐨𝐭𝐡𝐞𝐫, 𝐨𝐧=𝐍𝐨𝐧𝐞, 𝐡𝐨𝐰=𝐍𝐨𝐧𝐞):  Joins with another DataFrame using the given join expression.
# MAGIC 𝐮𝐧𝐢𝐨𝐧(): Combine two DataFrames with the same schema.
# MAGIC 𝐢𝐧𝐭𝐞𝐫𝐬𝐞𝐜𝐭(): Return common rows between DataFrames.
# MAGIC
# MAGIC 𝟒. 𝐀𝐝𝐯𝐚𝐧𝐜𝐞𝐝 𝐓𝐫𝐚𝐧𝐬𝐟𝐨𝐫𝐦𝐚𝐭𝐢𝐨𝐧𝐬:
# MAGIC 𝐰𝐢𝐭𝐡𝐂𝐨𝐥𝐮𝐦𝐧𝐑𝐞𝐧𝐚𝐦𝐞𝐝():  Rename a column.
# MAGIC 𝐝𝐫𝐨𝐩𝐃𝐮𝐩𝐥𝐢𝐜𝐚𝐭𝐞𝐬(): Drop duplicate rows based on columns.
# MAGIC 𝐬𝐚𝐦𝐩𝐥𝐞(): Sample a fraction of rows.
# MAGIC 𝐥𝐢𝐦𝐢𝐭(): Limit the number of rows.
# MAGIC
# MAGIC 𝟓. 𝐖𝐢𝐧𝐝𝐨𝐰 𝐅𝐮𝐧𝐜𝐭𝐢𝐨𝐧𝐬:
# MAGIC 𝐨𝐯𝐞𝐫(𝐰𝐢𝐧𝐝𝐨𝐰𝐒𝐩𝐞𝐜): Defines a window specification for window functions.
# MAGIC 𝐫𝐨𝐰_𝐧𝐮𝐦𝐛𝐞𝐫().𝐨𝐯𝐞𝐫(𝐰𝐢𝐧𝐝𝐨𝐰𝐒𝐩𝐞𝐜): Assigns a row number starting at 1 within a window partition.
# MAGIC rank().over(windowSpec):  Provides the rank of rows within a window partition.
# MAGIC
# MAGIC ### Actions
# MAGIC
# MAGIC Actions trigger the execution of the transformations and return a result to the driver program or write data to an external storage system.
# MAGIC
# MAGIC 1. Basic Actions:
# MAGIC show(): Display the top rows of the DataFrame.
# MAGIC collect(): Return all rows as an array.
# MAGIC count(): Count the number of rows.
# MAGIC take(): Return the first N rows as an array.
# MAGIC first(): Return the first row.
# MAGIC head(): Return the first N rows.
# MAGIC
# MAGIC 2. Writing Data:
# MAGIC write(): Write the DataFrame to external storage.
# MAGIC write.mode(): Specify save mode (e.g., overwrite, append).
# MAGIC save(): Save the DataFrame to a specified path.
# MAGIC toJSON(): Convert the DataFrame to a JSON dataset.
# MAGIC
# MAGIC 3. Other Actions:
# MAGIC foreach(): Apply a function to each row.
# MAGIC foreachPartition(): Apply a function to each partition.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining Data

# COMMAND ----------

customer_orders_df = orders_df \
    .join(customers_df, orders_df.customer_id == customers_df.customer_id, "left") \
    .drop(orders_df.customer_id)   

# COMMAND ----------

display(customer_orders_df)

# COMMAND ----------

order_payments_df = customer_orders_df \
    .join(order_payments_df, customer_orders_df.order_id == order_payments_df.order_id, "left") \
    .drop(order_payments_df.order_id)

# COMMAND ----------

display(order_payments_df)

# COMMAND ----------

orders_items_df = order_payments_df \
    .join(items_df, "order_id", "left")

# COMMAND ----------

orders_items_products_df = orders_items_df \
    .join(products_df, orders_items_df.product_id == products_df.product_id, "left") \
    .drop(products_df.product_id)

# COMMAND ----------

display(orders_items_products_df)

# COMMAND ----------

final_df = orders_items_products_df \
    .join(sellers_df, orders_items_products_df.seller_id == sellers_df.seller_id, "left") \
    .drop(sellers_df.seller_id)

# COMMAND ----------

final_df = final_df \
    .join(mongo_spark_df, "product_category_name", "left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export Transformed Data to Silver Layer as a Parquet file

# COMMAND ----------

final_df.write \
    .mode("overwrite") \
    .parquet("abfss://ecom-data@ecomdatastorage.dfs.core.windows.net/silver")

# COMMAND ----------

# MAGIC %md
# MAGIC
