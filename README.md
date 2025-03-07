# Microsoft Azure Data Engineering Project

## 0. Project Architecture

This project demonstrates my ability to leverage cutting edge cloud based tools and technologies to engineer data pipelines to move data to and from a variety of sources, adopting the industry best practice Medallion Architecture.

This particular project illustrates my working knowledge of :

- Microsoft Azure 
- Azure Data Lake Storage (ADLS) Gen2 
- Azure Data Factory
- Azure Synapse Analytics
- Databricks | Spark
- MySQL
- MongoDB

These skills are highly transferable across other industy cloud platforms and tools. Whilst this is not a fully fledged data analysis project, it also illustrates my ability to work with both PySpark, pandas and SQL to read, write, clean and transform data.

![pipeline_diagram.png](images/d16b8ab3-3d99-42f8-9998-0018638fba79.png)

When working on independent data engineering projects, one of the major hurdles is getting (free) access to industry level tools. As far as possible this project has been performed at no cost, taking advantages of free tiers and trial periods.

## 1. Microsoft Azure

The first step is to sign up for a [free trial of Microsoft Azure](https://azure.microsoft.com/en-gb/pricing/offers/ms-azr-0044p) which at the time of writing offers the equivalent of $200 credit to be used within 30 days, and 12 months of selected free services. 

As with all cloud platforms, the biggest challenge is navigating the initial set-up configuration. However, the diagram below illustrates the general hierearch, with a management group having one or more subscriptions, which have one or more resource groups, which in turn have access to one or more resources.

![azure_account.JPG](images/049a1fc0-c8b7-401a-883e-2a6c052444b2.JPG)

Azure provides extensive [learning documentation](https://azure.microsoft.com/en-gb/get-started/). to help you get started. Access control is provided by way of keys, tokens, and secrets. Care should be taken to ensure these are not exposed. Vaults are provided within the Azure portal for enhanced security.

### Create a Resource Group 
![resource_group.JPG](images/53820064-486e-495a-9d88-9a72fe64a891.JPG)

# 2. Project Data

The data was originally hosted on [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

>This is a Brazilian ecommerce public dataset of orders made at Olist Store. The dataset has information of 100k orders from 2016 to 2018 made at multiple marketplaces in Brazil. Its features allows viewing an order from multiple dimensions: from order status, price, payment and freight performance to customer location, product attributes and finally reviews written by customers. We also released a geolocation dataset that relates Brazilian zip codes to lat/lng coordinates.
This is real commercial data, it has been anonymised, and references to the companies and partners in the review text have been replaced with the names of Game of Thrones great houses.

Acknowledgements to Mayank Aggarwal for making the raw csv files available [here](https://github.com/mayank953/BigDataProjects/tree/main/Project-Brazillian%20Ecommerce/Data).

## Source files

![dataset.JPG](images/33761357-be62-4d68-ab19-1c361dbebd8c.JPG)


## Schema

![schema.png](images/98373bb5-ec10-443a-bee7-da3a3326a94c.png)

# 3.Databases

If you need to create a database for independent learning purposes, then check out [filess.io](https://filess.io/) which offers a free tier.

For this project I created a MySQL and a MongoDB database. A very useful feature is the Connection Code which is available in a number of programming languages, facilitating easy access to the Database from other sources.


![filess.io.JPG](images/8e70acff-1423-4bbe-beb8-1203322cf817.JPG)

![create_databases.JPG](images/a38e33eb-1975-469f-8c9a-4f2ccebc5cb1.JPG)

## 3.1 MySQL

![mySQLdbase.JPG](images/f8366291-71f8-4233-9588-47d5f5e23cb2.JPG)

![mysql_dbase_connection.JPG](images/411e1784-e3c7-43f4-844e-66fea93d2284.JPG)


## 3.2 MongoDB
![mongodbase.JPG](images/f6ce6203-5139-481e-8492-f74adc109387.JPG)

![mongo_dbase_connection.JPG](images/7d1bf03f-2cce-4ad5-b935-984aad8f24e9.JPG)


# 4. Databricks

## Connecting to external databases from Databricks

Databricks offers a [free Community Edition](https://login.databricks.com/?dbx_source=CE&intent=CE_SIGN_UP&tuuid=8d37e8ce-279a-4438-8d16-472ceb4bc8aa).

>The Databricks Community Edition is the free version of our cloud-based big data platform. Its users can access a micro-cluster as well as a cluster manager and notebook environment. All users can share their notebooks and host them free of charge with Databricks. We hope this will enable everyone to create new and exciting content that will benefit the entire Apache Spark™ community.

The Databricks Community Edition also comes with a rich portfolio of award-winning training resources that will be expanded over time, making it ideal for developers, data scientists, data engineers and other IT professionals to learn Apache Spark.


## 4.1 Connecting to MySQL from DataBricks

In this step we will work within Databricks to:

- connect to the MySQL database we created on filess.io
- create a database table
- BATCH ingest a csv file held on GitHub
- insert into the MySQL table

`DataBricks Python NoteBook`


```python
# Connect to MySQL database held on filess.io
import mysql.connector
from mysql.connector import Error
import pandas as pd

# Connection details taken from filess.io
hostname = "y3bez.h.filess.io"
database = "ecomproject_breezetime"
port = 3307
username = "ecomproject_breezetime"
password = "<password>"

# Data file path
url_file_path = "https://raw.githubusercontent.com/mayank953/BigDataProjects/refs/heads/main/Project-Brazillian%20Ecommerce/Data/olist_order_payments_dataset.csv"

# Database table name where data will be ingested
table_name = "ecomm_order_payments"

try:
    # Establish a connection to MySQL server
    connection = mysql.connector.connect(
        host=hostname, 
        database=database,
         user=username,
          password=password,
           port=port
    )
    
    if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server")

        # Create a cursor to execute SQL queries
        cursor = connection.cursor()

        # Drop table of it already exists
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`;")
        print(f"Table `{table_name}` dropped if it existed.")

        # Create a table structure to match csv file
        create_table_query = f"""
        CREATE TABLE {table_name} (
            order_id VARCHAR(50),
            payment_sequential INT,
            payment_type VARCHAR(20),
            payment_installments INT,
            payment_value FLOAT
        );
        """
        cursor.execute(create_table_query)
        print(f"Table `{table_name}` created successfully!")

        # Load the csv data into a pandas DataFrame
        order_payments = pd.read_csv(url_file_path)
       
        print(f"CSV data loaded into a pandas DataFrame.")     
        
        # Insert data in batches of 1000 records
        batch_size = 1000
        total_records = len(order_payments)

        print(f"Starting data insertion into `{table_name}` in batches of {batch_size} records.")
        for start in range(0, total_records, batch_size):
            end = start + batch_size
            batch = order_payments.iloc[start:end] # Get the current batch of records

            # Convert batch to list of tuples for MySQL insertion                     
            batch_records = [
                tuple(row) for row in batch.itertuples(index=False, name=None)
            ]
            
            #Prepare the INSERT query
            insert_query = f"""
            INSERT INTO {table_name}
            (order_id, payment_sequential, payment_type, payment_installments, payment_value)
            VALUES (%s, %s, %s, %s, %s);
            """
            
            # Execute the INSERTION query
            cursor.executemany(insert_query, batch_records)
            connection.commit()
            print(f"Inserted records {start + 1} to {min(end, total_records)} successfully.")

        print(f"All `{total_records}` records inserted successfully into `{table_name}`.")

            
except Error as e:
    # Error handling
    print("Error while connecting to MySQL or inserting data", e)

finally:
    # Close the cursor and connection
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")
```

![load_data_to_MYSQL.JPG](images/9e8b4bb7-6eed-41df-a9e9-cb2292e9868f.JPG)

![load_data_to_MYSQL_2.JPG](images/96f4c6c0-46f5-4d9f-a6ef-122d0ece9624.JPG)

![load_data_to_MYSQL_3.JPG](images/74c936c3-ef68-458e-9743-6eb5b568234a.JPG)

## 4.2 Connecting to MongoDB from DataBricks

In this step we will work within Databricks to:

- connect to the MongoDB database we created on filess.io
- create a collection fron the csv held on GitHub
- insert into the MongoDB collection

`DataBricks Python NoteBook`


```python
# importing module
import pandas as pd
from pymongo import MongoClient

#csv_file_path = "dbfs:/FileStore/product_category_name_translation.csv"
url_file_path = "https://raw.githubusercontent.com/mayank953/BigDataProjects/refs/heads/main/Project-Brazillian%20Ecommerce/Data/product_category_name_translation.csv"

## Load the product_category csv file into a pandas dataframe
try:

    product_category_df = pd.read_csv(url_file_path)    
    
except FileNotFoundError:
    print("Error: 'product_category_name_translation.csv' not found.")
    exit()

## Mongo DB connection configuration 
hostname = "7nbzb.h.filess.io"
database = "ecomprojectNoSQL_ispaidsend"
port = "27018"
username = "ecomprojectNoSQL_ispaidsend"
password = "<password>"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

try:    
    ## Establish a connection to MongoDB
    client = MongoClient(uri)
    db = client[database]

    ## Select the collection (or create it if it doesn't exist)
    collection = db["product_categories"]

    ## Convert the DataFrame to a list of dictionaries for insertion into MongoDB 
    data_to_insert = product_category_df.to_dict(orient="records")

    ## Insert the data into the collection
    collection.insert_many(data_to_insert)

    print("Data uploaded to MongoDB successfully!")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the MongoDB connection
    if client:
        client.close()
```

![mogodb_data.JPG](images/3c57d63d-ad26-4162-8f9d-c8f777d28a8c.JPG)

# 5. Azure Data Factory

Azure Data Factory is a cloud ETL (Extract, Transform, Load) tool that collects data from different sources (INGESTION), cleans and organises it (TRANSFORMATION) and moves it (LOAD) to where we need it (SINK).



![azure_data_factory_resource.JPG](images/77a014e3-3a3b-4133-99b6-c3d87cc4f66f.JPG)

![ADF_data_source.JPG](images/0b595fef-1bba-454e-a5fc-d5fa62eec8f4.JPG)

![ADF_data_source_HTTP.JPG](images/69e9cbba-7be5-4ae2-9a33-470210dcc646.JPG)

![ADF_data_source_CSV.JPG](images/29988574-ae71-4c95-87c7-0883c922548f.JPG)

![ADF_linked_service.JPG](images/bf859b6c-482b-4cbd-b209-6e60b1a99cec.JPG)

![ADF_linked_service3.JPG](images/a37ca676-bbea-46c1-8a02-ef5e040a46c6.JPG)

![ADF_preview_data.JPG](images/896e24f1-2190-4c9b-aed7-0c7d038b6ea0.JPG)

We have manually configured the `olist_customers_dataset`, but by splitting the file paths into a base url, and a relative url, we can iterate over all of the remaining csv files to automate the source configuration process.

![ADF_iterate_over_base_URL.JPG](images/3f4ce394-61df-49c0-89de-d905d8ce8522.JPG)

# 6. Azure Data Lake Storage (ADLS)Gen 2

ADLSGen2 is a secure cloud platform that provides scalable, cost effective storage for Big Data analytics.

We will be taking the csv files from **source** : `Github` and moving them to a **data sink** : `Azure Data Lake Storage Gen2`. So let's first create a storage resource:

![AzureStorageAccount.JPG](images/299aa275-718c-456e-83b8-1375fe1e4d95.JPG)

![AzureStorageAccount_hierarchy.JPG](images/6cdc0df2-9790-447c-ac3e-caa395c527a7.JPG)

![ADLS_create_container.JPG](images/dfe710b1-7768-402e-8efe-cf32d0cdef68.JPG)

# 7. Medallion Architecture

In this project we will follow industry best practice, and adopt the [Medallion architecture](https://www.databricks.com/glossary/medallion-architecture), as illustrated below.

![medallion.JPG](images/356eab28-932c-485f-ab58-00941edeeb97.JPG)

![medallion_creation.JPG](images/8c035e7c-0456-4134-80bf-2d43db38046f.JPG)

# 8. Create a data ingestion Pipeline to connect to ADLSGen2 data sink

Using Azure Data Factory, we can create a data ingestion Pipeline to move the data from source (GitHub) to sink (ADLSGen2) :

![data_sink.JPG](images/23fe5c5f-d8b6-4988-ba36-fd3ea91754f5.JPG)

![sink_path.JPG](images/816de750-ffa4-4b02-a43e-edcd19948ac9.JPG)

Hit **Debug** to run the Pipeline, in our case move the csv file stored on Github to our Azure Data Storage container.

![source_to_sink.JPG](images/7e5b1372-d445-4aa3-aaa9-73687d3b21e0.JPG)

![hot_cold_storage.JPG](images/db880d70-ad97-4c7c-9391-b0e2208f097b.JPG)

![input_array.JPG](images/46c7a927-adac-4b1b-b5ec-fb5a37c81beb.JPG)

![dynamic_input.JPG](images/fb69532e-9fef-4f13-b964-49d15ca2cab7.JPG)

![dynamic_source.JPG](images/2869ca82-af5e-4339-8599-9b1ec604e7fd.JPG)

![dynamic_sink.JPG](images/b03653f5-ab56-4046-b067-61149bacb938.JPG)

# 9. Parameterisation

Initially, I manually configured the `olist_customers_dataset`, but by splitting the file paths into a base and relative url, and a file name, I can iterate over all of the remaining csv files to automate the source and sink configuration process.

To do this, I will use the Azure Data Factory activity: `Iteration & conditionals:For Each` and set each input as the following array :

`input_array.json`


```python
[

{
"csv_relative_url": "BigDataProjects/refs/heads/main/Project-Brazillian%20Ecommerce/Data/olist_customers_dataset.csv", #source
"file_name": "olist_customers_dataset.csv" #sink
},

{
"csv_relative_url": "BigDataProjects/refs/heads/main/Project-Brazillian%20Ecommerce/Data/olist_geolocation_dataset.csv",
"file_name": "olist_geolocation_dataset.csv"
},

{
"csv_relative_url": "BigDataProjects/refs/heads/main/Project-Brazillian%20Ecommerce/Data/olist_order_items_dataset.csv",
"file_name": "olist_order_items_dataset.csv"
},

{
"csv_relative_url": "BigDataProjects/refs/heads/main/Project-Brazillian%20Ecommerce/Data/olist_order_reviews_dataset.csv",
"file_name": "olist_order_reviews_dataset.csv"
},

{
"csv_relative_url": "BigDataProjects/refs/heads/main/Project-Brazillian%20Ecommerce/Data/olist_orders_dataset.csv",
"file_name": "olist_orders_dataset.csv"
},

{
"csv_relative_url": "BigDataProjects/refs/heads/main/Project-Brazillian%20Ecommerce/Data/olist_products_dataset.csv",
"file_name": "olist_products_dataset.csv"
},

{
"csv_relative_url": "BigDataProjects/refs/heads/main/Project-Brazillian%20Ecommerce/Data/olist_sellers_dataset.csv",
"file_name": "olist_sellers_dataset.csv"
}

]
```

Rather than copy the array into the value parameter, I will use another ADF Activity: `Lookup` to read in the array from the JSON file held on GitGub. This is better practice as it reduces the risk of error.

![ADF_lookup.JPG](images/59fdb283-5dd3-4000-98eb-917174091553.JPG)

# 10. Creating Linked Service to facilitate connection to MySQL database

![filess.io_connection.JPG](images/b8eacb9c-d8b3-4446-88e7-70cc8d16a7d3.JPG)

![ADF_connect_filess.io.JPG](images/9eb15cc4-a0b3-40e1-aebf-15438aae9df6.JPG)


# 11. Azure Databricks Workflow

I will be using Databricks to:

- read raw data from ADLSGen2 BRONZE layer
- perform basic transformation using Spark to clean, rename, filter etc
- perform join operations to integrate multiple datasets
- enrich the data via MongoDB
- perform aggregation and derive insights
- write final data back to ADLSGen2 SILVER layer

## Reading a csv file stored in ADLSGen2 BRONZE layer

https://learn.microsoft.com/en-us/azure/databricks/connect/storage/tutorial-azure-storage

Step 1: Create a Microsoft Entra ID service principal

Step 2: Create a client secret for your service principal

Step 3: Grant the service principal access to Azure Data Lake Storage Gen2

Step 4: Add the client secret to Azure Key Vault

Step 5: Configure your Azure key vault instance for Azure Databricks

Step 6: Create Azure Key Vault-backed secret scope in your Azure Databricks workspace


```python
# Access configuration for ADLSGen2
container = "ecom-data"
storage_account = "ecomdatastorage"
application_id = "fb7ba273-59ce-42eb-91d7-4d054efd9277" #Application(client)ID
directory_id = "9304b1b4-1646-4e1f-8059-6f38efeb3f40" #Directory(tenant)ID
service_credential = "<client_secrets_value>" #Client secrets - value

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")
```


```python
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
    df.show()  # Display the contents of the DataFrame
```

The transformation scripts are included in a separate file `Transforms.py`.

# 12. Azure Synapse

So far I have :

- ingested raw csv data and stored it in the BRONZE layer of ADLSGen2
- performed Spark transformations within DataBricks and written the data to the SILVER layer of ADLSGen2 

It is now time to leverage Azure Synapse Analytics to derive insights and write the final data to the GOLD layer for serving to clients, data scientists, analysts etc.



![azure_synapse_overview.JPG](images/26fb7da2-764a-46ee-a4a2-ceb30936a8f4.JPG)

![azure_synapse.JPG](images/c9a8fef9-7e14-43b3-beb8-1980f86434b4.JPG)

![azure_synapse_workspace.JPG](images/3ecc3914-f084-4201-8edb-981f083acb40.JPG)

![azure_synapse_console.JPG](images/ae324418-e2ad-4d82-b59d-d9981c35db2f.JPG)

![azure_synapse_console_data.JPG](images/f2b615a2-e14d-4d22-9660-69328b4e59d1.JPG)

![azure_synapse_console_develop.JPG](images/301a8c48-6c00-41bd-9b75-844b6fb1d27f.JPG)

![azure_synapse_console_integrate.JPG](images/71c4c89e-bf74-46fd-b662-c0386298aecb.JPG)

![azure_synapse_console_monitor.JPG](images/042355ef-4b78-403b-9bb6-4ab0a7d8e192.JPG)

![azure_synapse_console_manage.JPG](images/c51aed5f-28e2-4c07-98e2-390937d19750.JPG)

![azure_synapse_serverless.JPG](images/12ee9b58-722f-4815-80c5-eb982ecd66f0.JPG)

## 12.1 Granting Azure Synapse access to ADLSGen2

We need to give our Azure Synapse permission to connect to ADLSGen2. We do this by adding role assignment `Storage Blob Data Contributor` to our Azure Synapse Workspace.

![azure_synapse_access_ADLS.JPG](images/e4889efe-37a7-4bea-9f86-9fc6d03866b5.JPG)

![azure_synapse_access_ADLS2.JPG](images/fd3e7128-064a-440b-ba7e-30b0ae01a8dc.JPG)

![synapse_container.JPG](images/f65c6571-60a3-4e59-9db0-0f81f9b1acf2.JPG)

![synapse_container_access.JPG](images/b70e4a0d-81f4-4679-88d0-f63a5bf8fd3c.JPG)

# 12.2  Serverless Vs Dedicated SQL pool

![azure_synapse_workflow.JPG](images/9da45c07-09ae-4045-ab16-bd67adf6ab8f.JPG)

We can create a serverless SQL database in Synapse.

![synapse_dbase.JPG](images/ac818c26-ec9b-4667-a129-318788076b95.JPG)

![serverless_vs_dedicated.JPG](images/ef6f3dc6-313e-442b-8c60-45b377c75e6e.JPG)

# 13. SQL queries within Synapse Analytics

## 13.1 Accessing files held in ADLSGen2
To connect with ADLSGen2 from Azure Synapse we use [OPENROWSET](https://learn.microsoft.com/en-us/sql/t-sql/functions/openrowset-transact-sql?view=sql-server-ver16).

![synapse_SQL.JPG](images/b23f5550-7549-4ce4-8d36-7eb00630aab1.JPG)

## 13.2 Create VIEWS to work with data prior to exporting as an external TABLE

![synapse_CREATE_VIEW.JPG](images/1d6dba64-7b0c-4a54-ba25-20853a8717f5.JPG)

![synapse_CREATE_VIEW.JPG](images/1f568ce2-9e68-40a0-abde-a615856f992a.JPG)

![synapseSQL_getdatafromview.JPG](images/bf97c3ef-dc0c-475f-b8fc-82f51eece6d8.JPG)

## 13.3 Visualisation within Synapse Analytics

![synapseSQL_visualise.JPG](images/7cf1ac8a-6baf-49cd-be75-f5ab7689a678.JPG)

![order_status_VIEW.JPG](images/8744a3d1-0112-4d8a-bef0-3aeace371c2c.JPG)

# 14 Serving our final data to the GOLD layer

We can see that we can access our SILVER layer data in ADLSGen2 storage from Synapse by using VIEWS. Once we are happy with our data, in order to serve the data to the GOLD layer, we have to create:

- a Database
- master key encryption
- database scoped credential
- external file format
- external data source
- external table

https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-tables-cetas#cetas-in-serverless-sql-pool

We can use CREATE EXTERNAL TABLE AS SELECT (CETAS) in a dedicated SQL pool or serverless SQL pool to complete the following tasks:

- Create an external table
- Export, in parallel, the results of a Transact-SQL SELECT statement to Azure Data Lake Storage Gen2 


```python
-- Set credentials
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<password>';
CREATE DATABASE SCOPED CREDENTIAL sjbarrieadmin WITH IDENTITY = 'Managed Identity';

-- Specify the source data format
CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- specify the location where we wish to send the data
CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://ecomdatastorage.dfs.core.windows.net/ecom-data/gold/',
    CREDENTIAL = sjbarrieadmin
);

-- create a table for the data we want to send to the gold layer
CREATE EXTERNAL TABLE gold.final_table WITH (
        LOCATION = 'FinalServing',
        DATA_SOURCE = goldlayer,
        FILE_FORMAT = extfileformat
) AS

--- data from our VIEW which was created from the SILVER layer data
SELECT * FROM gold.final2
```

You can access your credentials using:

    -- View credential details
    SELECT * FROM sys.database_credentials

## 14.1 Confirm parquet file written to ADLSGen2 GOLD layer

![gold_layer_sent.JPG](images/8b10ef2e-6f94-4f88-bf42-3fb05883047e.JPG)


## 14.2 Run SQL queries from within Azure Synapse on data held in ADLSGen2

![gold_access.JPG](images/18d068aa-37ea-44c1-86f2-55ff34c95372.JPG)


## 14.3 Access to gold layer via Serverless SQL endpoint

![serverless_SQL_endpoint.JPG](images/297a7d42-7e47-47c6-a2f8-794ca6856a68.JPG)
