# End-to-End Data Engineering Project

## Project Overview:

This project demonstrates an end-to-end data engineering workflow using various Azure services. Data from an on-premises MS SQL Server database is ingested into Azure Data Lake Gen2 using Azure Data Factory, transformed using Azure Databricks, and finally loaded into Azure Synapse Analytics for analysis and reporting with Microsoft Power BI.

![](https://github.com/RajkumarManala1/Azure-Data-Engineering-Project/blob/main/Screenshots%20of%20resources/Project.jpg?raw=true)

### Azure Resources Used

1. Azure Data Factory (V2): Data-Ingestion-from-onpremises
2. Azure Key Vault: KeyforDBs
3. Azure Storage Account: onpremisesstoredincloud
4. Azure Synapse Workspace: synapseforingestion
5. Azure Databricks: Transformdata
6. Power BI: Data Visulazation


  ![](https://github.com/RajkumarManala1/Azure-Data-Engineering-Project/blob/main/Screenshots%20of%20resources/resources.png?raw=true)
  
---
### Workflow Details:

### Azure Data Factory (ADF)

Azure Data Factory (ADF) played a critical role in orchestrating the data pipeline, responsible for moving data from an on-premises MS SQL Server to Azure Data Lake Gen2.

#### Key Steps:

##### 1. Connecting to On-Premises SQL Server:

A Self-hosted Integration Runtime was installed using Microsoft Integration Runtime Configuration Manager, enabling secure connectivity between ADF and the on-premises SQL Server.
Database credentials (username and password) were securely stored in Azure Key Vault, and accessed within the ADF pipeline.

##### 2. Data Ingestion Pipeline:

- Copy Activity was used to copy data from the on-premises SQL Server to Azure Data Lake Gen2 in Parquet format.

- A Lookup Activity was used to fetch metadata (schema and table names) from the SQL Server using the following query:

  ```sql
  SELECT
      s.name AS SchemaName,
      t.name AS TableName
  FROM 
      sys.tables t
  INNER JOIN 
      sys.schemas s ON t.schema_id = s.schema_id
  WHERE 
      s.name = 'SalesLT';
  ```

##### 3. Dynamic Folder Structure in Data Lake:

- Data was stored in Azure Data Lake Gen2 using a dynamic folder structure:

   `bronze/Schema/Tablename/Tablename.parquet`

- Dynamic expressions to define the folder paths:

  `@{concat(dataset().schemaname, '/', dataset().tablename)}`
  
  `@{concat(dataset().tablename, '.parquet')}`

##### 4. ELT Process: 

- ADF pipeline followed the ELT (Extract, Load, Transform) methodology. Data from SQL Server was first extracted and loaded into the bronze layer in Azure Data Lake, followed by transformations in Azure Databricks.

  ![](https://github.com/RajkumarManala1/Azure-Data-Engineering-Project/blob/main/Screenshots%20of%20resources/adf3.png?raw=true)

### Azure Databricks
Azure Databricks was used to perform data transformations on the ingested data from Azure Data Lake Gen2. The data was processed in multiple layers: Bronze, Silver, and Gold.

#### Key Steps:

##### 1. Apache Spark Cluster Setup:

An Apache Spark Cluster was created to handle distributed data processing. Notebooks were used for code execution.

##### 2. Mounting Data from Azure Data Lake:

- The following code was used to mount bronze, silver, and gold layers in Databricks:

  ```python
  configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
  }
  
  dbutils.fs.mount(
    source = "abfss://bronze@onpremisesstoredincloud.dfs.core.windows.net/",
    mount_point = "/mnt/bronze",
    extra_configs = configs)
  
  dbutils.fs.mount(
    source = "abfss://silver@onpremisesstoredincloud.dfs.core.windows.net/",
    mount_point = "/mnt/silver",
    extra_configs = configs)
  
  dbutils.fs.mount(
    source = "abfss://gold@onpremisesstoredincloud.dfs.core.windows.net/",
    mount_point = "/mnt/gold",
    extra_configs = configs)
  ```

##### 3. Data Transformations:

- Using PySpark, transformations such as date formatting and column renaming were applied. Example for transforming dates:

  ```python
  from pyspark.sql.functions import from_utc_timestamp, date_format
  from pyspark.sql.types import TimestampType
  df = df.withColumn("ModifiedDate", date_format(from_utc_timestamp(df["ModifiedDate"].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
  display(df)
  ```

 ###### Before Table

  ![](https://github.com/RajkumarManala1/Azure-Data-Engineering-Project/blob/main/Screenshots%20of%20resources/before2.png?raw=true)

 ###### After Transformation Table
  ![](https://github.com/RajkumarManala1/Azure-Data-Engineering-Project/blob/main/Screenshots%20of%20resources/after2.png?raw=true)
  
- Data was transformed from bronze to silver layer with the following code:

  ```python
  for i in table_name:
    path = '/mnt/bronze/SalesLT/' + i + '/' + i + '.parquet'
    df = spark.read.format('parquet').load(path)
    column = df.columns

    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))

    output_path = '/mnt/silver/SalesLT/' + i + '/'
    df.write.format('delta').mode("overwrite").save(output_path)
  ```
  
##### 4. Final Transformation in Gold Layer:

- The final transformation involved renaming columns to follow the snake_case convention:

  ```python
  for name in table_name:
    path = '/mnt/silver/SalesLT/' + name
    df = spark.read.format('delta').load(path)
    
    column_names = df.columns
    for old_col_name in column_names:
        new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i - 1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")
        df = df.withColumnRenamed(old_col_name, new_col_name)
    
    output_path = '/mnt/gold/SalesLT/' + name + '/'
    df.write.format('delta').mode("overwrite").save(output_path)
  ```
- This ensured consistent column naming conventions, improving data readability and usability.

###### Before inserting underscores

  ![](https://github.com/RajkumarManala1/Azure-Data-Engineering-Project/blob/main/Screenshots%20of%20resources/before1.png?raw=true)

###### After inserting underscores

  ![](https://github.com/RajkumarManala1/Azure-Data-Engineering-Project/blob/main/Screenshots%20of%20resources/after1.png?raw=true)
