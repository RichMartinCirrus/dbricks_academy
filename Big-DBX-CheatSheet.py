# Databricks notebook source
# MAGIC %md
# MAGIC #Markdown
# MAGIC 
# MAGIC #Header 1
# MAGIC ##Header 2
# MAGIC 
# MAGIC *bullet 1
# MAGIC *bullet 2
# MAGIC 
# MAGIC 1. Number 1
# MAGIC 2. Number 2

# COMMAND ----------

# MAGIC %md
# MAGIC # MAGIC COMMANDS
# MAGIC 
# MAGIC *%RUN - RUN A NOTEBOOK BY PASSING A PATH
# MAGIC 
# MAGIC *%MD - SET A CELL AS MARKDOWN
# MAGIC 
# MAGIC *%PYTHON, %SQL - DECLARE A CELL'S LANGUAGE PYTHON OR SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #Databricks Utilities
# MAGIC 
# MAGIC *dbutils.fs.ls("/databricks-datasets")
# MAGIC 
# MAGIC *dbutils.help()
# MAGIC 
# MAGIC dbutils.widgets.text("ICDVersion","")
# MAGIC dbutils.widgets.dropdown("Env", "DEV", ["QA","PRD","DEV","UAT"])
# MAGIC dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()

# COMMAND ----------

# MAGIC %md
# MAGIC # Set up Data Mount

# COMMAND ----------

def setupDataMount(_mount): 
  
  config = {"fs.azure.account.auth.type": "OAuth",
     "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
     "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope = "dlakekeyvault", key = reqMounts[_mount]['KeyVault']['SP']),
     "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope = 'dlakekeyvault', key = reqMounts[_mount]['KeyVault']['Secret']),
     "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+ dbutils.secrets.get(scope = 'dlakekeyvault', key = "cirrus-tennant")+"/oauth2/token",
     "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

  dbutils.fs.mount(
    source = "abfss://uademo@cirdlake.dfs.core.windows.net/",
    mount_point = "/mnt/uademo",
    extra_configs = config)

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Context

# COMMAND ----------

# MAGIC %python
# MAGIC import json, sys
# MAGIC context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
# MAGIC context = json.loads(context_str)
# MAGIC 
# MAGIC for value in context:
# MAGIC     print(value)

# COMMAND ----------

# MAGIC %md
# MAGIC #display()
# MAGIC When running SQL queries from cells, results will always be displayed in a rendered tabular format.
# MAGIC 
# MAGIC When we have tabular data returned by a Python cell, we can call display to get the same type of preview.
# MAGIC 
# MAGIC Here, we'll wrap the previous list command on our file system with display

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Basics

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ########################################################## CREATE
# MAGIC --CREATE TABLE IF NOT EXISTS students 
# MAGIC  -- (id INT, name STRING, value DOUBLE)
# MAGIC  
# MAGIC -- ########################################################## CREATE EXTERNAL TABLE
# MAGIC -- An external table is a table that references an external storage path by using a LOCATION clause.
# MAGIC -- The storage path should be contained in an existing external location to which you have been granted access.
# MAGIC -- Dropping an external table, will not drop the external files.
# MAGIC 
# MAGIC -- CREATE OR REPLACE TABLE external_table 
# MAGIC -- LOCATION '${da.paths.working_dir}/external_table' 
# MAGIC -- AS SELECT * FROM temp_delays;
# MAGIC  
# MAGIC   
# MAGIC -- ########################################################## INSERT
# MAGIC 
# MAGIC ---- ################## NOTE, COMMITS OCCUR WHEN JOB COMPLETES
# MAGIC INSERT INTO students VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO students VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO students VALUES (3, "Elia", 3.3);
# MAGIC 
# MAGIC --- alternatively
# MAGIC INSERT INTO students
# MAGIC VALUES 
# MAGIC   (4, "Ted", 4.7),
# MAGIC   (5, "Tiffany", 5.5),
# MAGIC   (6, "Vini", 6.3)
# MAGIC   
# MAGIC ----- ####################################################### DELETE  
# MAGIC DELETE FROM students 
# MAGIC WHERE value > 6  
# MAGIC 
# MAGIC ----- ####################################################### UPDATE
# MAGIC UPDATE students 
# MAGIC SET value = value + 1
# MAGIC WHERE name LIKE "O%"
# MAGIC 
# MAGIC   
# MAGIC -- ########################################################## QUERY
# MAGIC select * from students
# MAGIC 
# MAGIC -- ################################## DROP TABLE
# MAGIC DROP TABLE students
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC -- ################################## SHOW TABLES
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta MERGE (UPSERT)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
# MAGIC   (2, "Omar", 15.2, "update"),
# MAGIC   (3, "", null, "delete"),
# MAGIC   (7, "Blue", 7.7, "insert"),
# MAGIC   (11, "Diya", 8.8, "update");
# MAGIC   
# MAGIC SELECT * FROM updates;
# MAGIC 
# MAGIC 
# MAGIC MERGE INTO students b
# MAGIC USING updates u
# MAGIC ON b.id=u.id
# MAGIC WHEN MATCHED AND u.type = "update"
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND u.type = "delete"
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND u.type = "insert"
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from students

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta - MANAGING TABLES
# MAGIC 
# MAGIC ##### DELETING RECORDS IN A DELTA TABLE DOES NOT PERMANENTLY DELETE THE DATA, IT JUST REMOVES IT FROM THE LATEST VERSION.
# MAGIC - You can always restore a table to a version using RESTORE TABLE {TABLE NAME} TO VERSION AS OF {VERSION NUMBER}
# MAGIC ##### TO PERMANENTLY DELETE DATA FROM DELTA TABLES, YOU MUST USE !VACUUM!.

# COMMAND ----------

# MAGIC %sql
# MAGIC -------------------------------------------------------------------- ##### DESCRIBING TABLES
# MAGIC DESCRIBE EXTENDED students --table location, name, column names and types
# MAGIC DESCRIBE DETAIL students -- similar to extended but without column specs
# MAGIC DESCRIBE HISTORY students -- show transaction history from delta log.
# MAGIC 
# MAGIC ######################################################### LISTING FILES AND EXAMINING LOG FILES
# MAGIC %python
# MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students")) ###list all files supporting a delta table
# MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log")) ## list all files in delta log.
# MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`")) ## query contents of a given delta log
# MAGIC 
# MAGIC ----------------------------------------- OPTIMIZATION - USING OPTIMIZE KEYWORD, COMBINE FILES INTO AN OPTIMAL SIZE, OPTIONALLY SPECIFY INDEX USING ZORDER
# MAGIC %sql
# MAGIC OPTIMIZE students
# MAGIC ZORDER BY id
# MAGIC 
# MAGIC ----------------------------------------- TIME TRAVEL
# MAGIC select * from students VERSION AS OF 3 -- show me table as of version 3

# COMMAND ----------

# MAGIC %md
# MAGIC #Delta - VACUUM OPERATION
# MAGIC - Delta by default will prevent you from deleting files less than 7 days old

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM students RETAIN 0 HOURS -- BY DEFAULT, THIS WILL NOT GO THROUGH
# MAGIC 
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false; -- DISABLE DBX DEFAULT TRANS SETTING OF ONLY ALLOWING VACUUM OF 7 DAYS
# MAGIC SET spark.databricks.delta.vacuum.logging.enabled = true; -- ENSURE DBX LOGGING OF VACUUM OPS IS ENABLED
# MAGIC 
# MAGIC VACUUM students RETAIN 0 HOURS DRY RUN -- show me all the files that would be deleted if I executed this operation.

# COMMAND ----------

# MAGIC %md
# MAGIC # VIEWS AND CTES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ####################### CREATING VIEWS……….DOESNT CREATE FILES, LOGIC STORED IN METASTORE
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW view_delays_ABQ_LAX AS
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM external_table 
# MAGIC WHERE origin = 'ABQ' AND destination = 'LAX';
# MAGIC 
# MAGIC SELECT * FROM view_delays_ABQ_LAX;
# MAGIC 
# MAGIC 
# MAGIC -- ######################## GLOBAL TEMP VIEW - It is added to the global_temp database that exists on the cluster. As long as the cluster is running, this database persists, and any notebooks attached to the cluster can access its global temporary views
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW global_temp_view_distance 
# MAGIC AS SELECT * FROM external_table WHERE distance > 1000;
# MAGIC 
# MAGIC SELECT * FROM global_temp.global_temp_view_distance;
# MAGIC 
# MAGIC 
# MAGIC -- ######################## TEMP VIEW WITH OPTIONS- CTAS WONT ALLOW OPTIONS
# MAGIC -- THIS ALLOWS GREATER FLEXIBILITY IN CREATING THE FINAL TABLE (SELECTING COLUMNS, RENAMING COLUMNS, ETC)
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW sales_tmp_vw
# MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path "${da.paths.datasets}/raw/sales-csv",
# MAGIC   header "true",
# MAGIC   delimiter "|"
# MAGIC );
# MAGIC 
# MAGIC CREATE TABLE sales_delta AS
# MAGIC   SELECT * FROM sales_tmp_vw;
# MAGIC   
# MAGIC SELECT * FROM sales_delta
# MAGIC 
# MAGIC 
# MAGIC -- ######################## CTE EXAMPLE
# MAGIC 
# MAGIC WITH flight_delays(
# MAGIC   total_delay_time,
# MAGIC   origin_airport,
# MAGIC   destination_airport
# MAGIC ) AS (
# MAGIC   SELECT
# MAGIC     delay,
# MAGIC     origin,
# MAGIC     destination
# MAGIC   FROM
# MAGIC     external_table
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   flight_delays
# MAGIC WHERE
# MAGIC   total_delay_time > 120
# MAGIC   AND origin_airport = "ATL"
# MAGIC   AND destination_airport = "DEN";

# COMMAND ----------

# MAGIC %md
# MAGIC # SPARK DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ################# CREATE DATABASE
# MAGIC %sql
# MAGIC CREATE DATABASE ${da.db_name}
# MAGIC 
# MAGIC 
# MAGIC -- ########### CREATE DATABASE FROM FILE SET
# MAGIC CREATE DATABASE ${da.db_name} LOCATION '${da.paths.working_dir}/${da.db_name}';
# MAGIC 
# MAGIC -- ############### USE DATABASE
# MAGIC USE ${da.db_name};
# MAGIC 
# MAGIC 
# MAGIC -- ############### CREATE TABLE (MANAGED- DATA AND METADATA IS MANAGED)
# MAGIC -- ############### CANT DEFINE A CUSTOM SCHEMA, SCHEMA IS INFERRED
# MAGIC CREATE TABLE weather_managed AS 
# MAGIC SELECT * 
# MAGIC FROM parquet.`${da.paths.working_dir}/weather`
# MAGIC 
# MAGIC -- ############### CREATE TABLE (EXTERNAL - DATA REMAINS DURING DROPS BUT METADATA IS MANAGED)
# MAGIC CREATE TABLE  weather_external
# MAGIC LOCATION "${da.paths.working_dir}/lab/external"
# MAGIC AS SELECT * 
# MAGIC FROM parquet.`${da.paths.working_dir}/weather`
# MAGIC 
# MAGIC 
# MAGIC --################################### CREATE TABLE (EXTERNAL - USING OPTIONS)
# MAGIC 
# MAGIC CREATE TABLE sales_csv
# MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = "|"
# MAGIC )
# MAGIC LOCATION "${da.paths.working_dir}/sales-csv"
# MAGIC 
# MAGIC --################################### CREATE TABLE WITH SCHEMA AND GENERATED COLUMNS
# MAGIC -- ### (Because date is a generated column, if we write to purchase_dates without providing values for the date column, Delta Lake automatically computes them.)
# MAGIC CREATE OR REPLACE TABLE purchase_dates (
# MAGIC   id STRING, 
# MAGIC   transaction_timestamp STRING, 
# MAGIC   price STRING,
# MAGIC   date DATE GENERATED ALWAYS AS (
# MAGIC     cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
# MAGIC     COMMENT "generated based on `transactions_timestamp` column")
# MAGIC     
# MAGIC     
# MAGIC     
# MAGIC -- ################################## CREATE TABLE WITH PARTITION, COMMENT, LOCATION
# MAGIC -- ################################## USING FILE METADATA(), CURRENT_TIMESTAMP()
# MAGIC 
# MAGIC 
# MAGIC -- !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! NOTE ON PARTITIONING
# MAGIC -- Partitioning is shown here primarily to demonstrate syntax and impact. Most Delta Lake tables (especially small-to-medium sized data) will not benefit from partitioning. Because partitioning physically separates data files, this approach can result in a small files problem and prevent file compaction and efficient data skipping. The benefits observed in Hive or HDFS do not translate to Delta Lake, and you should consult with an experienced Delta Lake architect before partitioning tables.
# MAGIC 
# MAGIC -- As a best practice, you should default to non-partitioned tables for most use cases when working with Delta Lake.
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE users_pii
# MAGIC COMMENT "Contains PII"
# MAGIC LOCATION "${da.paths.working_dir}/tmp/users_pii"
# MAGIC PARTITIONED BY (first_touch_date)
# MAGIC AS
# MAGIC   SELECT *, 
# MAGIC     cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
# MAGIC     current_timestamp() updated,
# MAGIC     input_file_name() source_file
# MAGIC   FROM parquet.`${da.paths.datasets}/raw/users-historical/`;
# MAGIC   
# MAGIC SELECT * FROM users_pii;    
# MAGIC     
# MAGIC     
# MAGIC 
# MAGIC -- ################################# CREATE TABLE FROM DBMS
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users_jdbc;
# MAGIC 
# MAGIC CREATE TABLE users_jdbc
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlite:/${da.username}_ecommerce.db",
# MAGIC   dbtable "users"
# MAGIC )
# MAGIC 
# MAGIC SELECT * FROM users_jdbc
# MAGIC 
# MAGIC DESCRIBE EXTENDED users_jdbc --(While the table is listed as MANAGED, listing the contents of the specified location confirms that no data is being persisted locally.)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC -- ############ SHOW TABLES
# MAGIC 
# MAGIC SHOW TABLES
# MAGIC 
# MAGIC -- ############# DESCRIBE EXTENDED TABLE DETAILS
# MAGIC DESCRIBE EXTENDED weather_managed;
# MAGIC 
# MAGIC 
# MAGIC -- ############ DROP DATABASE AND ALL CHILD OBJECTS (CASCADE)
# MAGIC DROP DATABASE ${da.db_name} CASCADE;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC -- #################### CREATING VIEWS (VIEW = PERSISTS LOGIC IN METASTORE, TEMP VIEW, AVAIL ONLY FOR NOTEBOOK SESSION, GLOBAL TEMP VIEW, AVAIL CLUSTER WIDE, WONT SHOW UP IN SHOW TABLES)
# MAGIC CREATE VIEW celsius
# MAGIC AS (SELECT *
# MAGIC   FROM weather_managed
# MAGIC   WHERE UNIT = "C")
# MAGIC   
# MAGIC     
# MAGIC   CREATE TEMP VIEW celsius_temp
# MAGIC AS (SELECT *
# MAGIC   FROM weather_managed
# MAGIC   WHERE UNIT = "C")
# MAGIC   
# MAGIC   
# MAGIC   CREATE GLOBAL TEMP VIEW celsius_global
# MAGIC AS (SELECT *
# MAGIC   FROM weather_managed
# MAGIC   WHERE UNIT = "C")
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --################################### FORCE A REFRESH OF A TABLE (Note that refreshing our table will invalidate our cache, meaning that we'll need to rescan our original data source and pull all data back into memory.
# MAGIC -- For very large datasets, this may take a significant amount of time.)
# MAGIC 
# MAGIC REFRESH TABLE sales_csv

# COMMAND ----------

# MAGIC %md
# MAGIC # ADDING CONSTRAINTS
# MAGIC 
# MAGIC Because Delta Lake enforces schema on write, Databricks can support standard SQL constraint management clauses to ensure the quality and integrity of data added to a table.
# MAGIC 
# MAGIC Databricks currently support two types of constraints:
# MAGIC 
# MAGIC NOT NULL constraints
# MAGIC CHECK constraints
# MAGIC In both cases, you must ensure that no data violating the constraint is already in the table prior to defining the constraint. Once a constraint has been added to a table, data violating the constraint will result in write failure

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');
# MAGIC -- ALTER TABLE purchase_dates ADD CONSTRAINT {constraint name} {CHECK, NOT NULL} ({constraint logic});

# COMMAND ----------

# MAGIC %md
# MAGIC # CLONING TABLES
# MAGIC 
# MAGIC Delta Lake has two options for efficiently copying Delta Lake tables.
# MAGIC 
# MAGIC DEEP CLONE fully copies data and metadata from a source table to a target. This copy occurs incrementally, so executing this command again can sync changes from the source to the target location.
# MAGIC 
# MAGIC 
# MAGIC If you wish to create a copy of a table quickly to test out applying changes without the risk of modifying the current table, SHALLOW CLONE can be a good option. Shallow clones just copy the Delta transaction logs, meaning that the data doesn't move

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE purchases_clone
# MAGIC DEEP CLONE purchases
# MAGIC 
# MAGIC 
# MAGIC -- If you wish to create a copy of a table quickly to test out applying changes without the risk of modifying the current table, SHALLOW CLONE can be a good option. Shallow clones just copy the Delta transaction logs, meaning that the data doesn't move
# MAGIC 
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE purchases_shallow_clone
# MAGIC SHALLOW CLONE purchases

# COMMAND ----------

# MAGIC %md
# MAGIC # MERGING

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled=true; -- ##### configures a setting to allow for generating columns when using a Delta Lake MERGE statement.
# MAGIC 
# MAGIC MERGE INTO purchase_dates a
# MAGIC USING purchases b
# MAGIC ON a.id = b.id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC #SPARK SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC --################################### Querying a file with spark sql:
# MAGIC SELECT * FROM file_format.`/path/to/file`
# MAGIC -- EXAMPLE (SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/001.json`)
# MAGIC 
# MAGIC --###################################Query a directory:
# MAGIC SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka`
# MAGIC 
# MAGIC --###################################Create view to point to files:
# MAGIC CREATE OR REPLACE TEMP VIEW events_temp_view
# MAGIC AS SELECT * FROM json.`${da.paths.datasets}/raw/events-kafka/`;
# MAGIC 
# MAGIC SELECT * FROM events_temp_view
# MAGIC 
# MAGIC --###################################Extract Text Files as Raw Strings using text (--loads each row as a string named value)
# MAGIC SELECT * FROM text.`${da.paths.datasets}/raw/events-kafka/`
# MAGIC 
# MAGIC --###################################Read as binary: (Specifically, the fields created will indicate the path, modificationTime, length, and content.)
# MAGIC SELECT * FROM binaryFile.`${da.paths.datasets}/raw/events-kafka/`

# COMMAND ----------

# MAGIC %md
# MAGIC # READING FROM SQL DB

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users_jdbc;
# MAGIC 
# MAGIC CREATE TABLE users_jdbc
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlite:/${da.username}_ecommerce.db",
# MAGIC   dbtable "users"
# MAGIC )
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM users_jdbc
# MAGIC 
# MAGIC 
# MAGIC DESCRIBE EXTENDED users_jdbc --(While the table is listed as MANAGED, listing the contents of the specified location confirms that no data is being persisted locally.)

# COMMAND ----------

# MAGIC %md
# MAGIC # PYTHON UTILITIES

# COMMAND ----------

-- ############## RETURN TABLE LOCATION FROM DESCRIBE EXTENDED COMMAND
def getTableLocation(tableName):
    return spark.sql(f"DESCRIBE EXTENDED {tableName}").select("data_type").filter("col_name = 'Location'").first()[0]


-- ############## DISPLAY FILES IN A PATH
%python
files = dbutils.fs.ls(managedTablePath)
display(files)


-- ############## WRITE TO CSV

%python
(spark.table("sales_csv")
      .write.mode("append")
      .format("csv")
      .save(f"{DA.paths.working_dir}/sales-csv"))
