# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop unwanted columns from the dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename columns and add ingestion date

# COMMAND ----------

constructor_add_ingestion_date_df = add_ingestion_date(constructor_dropped_df)

# COMMAND ----------

constructor_final_df = constructor_add_ingestion_date_df.withColumnRenamed("constructorId", "constructor_Id") \
                                                        .withColumnRenamed("constructorRef", "constructor_Ref") \
                                                        .withColumn("data_source", lit(v_data_source)) \
                                                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

