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

# MAGIC %md
# MAGIC ###Read qualifying JSON files

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

qualifying_add_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

final_df = qualifying_add_ingestion_date_df.withColumnRenamed("driverId", "driver_Id") \
                                           .withColumnRenamed("raceId", "race_Id") \
                                           .withColumnRenamed("qualifyId", "qualify_Id") \
                                           .withColumnRenamed("constructorId", "constructor_Id") \
                                           .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualify")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualify;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the parquet file we just wrote back out so we can see results.  Comment this out
# MAGIC display(spark.read.parquet("/mnt/formula1dl100/processed/qualfying"))

# COMMAND ----------

dbutils.notebook.exit("Success")