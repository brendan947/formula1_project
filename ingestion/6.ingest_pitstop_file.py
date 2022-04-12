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
# MAGIC ###Read pitstop JSON file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import lit

# COMMAND ----------

pit_stop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pit_stop_df = spark.read \
.schema(pit_stop_schema) \
.option("multiline", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

pit_stop_add_ingestion_date_df = add_ingestion_date(pit_stop_df)

# COMMAND ----------

final_df = pit_stop_add_ingestion_date_df.withColumnRenamed("driverId", "driver_Id") \
                                         .withColumnRenamed("raceId", "race_Id") \
                                         .withColumn("data_source", lit(v_data_source)) \
                                         .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stop")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stop;

# COMMAND ----------

dbutils.notebook.exit("Success")