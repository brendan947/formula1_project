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
# MAGIC ###Read lap times csv file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times") 

# COMMAND ----------

lap_times_add_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

final_df = lap_times_add_ingestion_date_df.withColumnRenamed("driverId", "driver_Id") \
                                          .withColumnRenamed("raceId", "race_Id") \
                                          .withColumn("data_source", lit(v_data_source)) \
                                          .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the parquet file we just wrote back out so we can see results.  Comment this out
# MAGIC display(spark.read.parquet("/mnt/formula1dl100/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")