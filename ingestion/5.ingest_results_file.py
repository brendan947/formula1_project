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
# MAGIC ###Read results JSON file

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True),
])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename columns

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId", "result_Id") \
                               .withColumnRenamed("raceId", "race_Id") \
                               .withColumnRenamed("driverId", "driver_Id") \
                               .withColumnRenamed("constructorId", "constructor_Id") \
                               .withColumnRenamed("positionText", "position_Text") \
                               .withColumnRenamed("positionOrder", "position_Order") \
                               .withColumnRenamed("fastestLap", "fastest_lap") \
                               .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                               .withColumnRenamed("fastestLapSpeed", "fastest_lap_Speed") \
                               .withColumn("data_source", lit(v_data_source)) \
                               .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_add_ingestion_date = add_ingestion_date(results_renamed_df)

# COMMAND ----------

results_final_df = results_add_ingestion_date.drop("statusId")

# COMMAND ----------

results_final_df.write.mode("append").partitionBy("race_Id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results;

# COMMAND ----------

dbutils.notebook.exit("Success")