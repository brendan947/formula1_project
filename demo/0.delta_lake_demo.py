# Databricks notebook source
# MAGIC %sql
# MAGIC create database f1_demo
# MAGIC location "/mnt/formula1dl100/demo"

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/formula1dl100/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formula1dl100/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table f1_demo.results_external
# MAGIC using DELTA
# MAGIC location 'mnt/formula1dl100/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table f1_demo.results_external