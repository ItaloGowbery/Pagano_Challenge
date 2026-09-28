# Databricks notebook source
display(
    spark.table("readings_gold")
    .filter(F.col("sensor") == "Temperature")
    .orderBy("minuto")
)

# COMMAND ----------

