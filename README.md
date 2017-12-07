# Glue Zeppelin Notebooks

## Demonstrates Parquet Conversion of a CSV Dataset in S3 partitioned by columns

* Uses Spark Dataframes
* Uses boto3 to access Glue catalog
* Parses CSV files by File header
* Repartitions output Parquet data by chosen columns
