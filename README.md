# Glue Zeppelin Notebooks

### GlueNotebook1-ParquetConversion.json : Demonstrates Parquet Conversion of a CSV Dataset in S3 partitioned by columns

* Uses Spark Dataframes
* Uses boto3 to access Glue catalog
* Parses CSV files by File header
* Repartitions output Parquet data by chosen columns

### GlueNotebook1-ParquetConversion.json : Demonstrates a MySQL to Redshift load

* Uses Glue DynamicFrames
* Uses joins to denormalize MySQL tables
* Uses User Defined Functions for computed columns
