### ETL : Parquet Conversion

#### Pre-Requisites

* Setting up IAM Permissions for AWS Glue [[1]](http://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html)
  * 3 Managed Policies: AWSGlueConsoleFullAccess, AWSGlueServiceNotebookRole, AWSGlueServiceRole 
* Run Crawler on S3 location "s3://neilawspublic/dataset228" to create the source database and table 

#### GlueNotebook1-ParquetConversion.json : Demonstrates Parquet Conversion of a CSV Dataset in S3 partitioned by columns

* Uses Spark Dataframes
* Uses boto3 to access Glue catalog
* Parses CSV files by File header
* Repartitions output Parquet data by chosen columns

### ETL : MySQL to Redshift 

#### Pre-Requisites 

#### GlueNotebook2-MySQL2Redshift.json : Demonstrates a MySQL to Redshift load

* Uses Glue DynamicFrames
* Uses joins to denormalize MySQL tables
* Uses User Defined Functions for computed columns

#### GlueNotebook3-MySQL2Redshift-Incr : Demonstrates a MySQL to Redshift incremental load by timestamp

* Uses external checkpointing to DynamoDB
* Uses Spark SQL for transforms and computed columns
* Uses Spark Redshift package for inserting data to Redshift
* Vacuums Redshift table as a post load statement

#### GlueNotebook5-Redshift2S3.json : Demonstrates a Redshift to S3 unload

* Uses Spark Redshift package UNLOAD from Redshift 
* Writes data to parquet in S3 partitioned by chosen columns
