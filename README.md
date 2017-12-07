## ETL : Parquet Conversion

### Pre-Requisites

* Setting up IAM Permissions for AWS Glue [[1]](http://docs.aws.amazon.com/glue/latest/dg/getting-started-access.html)
  * 3 Managed Policies: AWSGlueConsoleFullAccess, AWSGlueServiceNotebookRole, AWSGlueServiceRole 
* Run Crawler on S3 location "s3://neilawspublic/dataset228" to create the source database and table 

### Steps

* Open Zeppelin notebook and import notebook 'GlueNotebook1-ParquetConversion.json'

#### GlueNotebook1-ParquetConversion.json : Demonstrates Parquet Conversion of a CSV Dataset in S3 partitioned by columns

* Uses Spark Dataframes
* Uses boto3 to access Glue catalog
* Parses CSV files by File header
* Repartitions output Parquet data by chosen columns

## ETL : MySQL to Redshift 

### Pre-Requisites 

* Install MySQL Workbench [[2]](https://dev.mysql.com/downloads/) and SQL WorkBench/J [[3]](http://www.sql-workbench.net/downloads.html). 
* Create a S3 VPC Endpoint. [[4]](http://docs.aws.amazon.com/AmazonVPC/latest/UserGuide/vpc-endpoints.html) 

#### MySQL 

* Launch a MySQL RDS instance.
* Connect to it and create the schema objects and load the data into the MySQL database. 
```
MySQL Script Location: https://github.com/nmukerje/glue/blob/master/scripts/salesdb.sql
$> mysql -f -u mysqldb -h <mysqldb-host>.rds.amazonaws.com  -p mysqldb < salesdb.sql
```

#### Redshift 
* Launch a Redshift instance in the same VPC.
* Create the Schema objects 
```
Redshift script location [https://github.com/nmukerje/glue/blob/master/scripts/redshift-schema.sql]
 
Use SQLWorkbench/J to execute the above script in Redshift. Redshift JDBC driver is here:[http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html]
```
### Steps

* Create Database connections to the MySQL Database. 
```
* Connection name : demo-mysql
* Connection type : JDBC
* JDBC URL : jdbc:mysql://<mysqldb-host>:3306/<database-name>
* Username : <database username>
* Password : <database password>
* VPC : <select VPC>
* Subnet : <select Subnet>
* Security Group : <select Security Group>
```
* Create Database connections to the Redshift Database. 
```
* Connection name : demo-redshift
* Connection type : JDBC
* JDBC URL : jdbc:redshift://<redshift-host>:5439/<database-name>
* Username : <database username>
* Password : <database password>
* VPC : <select VPC>
* Subnet : <select Subnet>
* Security Group : <select Security Group>
```
* Create Crawler to the MySQL Database. 
```
* Crawler name : demo-mysql
* IAM role : <Glue Default Role>
* Data store : JDBC
* Connection : demo-mysql
* Include path : salesdb/%
* Frequency : Run on demand
* Database : Add Database -> demo-mysql
```
* Create Crawler to the Redshift Database
```
* Crawler name : demo-mysql
* IAM role : <Glue Default Role>
* Data store : JDBC
* Connection : demo-mysql
* Include path : salesdb/%
* Frequency : Run on demand
* Database : Add Database -> demo-mysql
```
* Create Crawler to the Redshift Database. 
```
* Crawler name : demo-redshift
* IAM role : <Glue Default Role>
* Data store : JDBC
* Connection : demo-redshift
* Include path : redshiftdb/public/%
* Frequency : Run on demand
* Database : Add Database -> demo-redshift
```
* Run Crawlers on the MySQL and Redshift Databases.
* Crawlers should create 7 tables for MySQL and 5 tables for Redshift in the respective databases. 
* Open the Zeppelin notebooks below:

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
