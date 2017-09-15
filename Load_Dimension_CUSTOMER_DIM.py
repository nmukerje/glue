import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.transforms import SelectFields

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print args['JOB_NAME']+" START..."
## @type: DataSource
## @args: [database = "demo-mysql", table_name = "", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "demo-mysql", table_name = "salesdb_customer_site", transformation_ctx = "datasource0")
print "Rows read from mysql table: salesdb_customer_site : "+str(datasource0.count())
datasource0.printSchema()

datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "demo-mysql", table_name = "salesdb_customer", transformation_ctx = "datasource0")
print "Rows read from mysql table: salesdb_customer : "+str(datasource1.count())
datasource1.printSchema()

datasource2=datasource0.join( ["CUST_ID"],["CUST_ID"], datasource1, transformation_ctx = "join")
print " Rows after Join transform: "+str(datasource2.count())
datasource2.printSchema()
## @type: ApplyMapping
## @args: [mapping = [("country", "string", "country", "string"), ("address", "string", "address", "string"), ("phone", "string", "phone", "string"), ("city", "string", "city", "string"), ("state", "string", "state", "string"), ("site_id", "double", "site_id", "int"), ("cust_id", "double", "cust_id", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource2, mappings = [("COUNTRY", "string", "country", "string"), ("ADDRESS", "string", "address", "string"), ("PHONE", "string", "phone", "string"), ("CITY", "string", "city", "string"), ("STATE", "string", "state", "string"), ("SITE_ID", "double", "site_id", "int"), ("CUST_ID", "double", "cust_id", "int"),("MKTSEGMENT", "string", "mktsegment", "string"), ("NAME", "string", "name", "string")], transformation_ctx = "applymapping1")
applymapping1.toDF().show()
## @type: SelectFields
## @args: [paths = ["mktsegment", "country", "address", "city", "phone", "name", "site_id", "state", "cust_id"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["mktsegment", "country", "address", "city", "phone", "name", "site_id", "state", "cust_id"], transformation_ctx = "selectfields2")
selectfields2.toDF().show()
## @type: DataSink
## @args: [database = "redshift", table_name = "redshiftdb4_public_customer_dim", redshift_tmp_dir = TempDir, transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = selectfields2]
datasink3 = glueContext.write_dynamic_frame.from_catalog(frame = selectfields2, database = "redshift", table_name = "redshiftdb4_public_customer_dim", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink3")
datasink3.toDF().show()
print "Rows inserted into Redshift table: redshiftdb4_public_customer_dim : "+str(datasink3.count())
print args['JOB_NAME']+" END..."
job.commit()
