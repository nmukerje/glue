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
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getLogger("org").setLevel(log4j.Level.ERROR)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "demo-mysql", table_name = "salesdb_auroradb", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "demo-mysql", table_name = "salesdb_supplier", transformation_ctx = "datasource0")
print "MySQL Rows read : "+str(datasource0.count())
datasource0.toDF().show()
## @type: ApplyMapping
## @args: [mapping = [("country", "string", "country", "string"), ("address", "string", "address", "string"), ("name", "string", "name", "string"), ("state", "string", "state", "string"), ("supplier_id", "int", "supplier_id", "int"), ("city", "string", "city", "string"), ("phone", "string", "phone", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("COUNTRY", "string", "country", "string"), ("ADDRESS", "string", "address", "string"), ("NAME", "string", "name", "string"), ("STATE", "string", "state", "string"), ("SUPPLIER_ID", "int", "supplier_id", "int"), ("CITY", "string", "city", "string"), ("PHONE", "string", "phone", "string")], transformation_ctx = "applymapping1")
print "Source count datasource0: "+str(applymapping1.count())
applymapping1.toDF().show()
## @type: SelectFields
## @args: [paths = ["country", "address", "city", "phone", "name", "state", "supplier_id"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["country", "address", "city", "phone", "name", "state", "supplier_id"], transformation_ctx = "selectfields2")
print "Source count datasource0: "+str(selectfields2.count())
selectfields2.toDF().show()
## @type: DataSink
## @args: [database = "demo-redshift", table_name = "redshiftdb_public_supplier_dim", redshift_tmp_dir = TempDir, transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = selectfields2]
datasink3 = glueContext.write_dynamic_frame.from_catalog(frame = selectfields2, database = "demo-redshift", table_name = "redshiftdb_public_supplier_dim", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink3")
print "Redshift Rows inserted : "+str(datasink3.toDF().count())
job.commit()
