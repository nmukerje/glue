import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.transforms import SelectFields
from awsglue.dynamicframe import DynamicFrame

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "demo-mysql", table_name = "salesdb_product", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "demo-mysql", table_name = "salesdb_product", transformation_ctx = "datasource0")
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "demo-mysql", table_name = "salesdb_product_category", transformation_ctx = "datasource1")

datasource3=datasource0.join( ["CATEGORY_ID"],["CATEGORY_ID"], datasource1, transformation_ctx = "join")
print "Joined Rows : "+str(datasource3.count())
datasource3.printSchema()
## @type: ApplyMapping
## @args: [mapping = [("name", "string", "name", "string"), ("unit_price", "decimal(10,2)", "unit_price", "decimal(10,2)"), ("product_id", "double", "product_id", "int"), ("quantity_per_unit", "double", "quantity_per_unit", "int"), ("category_id", "double", "category_id", "int"), ("supplier_id", "double", "supplier_id", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource3]
applymapping1 = ApplyMapping.apply(frame = datasource3, mappings = [("NAME", "string", "name", "string"), ("UNIT_PRICE", "decimal(10,2)", "unit_price", "decimal(10,2)"), ("PRODUCT_ID", "int", "product_id", "int"), ("QUANTITY_PER_UNIT", "int", "quantity_per_unit", "int"), ("CATEGORY_ID", "int", "category_id", "int"), ("SUPPLIER_ID", "int", "supplier_id", "int"), ("CATEGORY_NAME", "string", "category_name", "string"), ("DESCRIPTION", "string", "description", "string"), ("IMAGE_URL", "string", "image_url", "string")], transformation_ctx = "applymapping1")
#applymapping1.toDF().show()
## @type: SelectFields
## @args: [paths = ["category_name", "category_id", "image_url", "product_id", "name", "description", "quantity_per_unit", "unit_price", "supplier_id"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["category_name", "category_id", "image_url", "product_id", "name", "description", "quantity_per_unit", "unit_price", "supplier_id"], transformation_ctx = "selectfields2")
print "selectfields2 Rows : "+str(selectfields2.count())
selectfields2.toDF().show()
## @type: DataSink
## @args: [database = "demo-redshift", table_name = "redshiftdb_public_product_dim", redshift_tmp_dir = TempDir, transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = selectfields2]
datasink3 = glueContext.write_dynamic_frame.from_catalog(frame = selectfields2, database = "demo-redshift", table_name = "redshiftdb_public_product_dim", redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink3")
job.commit()
