import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import ApplyMapping
from awsglue.transforms import SelectFields
from  pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import DecimalType
from awsglue.context import DynamicFrame

args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

## Glue database references
source_database="mysqldb"
target_database="redshift"

## Glue table references
source_tables= ["auroradb_sales_order_detail_af48b305", "auroradb_sales_order_cdacc40b"]
target_tables= ["redshiftdb4_public_sales_order_fact"]

spark = SparkContext()
glueContext = GlueContext(spark)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
print args['JOB_NAME']+" BEGIN..."
## @type: DataSource
## @args: [database = "mysqldb", table_name = "auroradb_sales_order_detail_af48b305", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = source_database, table_name = source_tables[0], transformation_ctx = "datasource0")
print "Rows read from mysql table: +"+source_tables[0]+" : "+str(datasource0.count())
datasource0.printSchema()

datasource1 = glueContext.create_dynamic_frame.from_catalog(database = source_database, table_name = source_tables[1], transformation_ctx = "datasource1")
print "Rows read from mysql table: +"+source_tables[1]+" : "+str(datasource1.count())
datasource1.printSchema()

#DynamicFrame join
datasource2=datasource1.join( ["ORDER_ID"],["ORDER_ID"], datasource0, transformation_ctx = "join")
#Spark DataFrame join
#df2=datasource1.toDF().join(datasource0.toDF(),"ORDER_ID")
#datasource2_1=DynamicFrame.fromDF(df2, glueContext,'datasource2_1')

print " Rows after Join transform: "+str(datasource2.count())
datasource2.printSchema()

## Adding computed columns
multiply_udf = UserDefinedFunction(lambda x,y : x * y, DecimalType(10,2))
compute_profit_udf = UserDefinedFunction(lambda x,y,z : x * (y-z), DecimalType(10,2))

df1=datasource2.toDF().withColumn("EXTENDED_PRICE",multiply_udf("QUANTITY","UNIT_PRICE")).withColumn("PROFIT",compute_profit_udf("QUANTITY","UNIT_PRICE","SUPPLY_COST"))
datasource3=DynamicFrame.fromDF(df1, glueContext,'datasource3')
df1.printSchema()

## @type: ApplyMapping
## @args: [mapping = [("discount", "decimal(10,2)", "discount", "decimal(10,2)"), ("unit_price", "decimal(10,2)", "unit_price", "decimal(10,2)"), ("tax", "decimal(10,2)", "tax", "decimal(10,2)"), ("supply_cost", "decimal(10,2)", "supply_cost", "decimal(10,2)"), ("product_id", "decimal(10,2)", "product_id", "decimal(10,2)"), ("quantity", "decimal(10,2)", "quantity", "int"), ("line_id", "decimal(10,2)", "line_id", "int"), ("line_number", "decimal(10,2)", "line_number", "int"), ("order_id", "decimal(10,2)", "order_id", "int")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource3, mappings = [("DISCOUNT", "decimal(10,2)", "discount", "decimal(10,2)"), ("UNIT_PRICE", "decimal(10,2)", "unit_price", "decimal(10,2)"), ("TAX", "decimal(10,2)", "tax", "decimal(10,2)"), ("SUPPLY_COST", "decimal(10,2)", "supply_cost", "decimal(10,2)"), ("PRODUCT_ID", "int", "product_id", "int"), ("QUANTITY", "int", "quantity", "int"), ("LINE_ID", "int", "line_id", "int"), ("LINE_NUMBER", "int", "line_number", "int"), ("ORDER_DATE", "timestamp", "order_date", "timestamp"), ("SHIP_MODE", "string", "ship_mode", "string"), ("SITE_ID", "double", "site_id", "int"), ("PROFIT", "decimal(10,2)", "profit", "decimal(10,2)"),("EXTENDED_PRICE", "decimal(10,2)", "extended_price", "decimal(10,2)"),("ORDER_ID", "int", "order_id", "int")], transformation_ctx = "applymapping1")
print "After ApplyMapping :"
applymapping1.printSchema()
applymapping1.toDF().show(5)

## @type: SelectFields
## @args: [paths = ["quantity", "discount", "tax", "unit_price", "line_id", "date_key", "order_date", "extended_price", "ship_mode", "line_number", "product_id", "site_id", "supply_cost", "order_id", "profit"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["quantity", "discount", "tax", "unit_price", "line_id", "date_key", "order_date", "extended_price", "ship_mode", "line_number", "product_id", "site_id", "supply_cost", "order_id", "profit"], transformation_ctx = "selectfields2")
print "After SelectFields :"
selectfields2.toDF().show(5)

## @type: DataSink
## @args: [database = "redshift", table_name = "redshiftdb4_public_sales_order_fact", redshift_tmp_dir = TempDir, transformation_ctx = "datasink3"]
## @return: datasink3
## @inputs: [frame = selectfields2]
datasink3 = glueContext.write_dynamic_frame.from_catalog(frame = selectfields2, database = target_database, table_name = target_tables[0], redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink3")
print "Rows inserted into Redshift table: "+target_tables[0]+" : "+str(datasink3.count())
print args['JOB_NAME']+" END..."
job.commit()
