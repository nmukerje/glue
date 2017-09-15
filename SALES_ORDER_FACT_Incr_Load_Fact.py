import sys, boto3, datetime
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import DecimalType,StringType
from awsglue.context import GlueContext, DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from awsglue.transforms import ApplyMapping,SelectFields
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])
print args['JOB_NAME']+" BEGIN..."

## Glue database references
source_database='mysqldb'
target_database='RedShift'

## Glue table references
source_table1='auroradb_sales_order_detail_af48b305'
source_table2='auroradb_sales_order_cdacc40b'
target_table_1='public.sales_order_fact'

## Function: Get Last Checkpoint from DynamoDB
def getLastCheckPoint (client, tablename):
   response = client.get_item(TableName='ETL_CHECKPOINT', Key={'TABLE_NAME':{'S':tablename}})
   checkpoint= response['Item']['CHECKPOINT']['S']
   return checkpoint
 
## Function: Update Checkpoint in DynamoDB
def updateCheckPoint (client,tablename, checkpoint, lastcheckpoint, status):
   response = client.put_item(TableName='ETL_CHECKPOINT', Item={'TABLE_NAME':{'S':tablename},
   	'CHECKPOINT':{'S':checkpoint}, 'LAST_CHECKPOINT':{'S':lastcheckpoint},'STATUS':{'S':status} })
   return True

glue=boto3.client(service_name='glue', region_name='us-east-1',
                       endpoint_url='https://glue.us-east-1.amazonaws.com')
redshiftConnection=glue.get_connection(Name=target_database)

jdbc_url=redshiftConnection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
username= redshiftConnection['Connection']['ConnectionProperties']['USERNAME']
password= redshiftConnection['Connection']['ConnectionProperties']['PASSWORD']

   
dynamodb = boto3.client('dynamodb',region_name='us-east-1')
checkpoint = getLastCheckPoint(dynamodb,'SALES_ORDER_FACT')
date_1 = datetime.datetime.strptime(checkpoint, "%Y-%m-%d %H:%M:%S")
newcheckpoint = date_1 + datetime.timedelta(days=1)
print("Last checkpoint : "+ str(checkpoint))
print("New checkpoint : "+ str(newcheckpoint))

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#Load the sales order detail table
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = source_database, table_name = source_table1, transformation_ctx = "datasource0")
print "Rows read from mysql table: "+source_table1+" : "+str(datasource0.count())
datasource0.printSchema()

#Load the sales order table
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = source_database, table_name = source_table2, transformation_ctx = "datasource1")
datasource1.printSchema()
df1=datasource1.toDF().filter(col("ORDER_DATE") == checkpoint)
print "Rows matching filter from mysql table: "+source_table2+": "+str(df1.count())
datasource2=DynamicFrame.fromDF(df1, glueContext,'datasource2')

#Join the source tables
datasource3=datasource2.join( ["ORDER_ID"],["ORDER_ID"], datasource0, transformation_ctx = "join")
print "Rows after Join transform: "+str(datasource3.count())
datasource3.printSchema()

datasource3.toDF().createOrReplaceTempView("tbl0") 
df1 = spark.sql("Select a.*, bround(a.QUANTITY*a.UNIT_PRICE,2) as EXTENDED_PRICE, \
bround(QUANTITY*(UNIT_PRICE-SUPPLY_COST) ,2) as PROFIT, \
DATE_FORMAT(ORDER_DATE,'yyyyMMdd') as DATE_KEY \
from (Select * from tbl0) a")
df1.show(5)
datasource4=DynamicFrame.fromDF(df1, glueContext,'datasource4')

applymapping1 = ApplyMapping.apply(frame = datasource4, mappings = [("DISCOUNT", "decimal(10,2)", "discount", "decimal(10,2)"), ("UNIT_PRICE", "decimal(10,2)", "unit_price", "decimal(10,2)"), ("TAX", "decimal(10,2)", "tax", "decimal(10,2)"), ("SUPPLY_COST", "decimal(10,2)", "supply_cost", "decimal(10,2)"), ("PRODUCT_ID", "int", "product_id", "int"), ("QUANTITY", "int", "quantity", "int"), ("LINE_ID", "int", "line_id", "int"), ("LINE_NUMBER", "int", "line_number", "int"), ("ORDER_DATE", "timestamp", "order_date", "timestamp"), ("SHIP_MODE", "string", "ship_mode", "string"), ("SITE_ID", "double", "site_id", "int"), ("PROFIT", "decimal(10,2)", "profit", "decimal(10,2)"),("EXTENDED_PRICE", "decimal(10,2)", "extended_price", "decimal(10,2)"),("DATE_KEY", "string", "date_key", "string"),("ORDER_ID", "int", "order_id", "int")], transformation_ctx = "applymapping1")
print "After ApplyMapping :"
applymapping1.printSchema()
applymapping1.toDF().show(5,False)

#selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["quantity", "discount", "tax", "unit_price", "line_id", "date_key", "order_date", "extended_price", "ship_mode", "line_number", "product_id", "site_id", "supply_cost", "order_id", "profit"], transformation_ctx = "selectfields2")

selectfields2 = SelectFields.apply(frame = applymapping1, paths = [ "order_id","site_id", "order_date", "date_key", "ship_mode", "line_id", "line_number","quantity", "product_id", "unit_price", "discount", "supply_cost", "tax", "extended_price", "profit" ], transformation_ctx = "selectfields2")

print "After SelectFields :"
selectfields2.toDF().select("date_key").show(5,False)
#selectfields2.toDF().show(5,False)

jdbc_url=jdbc_url+'?user='+username+'&password='+password
postActionStmt = "vacuum SALES_ORDER_FACT"

selectfields2.toDF().write.format("com.databricks.spark.redshift") \
.option("url", jdbc_url) \
.option("dbtable", target_table_1) \
.option("tempdir", args["TempDir"]) \
.option("postactions", postActionStmt) \
.mode("append") \
.save() 

updateCheckPoint(dynamodb,'SALES_ORDER_FACT', str(newcheckpoint),str(checkpoint),'COMPLETED')

print "Rows inserted into Redshift table: redshiftdb4_public_sales_order_fact : "+str(selectfields2.count())
print args['JOB_NAME']+" END..."

job.commit()
