from pyspark.sql.functions import col, year, month, dayofmonth, hour, to_date, from_unixtime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "anpr", table_name = "anpr-parquet-step1", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database="anpr", table_name="anpr-parquet-step1",
                                                            transformation_ctx="datasource0")

df = datasource0.toDF()

columns_to_drop = ['year', 'month', 'day', 'hour']
df = df.drop(columns_to_drop, axis=1)

repartitioned_with_new_columns_df = df
    .withColumn(“date_col”, to_date(from_unixtime(col(“recordtimestamp”))))
    .withColumn(“year”, str(year(col(“date_col”))))
    .withColumn(“month”, str(month(col(“date_col”))))
    .withColumn(“day”, str(dayofmonth(col(“date_col”))))
    .withColumn(“hour”, str(hour(col(“date_col”))))
    .drop(col(“date_col”))
    .repartition(1)

dyf = DynamicFrame.fromDF(repartitioned_with_new_columns_df, glueContext, "enriched")

## @type: ApplyMapping
## @args: [mapping = [("uniqueid", "int", "uniqueid", "int"), ("speed", "int", "speed", "int"), ("trafficjamindicator", "int", "trafficjamindicator", "int"), ("recordtimestamp", "long", "recordtimestamp", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame=datasource0,
                                   mappings=[("uniqueid", "int", "uniqueid", "int"), ("speed", "int", "speed", "int"),
                                             ("trafficjamindicator", "int", "trafficjamindicator", "int"),
                                             ("recordtimestamp", "long", "recordtimestamp", "long")],
                                   transformation_ctx="applymapping1")

## @type: SelectFields
## @args: [paths = ["uniqueid", "speed", "trafficjamindicator", "recordtimestamp"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame=applymapping1,
                                   paths=["uniqueid", "speed", "trafficjamindicator", "recordtimestamp"],
                                   transformation_ctx="selectfields2")

## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "anpr", table_name = "anpr-parquet-repartitioned", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame=selectfields2, choice="MATCH_CATALOG", database="anpr",
                                     table_name="anpr-parquet-repartitioned", transformation_ctx="resolvechoice3")

## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice4"]
## @return: resolvechoice4
## @inputs: [frame = resolvechoice3]
resolvechoice4 = ResolveChoice.apply(frame=resolvechoice3, choice="make_struct", transformation_ctx="resolvechoice4")

## @type: DataSink
## @args: [database = "anpr", table_name = "anpr-parquet-repartitioned", transformation_ctx = "datasink5"]
## @return: datasink5
## @inputs: [frame = resolvechoice4]
datasink5 = glueContext.write_dynamic_frame.from_catalog(frame=dyf, database="anpr",
                                                         table_name="anpr-parquet-repartitioned",
                                                         transformation_ctx="datasink5")
job.commit()