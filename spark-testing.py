from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg,max,count, col, to_timestamp,min,max
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
)

# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()

## DataFrame creation and simple modification

orders_df = spark.read.options(delimiter=',',header = True, inferSchema = True).csv('data-src/orders.csv')
products_df = spark.read.options(delimiter=',',header = True, inferSchema = True).csv('data-src/products.csv')

orders_df.show(5)

orders_df = orders_df.withColumn('product_id', orders_df.product_id.cast('int'))

orders_df = orders_df.withColumn('product_id_twice', orders_df.product_id*2)

orders_df.show(5)

## Create table for using SQL instead

orders_df.createOrReplaceTempView("viewExample")

df1 = spark.sql("SELECT product_id,quantity from viewExample limit 5")

df1.show()

## GROUPING AND AGGREGATING

dfgroup2 = spark.sql("SELECT created_date, count(distinct product_id) prods, sum(quantity) sum_qty from viewExample group by created_date")
dfgroup2.show()


##### groupBy returns groupedData, not dataframe
##dfgroup1 is a GroupedData object, not a DataFrame 
dfgroup1 = orders_df.groupBy('created_date')
type(dfgroup1)

##### when aggregated, it already returns a DataFrame again, 
# so you can work it normally now

##dfaggregated_summed1 is a DataFrame
dfaggregated_summed1 = dfgroup1.sum('product_id','quantity')
dfaggregated_summed1.show()
type(dfaggregated_summed1)

### count, sum, avg, min, max, etc., need to be imported from pyspark.sql.functions
dfaggregated_summed2 = dfgroup1.agg(count('product_id').alias('prods'),sum('quantity').alias('qty_sum'))
dfaggregated_summed2.show()
type(dfaggregated_summed2)

## JOINS
orders_df.show(10)
products_df.show(10)
orders_and_prods = orders_df.join(products_df, orders_df.product_id==products_df.id,'left')

orders_and_prods.show(10)


## Reading JSON files
dfjson = spark.read.json('data-src/logs.jsonl')
dfjson.printSchema()
dfjson.show(30)

## Creating dataframe manually with a list
metrics_list = [(0,'Loss'),(1,'Accuracy')]

metrics_df = spark.createDataFrame(
        metrics_list,
        StructType(
            [
                StructField("metricId", IntegerType()),
                StructField("metricName", StringType()),
            ]
        ),
    )

new_mets = metrics_df.select(col('metricId'))
new_mets.show()

from pathlib import Path

curr_path = Path('/data-src')

curr_path.as_uri() + 'experiments.csv'
curr_path.absolute() + 'experiments.csv' 

curr_path.is_file()
curr_path.is_dir()


str(curr_path)
orders_df_test = spark.read.options(delimiter=',',header = True, inferSchema = True).csv(curr_path.as_uri() + 'experiments.csv')
orders_df_test = spark.read.options(delimiter=',',header = True, inferSchema = True).csv(curr_path.absolute())