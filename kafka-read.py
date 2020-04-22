from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.sql.functions import udf, log
import time

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-3.logging.2wby21.c6.kafka.us-east-1.amazonaws.com:9094,b-1.logging.2wby21.c6.kafka.us-east-1.amazonaws.com:9094,b-2.logging.2wby21.c6.kafka.us-east-1.amazonaws.com:9094") \
  .option("subscribe", "signals") \
  .option("kafka.security.protocol", "SSL") \
  .option("startingOffsets", "latest") \
  .load()

df.printSchema()
df1=df.selectExpr("CAST(value AS STRING)")
# def decompress_babyloger(value, timestamp):
    #logger.info("Inside the convert_binery_string function ")
    # bytes_cont = _uncompress_content(raw_content=value, timestamp=timestamp)
    # if bytes_cont == None:
    #     return None
    # parsed_message =  _convert_raw_content_and_send(bytes_cont=bytes_cont)
    # return parsed_message

#UDF to decompress Message
# decompress_babyloger_udf = udf(lambda x,y: decompress_babyloger(x,y), ArrayType(StringType()) )

#df2 = df1.select(decompress_babyloger_udf('value','timestamp').alias('binary_date_in_string') )

#df2 = df2.select(explode('binary_date_in_string').alias("messages_detail_unpacked"))
#{"msgId":277,"timestamp":1587141661475,"epoch":1587141661,"usec":475620,"vlan":"MCU","vin":"000017","msgName":"MCUR_VCU_Tq","signalName":"IMCUR_TqDerateFac","value":0.0}
schema = StructType() \
    .add("msgId", IntegerType()) \
    .add("timestamp", LongType()) \
    .add("epoch", LongType()) \
    .add("usec", IntegerType()) \
    .add("vlan",  StringType()) \
    .add("vin", StringType()) \
    .add("msgName", StringType()) \
    .add("signalName", StringType()) \
    .add("value", FloatType())
#df4 = df3.select(from_json(col("messages_detail_unpacked"), message_schema).alias("message_details"))

record = df1.select(from_json(col("value"), schema).alias("signal"))

final = record.selectExpr("signal.*")



#
#
# # value schema: { "a": 1, "b": "string" }
# schema = StructType().add("a", IntegerType()).add("b", StringType())
# df.select( \
#   col("key").cast("string"),
#   from_json(col("value").cast("string"), schema))
# query=signal.writeStream.format("console").start()
# time.sleep(10) # sleep 10 seconds
# query.stop()

query = final \
    .writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "s3a://pyspark-kafka/") \
    .option("checkpointLocation", "file:///opt/spark/checkpoint") \
    .start()
