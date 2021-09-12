
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, DecimalType

spark = SparkSession.builder.master('local').appName('app').getOrCreate()
# sc = SparkContext()
spark.conf.set("fs.azure.account.key.adfrachaelcdevstorage.blob.core.windows.net","cXM5vzkdfXgMVwxhGZtfhD2EEEcFzhyltJCArHRA+cXuRQbJG2kpLBGCFg5FRuLURmuz7Ik4ZXDpQBVvnpZpdw==")

trade_common = spark.read.parquet("output_dir/partition=T")

trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm','event_seq_nb', 'trade_price')

trade_grouped_df = trade.groupBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb") \
                .agg(F.collect_set("trade_price").alias("trade_prices"))

trade_grouped_df.show()
trade_date = '2020-07-29'
trade.write.parquet("path/trade/trade_dt={}".format(trade_date))


quote_common = spark.read.parquet("output_dir/partition=Q")

quote = quote_common.select('trade_dt', 'symbol', 'exchange', 'event_tm','event_seq_nb', 'bid_price', 'bid_size', 'ask_price', 'ask_size')

quote_grouped_df = quote.groupBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb") \
                .agg(F.collect_set("bid_price").alias("bid_prices"), F.collect_set("bid_size").alias("bid_sizes"),
                     F.collect_set("ask_price").alias("ask_prices"), F.collect_set("ask_price").alias("ask_prices"))

quote_grouped_df.show()
quote_date = '2020-07-29'
quote.write.parquet("path/quote/quote_dt={}".format(quote_date))


