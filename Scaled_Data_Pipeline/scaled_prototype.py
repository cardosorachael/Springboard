from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col
import logging
import sys

spark = SparkSession.builder.master('local').appName('app').getOrCreate()

class TransformData:
    """
       Args:
           source: path of s3 bucket containing datasets
           output: path of s3 bucket to store processed parquet files after transforming+cleaning
    """
    def __init__(self, source: str, output: str):
        self.source = source
        self.output = output

    def transform_business(self):
        """
            Reads business dataset, cleans data and stores processed files in output destination
        """
        logging.info('reading business dataset...')
        df_b = spark.read.json(self.source + "yelp_academic_dataset_business.json")
        df_b = df_b.withColumn("price_range", col("attributes").getField("RestaurantsPriceRange2"))
        df_b = df_b.na.drop()
        df_b = df_b.filter(col('categories').contains("Restaurants"))
        df_b = df_b.drop('attributes')
        df_b = df_b.drop('hours')
        logging.info('saving business dataset...')
        df_b.write.format("parquet").mode("overwrite").save(self.output + "business/")

    def transform_review(self):
        """
            Reads review dataset, cleans data and stores processed files in output destination
        """
        logging.info('reading review dataset...')
        df_r = spark.read.json(self.source + "yelp_academic_dataset_review.json")
        df_r = df_r.na.drop()
        logging.info('saving review dataset...')
        df_r.write.format("parquet").mode("overwrite").save(self.output + "review/")

    def transform_tip(self):
        """
            Reads tip dataset, cleans data and stores processed files in output destination
        """
        logging.info('reading tip dataset...')
        df_t = spark.read.json(self.source + "yelp_academic_dataset_tip.json")
        df_t = df_t.na.drop()
        logging.info('saving tip dataset...')
        df_t.write.format("parquet").mode("overwrite").save(self.output + "tip/")

    def transform_user(self):
        """
            Reads user dataset, cleans data and stores processed files in output destination
        """
        logging.info('reading user dataset...')
        df_u = spark.read.json(self.source + "yelp_academic_dataset_user.json")
        logging.info('saving user dataset...')
        df_u.write.format("parquet").mode("overwrite").save(self.output + "user/")

    def tranform_checkin(self):
        """
            Reads checkin dataset, cleans data and stores processed files in output destination
        """
        logging.info('reading checkin dataset...')
        df_c = spark.read.json(self.source + "yelp_academic_dataset_checkin.json")
        df_c = df_c.withColumn('business_id', F.regexp_replace('business_id', r'^[--]*', ''))
        df_c = df_c.drop('_corrupt_record')
        logging.info('saving checkin dataset...')
        df_c.write.format("parquet").mode("overwrite").save(self.output + "checkin/")


class Main:
    """
        Args:
            source: path of s3 bucket containing datasets
            output: path of s3 bucket to store processed parquet files after transforming+cleaning
    """
    def __init__(self):
        logging.basicConfig(filename='pipeline.log', level=logging.DEBUG)

        transform_data = TransformData(source=sys.argv[1], output=sys.argv[2])
        transform_data.transform_business()
        transform_data.tranform_checkin()
        transform_data.transform_review()
        transform_data.transform_tip()
        transform_data.transform_user()


if __name__ == '__main__':
    Main()

