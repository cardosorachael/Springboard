from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import *
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

        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Thai"), "Thai").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Deli"), "Deli").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("American"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Mediterranean"), "Mediterranean").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Indian"), "Indian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Japanese"), "Japanese").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Seafood"), "Seafood").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Italian"), "Italian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Chinese"), "Chinese").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Mexican"), "Mexican").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Vietnamese"), "Vietnamese").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("German"), "German").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("French"), "French").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Ice Cream"), "Ice Cream").otherwise(df_b.categories))

        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Cafe"), "Coffee").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Taiwanese"), "Taiwanese").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Hawaiian"), "Hawaiian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("African"), "African").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Polish"), "Polish").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Korean"), "Korean").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories', when(df_b.categories.contains("Boba"), "Boba").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Persian"), "Persian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Filipino"), "Filipino").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Lebanese"), "Lebanese").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Portuguese"), "Portuguese").otherwise(df_b.categories))
        #
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Halal"), "Halal").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Deli"), "Sandwiches").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Canadian"), "Canadian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("American"), "Fast Food").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Lebanese"), "Lebanese").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Nepalese"), "Nepalese").otherwise(df_b.categories))

        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("British"), "British").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Italian"), "Pizza").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Middle Eastern"), "Middle Eastern").otherwise(
                                   df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Ethiopian"), "Ethiopian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Dessert"), "Dessert").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Fusion"), "Fusion").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Greek"), "Greek").otherwise(df_b.categories))

        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Sandwiches"), "Deli").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Pizza"), "Italian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Fast Food"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Tapas"), "Spanish").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Burgers"), "American").otherwise(df_b.categories))

        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Coffee"), "Cafe").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Irish"), "Irish").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Malaysian"), "Malaysian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Russian"), "Russian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Ukrainian"), "Ukrainian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Cambodian"), "Cambodian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Caribbean"), "Caribbean").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Mongolian"), "Mongolian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Brazilian"), "Brazilian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Steakhouse"), "Steakhouse").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Smoothies"), "Smoothies").otherwise(df_b.categories))

        # food, bars
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Pancakes"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Waffles"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Brunch"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Bars"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Southern"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Comfort"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Food"), "American").otherwise(df_b.categories))

        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Cuban"), "Cuban").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Gastropubs"), "Irish").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Fish & Chips"), "British").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Diners"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Peruvian"), "Peruvian").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Creperies"), "French").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Hot Dogs"), "American").otherwise(df_b.categories))
        df_b = df_b.withColumn('categories',
                               when(df_b.categories.contains("Restaurants"), "Misc").otherwise(df_b.categories))

        logging.info('saving business dataset...')
        df_b.write.format("csv").mode("overwrite").save(self.output + "business/")

    def transform_review(self):
        """
            Reads review dataset, cleans data and stores processed files in output destination
        """
        logging.info('reading review dataset...')
        df_r = spark.read.json(self.source + "yelp_academic_dataset_review.json")
        df_r = df_r.na.drop()
        logging.info('saving review dataset...')
        df_r.write.format("csv").mode("overwrite").save(self.output + "review/")

    def transform_tip(self):
        """
            Reads tip dataset, cleans data and stores processed files in output destination
        """
        logging.info('reading tip dataset...')
        df_t = spark.read.json(self.source + "yelp_academic_dataset_tip.json")
        df_t = df_t.na.drop()
        logging.info('saving tip dataset...')
        df_t.write.format("csv").mode("overwrite").save(self.output + "tip/")

    def transform_user(self):
        """
            Reads user dataset, cleans data and stores processed files in output destination
        """
        logging.info('reading user dataset...')
        df_u = spark.read.json(self.source + "yelp_academic_dataset_user.json")
        logging.info('saving user dataset...')
        df_u.write.format("csv").mode("overwrite").save(self.output + "user/")

    def tranform_checkin(self):
        """
            Reads checkin dataset, cleans data and stores processed files in output destination
        """
        logging.info('reading checkin dataset...')
        df_c = spark.read.json(self.source + "yelp_academic_dataset_checkin.json")
        df_c = df_c.withColumn('business_id', F.regexp_replace('business_id', r'^[--]*', ''))
        df_c = df_c.drop('_corrupt_record')
        logging.info('saving checkin dataset...')
        df_c.write.format("csv").mode("overwrite").save(self.output + "checkin/")


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

