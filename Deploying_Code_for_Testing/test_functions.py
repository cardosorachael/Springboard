import os.path
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import unittest
spark = SparkSession.builder.master('local').appName('app').getOrCreate()

class TestMethods(unittest.TestCase):

    def test_read_file(self):
        path = 'testfile.json'
        res = spark.read.json(path)
        self.assertEqual(type(res), pyspark.sql.dataframe.DataFrame)

    def test_remove_null_values(self):
        df = spark.read.csv("testfile.csv")
        self.assertEqual(df.na.drop().count(), 0)


    def test_filter_columns(self):
        df = spark.read.csv("testfile.csv")
        self.assertEqual(df.filter(col('_c0').contains("D")).count(), 1)


    def test_drop_columns(self):
        df = spark.read.csv("testfile.csv")
        self.assertEqual(len(df.drop('_c0').columns), 2)

    # def test_write_file(self):
    #     df = spark.read.csv("testfile.csv")
    #     df.write.format("parquet").mode("overwrite").save("test")
    #     self.assertEqual(os.path.exists('test'), True)

if __name__ == '__main__':
    unittest.main()