from pyspark.sql import *
from pyspark import SparkContext

def extract_vin_key_value(line: str):
   str_vals = line.split(',')
   type_val = str_vals[1]
   vin_num_val = str_vals[2]
   make_val = str_vals[3]
   year_val = str_vals[5]
   value = (type_val, make_val, year_val)
   return (vin_num_val, value)


sc  = SparkContext("local", "MyApplication")

raw_rdd = sc.textFile("data.csv")

# map (key,value) with key is vin_number and value is (make, year, incident_type)
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

# perform group aggregation to populate make and year to all the records
enhance_make = vin_kv.groupByKey().flatMap(lambda kv: kv[1]).filter(lambda x: len(x[1]) > 0 and len(x[2]) > 0)

# Combine make, year to make-year
make_kv = enhance_make.map(lambda x: x[1] + '-' + x[2])

#Map each make-year to 1 and count by key
make_kv_count = make_kv.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
print(*make_kv_count.collect(), sep = '\n')