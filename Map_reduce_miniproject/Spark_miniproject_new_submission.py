from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext


# sqlContext = SQLContext(sc)
def extract_vin_key_value(line: str):
   str_vals = line.split(',')
   type_val = str_vals[1]
   vin_num_val = str_vals[2]
   make_val = str_vals[3]
   year_val = str_vals[5]
   value = [type_val, make_val, year_val]
   return [vin_num_val,value]

sc = SparkContext()
raw_rdd = sc.textFile("data.csv")

# map (key,value) with key is vin_number and value is (make, year, incident_type)
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

def func(df):
    df2 = ()
    a = []
    for row in df.collect():
        if len(row[1][1]) !=0:
            a.append(row[0])
    dict1 = dict.fromkeys(a, 0)
    for row in df.collect():
        if len(row[1][1]) !=0:
            dict1[row[0]] = (row[1][1],row[1][2])
    return(dict1)

from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(vin_kv).toDF("vin", "value")

df.show()
dict1 = func(df)

def func2(x, dict1):
    print(x[1][1])
    if len(x[1][1]) == 0:
        x[1][1] = dict1[x[0]][0]
        x[1][2] = dict1[x[0]][1]
    return (x)


x_test = ['INU45KIOOPA343980', ['A', '', '']]

func2(x=x_test, dict1=dict1)
enhance_make = vin_kv.map(lambda kv: func2(kv, dict1))

enhance_make2 = enhance_make.filter(lambda x: x[1][0] == 'A')
make_kv = enhance_make2.map(lambda x: x[1][1] + '-' + x[1][2])
make_kv_count = make_kv.map(lambda x: (x, 1))
make_kv_c = make_kv_count.reduceByKey(lambda x, y: x+y)
print(*make_kv_c.collect(), sep = '\n')



