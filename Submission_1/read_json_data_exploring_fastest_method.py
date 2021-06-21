import csv
import numpy as np
import json
import pandas as pd
import time
import tabulate

#Enter filename + path
path = r"C:\Users\cardo\PycharmProjects\SPRINGBOARD\CAPSTONE\Access_data\YELP"
fname = r'\user.json' #file of interest
filename = path + fname

"""
Method 1 : load df as generator then iterate row by row 
"""
def csv_reader(filename):
    data = []
    for row in open(filename, "r", encoding="utf8"):
        data.append(json.loads(row))
        df = pd.DataFrame(data)
        df.to_csv('generator_test.csv',index=False)
        yield row

tic = time.perf_counter()
csv_gen = csv_reader(filename)
row_count = 0

for row in csv_gen:
    # print(row)
    row_count += 1

toc = time.perf_counter()
print(f"GENERATOR METHOD:  {toc - tic:0.4f} seconds")

#############################################
"""
Method 2 : load df in chunks and save each chunk to csv file 
"""
tic1 = time.perf_counter()
count = 0
for df in pd.read_json(filename, lines=True, chunksize=1000):
    # print(tabulate(df))
    df.to_csv("time_test_chunks_"+str(count)+'.csv', index=False)
    count +=1
toc1 = time.perf_counter()
print(f"CHUNKS/DF METHOD:  {toc1 - tic1:0.4f} seconds")

###################################
"""
Method 3 : load full df and save as one csv file 
"""
tic2 = time.perf_counter()
data_file = open(filename)
data = []
for line in data_file:
    data.append(json.loads(line))
    checkin_df = pd.DataFrame(data)
    checkin_df.to_csv('TIME_test_load_full_df.csv',index=False)
data_file.close()
toc2 = time.perf_counter()
print(f"FULL DF METHOD:  {toc2 - tic2:0.4f} seconds")

"""
RESULTS: 
GENERATOR METHOD: 0.0510 seconds 
CHUNKS/DF METHOD: 0.0103 seconds 
FULL DF METHOD:   0.0497 seconds 
Seeing these results - the chunks/df method was chosen 
"""