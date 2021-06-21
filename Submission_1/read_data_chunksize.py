
import pandas as pd
from tabulate import tabulate
import time
path = r"C:\Users\cardo\PycharmProjects\SPRINGBOARD\CAPSTONE\Access_data\YELP"
fname = r'\yelp_academic_dataset_checkin.json'
filename = path + fname
path_to_store = r"C:\Users\cardo\PycharmProjects\SPRINGBOARD\CAPSTONE\Access_data\CHECKINS_file"
count = 0
for df in pd.read_json(filename, lines=True, chunksize=1000):

    print(tabulate(df))
    df.to_csv(path_to_store +r"\checkin_"+str(count)+'.csv', index=False)
    count +=1
