import json
import pandas as pd
from tabulate import tabulate

path = r"C:\Users\cardo\PycharmProjects\SPRINGBOARD\CAPSTONE\Access_data\YELP"
fname = r'\yelp_academic_dataset_business.json'
filename = path + fname
#JSON TO CSV CONVERTER - useful for quick look at dataset, fields, to get an idea of how to clean/export data
# data_file = open(filename)
# data = []
# for line in data_file:
#     data.append(json.loads(line))
#     checkin_df = pd.DataFrame(data)
#     checkin_df.to_csv('name.csv',index=False)
# data_file.close()

#JSON LOAD AS DF - more practical to work on, clean

df=pd.read_json(filename, lines=True)
df = df[df.categories.str.contains('Restaurants', na=False)]  #example of extracting only restaurants from business dataset
df.to_csv('business.csv', index=False)
