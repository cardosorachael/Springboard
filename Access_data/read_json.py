import json
import pandas as pd
from tabulate import tabulate
path = r"C:\Users\cardo\PycharmProjects\SPRINGBOARD\CAPSTONE\Access_data\YELP"
fname = r'\yelp_academic_dataset_business.json'

#JSON TO CSV CONVERTER - useful for quick look at dataset, fields, to get an idea of how to clean/export data
data_file = open(path+fname)
data = []
for line in data_file:
    data.append(json.loads(line))
    checkin_df = pd.DataFrame(data)
    checkin_df.to_csv('name.csv',index=False)
data_file.close()

#JSON LOAD AS DF - more practical to work on, clean

df=pd.read_json(path+fname, lines=True)
df = df[df['categories'].isin(['Restaurants'])]  #example of extracting only restaurants from business dataset
print(tabulate(df))


#Alternative approach
# df = pd.DataFrame()
# with open(path+fname, encoding='utf-8') as f:
#     for line in f:
#         data = json.loads(line)
#         data.pop('attributes', None)
#         data.pop('categories', None)
#         data.pop('hours', None)
#         # print(data)
#         # print(data['attributes'])
#         df = df.append(data, ignore_index=True)
# print(tabulate(df.head()))

