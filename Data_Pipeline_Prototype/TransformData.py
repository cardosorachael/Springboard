import os
import logging
import pandas as pd

from CAPSTONE.Data_Pipeline_Prototype.Datasets import *

class TransformData:
    """
    Args:
        dataset: name of dataset pulled from Kaggle
        unzip_folder_path: path dir of unzipped, downloaded dataset from kaggle
    """
    def __init__(self, dataset, unzip_folder_path):
        self.dataset = dataset
        self.unzip_folder_path = unzip_folder_path

    def init_output_folder(self):
        """
        Initializes a new folder to store the transformed files in directory
        :return: output folder file path
        """
        output_dir = "output"
        self.output_path = os.path.join(os.getcwd(), output_dir)
        try:
            os.mkdir(self.output_path)
            logging.info('output folder created')
            return (self.output_path)
        except FileExistsError:
            logging.warning('Output folder already created!')
            return (self.output_path)

    def loop_and_chunk(self):
        """
        Loops over every file in the unzipped folder and applies different transformation steps to each type of dataset
        .json files loaded in chunks and stored for faster processing
        :return:
        """
        count = 0
        for filename in os.listdir(self.unzip_folder_path):
            if filename.endswith("business.json"):
                for df in pd.read_json(self.unzip_folder_path + r'\/' + filename, lines=True, chunksize=1000):
                    df = df[df['categories'].str.contains('Restaurant')==True]
                    print(df.head())
                    df.to_csv(self.output_path + r"\business_" + str(count) + '.csv', index=False)
                    count += 1
            else:
                continue


    def loop_generic(self):
        #TODO implement threads instead of if/else
        #TODO store files in sub directory in output by name

        """
        Loops over every file in the unzipped folder and applies different transformation steps to each type of dataset
        .json files loaded in chunks and stored for faster processing
        :return:
        """
        for filename in os.listdir(self.unzip_folder_path):
            if filename.endswith('business.json'):
                count = 0
                for dataframe in pd.read_json(self.unzip_folder_path + r'\/' + filename, lines=True, chunksize=1000):
                    b = Business(dataframe=dataframe)
                    df = b.remove_non_restaurants()
                    df = b.aggregate_hours_open(df=df)
                    df = b.clean_attributes(df=df)
                    df.to_csv(self.output_path + r"\business_" + str(count) + '.csv', index=False)
                    count += 1
                pass

            elif filename.endswith('user.json'):
                count = 0
                for dataframe in pd.read_json(self.unzip_folder_path + r'\/' + filename, lines=True, chunksize=1000):
                    u = User(dataframe=dataframe)
                    # df = u.yelping_since()
                    df = u.is_elite()
                    df.to_csv(self.output_path + r"\user_" + str(count) + '.csv', index=False)
                    count += 1
                pass
            elif filename.endswith('review.json'):
                count = 0
                for dataframe in pd.read_json(self.unzip_folder_path + r'\/' + filename, lines=True, chunksize=1000):
                    r = Review(dataframe=dataframe)
                    df = r.transform_review_data()
                    df.to_csv(self.output_path + r"\review_" + str(count) + '.csv', index=False)
                    count += 1
                pass

            elif filename.endswith('tip.json'):
                count = 0
                for dataframe in pd.read_json(self.unzip_folder_path + r'\/' + filename, lines=True, chunksize=1000):
                    t = Tip(dataframe=dataframe)
                    df = t.transform_tip_data()
                    df.to_csv(self.output_path + r"\tip_" + str(count) + '.csv', index=False)
                    count += 1
                pass

            elif filename.endswith('checkin.json'):
                count = 0

                for dataframe in pd.read_json(self.unzip_folder_path + r'\/' + filename, lines=True, chunksize=1000):
                    c = Checkin(dataframe=dataframe)
                    df = c.sum_num_of_checkins()
                    df = c.transform_business_id_col(df=df)
                    df = c.change_column_name(df=df)
                    df.to_csv(self.output_path + r"\checkin_" + str(count) + '.csv', index=False)
                    count += 1
                pass

            else:
                logging.info('finished going through folder')
