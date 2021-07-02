from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
import os
import logging


class LoadData:
    """
    Args:
        dataset: name of dataset pulled from Kaggle
    """
    def __init__(self, dataset):
        self.dataset= dataset

    def get_dataset_from_kaggle(self):
        """
        Uses Kaggle api to download all datasets from yelp database
        :return:
        """
        api = KaggleApi()
        api.authenticate()
        logging.info('downloading {} dataset...'.format(self.dataset))
        try:
            api.dataset_download_files('yelp-dataset/' + self.dataset)
            logging.info('successfully downloaded {} dataset'.format(self.dataset))
        #     #TODO handle exception if wrong dataset is downloaded
        except ValueError:
            logging.critical('Invalid dataset: no files downloaded')


    def get_downloaded_zip_path(self):
        """
        gets path of downloaded folder to use in later processing
        :return:
        """
        self.downloaded_zip_path = os.getcwd() + '\\' + self.dataset + '.zip'


    def unzip_file(self):
        """
        unzips downloaded dataset into same directory into folder with the same name
        :return:
        """
        with ZipFile(self.downloaded_zip_path, 'r') as zipObj:
            # Extract all the contents of zip file in current directory
            unzip_folder = self.dataset
            zipObj.extractall(unzip_folder)


    def get_unzip_folder_path(self):
        """
        gets file path of unzipped folder
        :return: unzipped file path
        """
        self.unzip_folder_path = os.getcwd() + '\\' + self.dataset
        return (self.unzip_folder_path)

    def remove_non_json_files(self):
        """
        Iterates over files in unzipped file path and removes any non-json files
        Non json files are usage agreements from kaggle and don't need to be included in data transformation steps
        :return:
        """
        for filename in os.listdir(self.unzip_folder_path):
            if filename.endswith(".json"):
                continue
            else:
                os.remove(self.unzip_folder_path + '\\' + filename)
        logging.info('non json files removed ')


