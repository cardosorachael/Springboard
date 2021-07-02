
from CAPSTONE.Data_Pipeline_Prototype.LoadData import *
from CAPSTONE.Data_Pipeline_Prototype.TransformData import *
from CAPSTONE.Data_Pipeline_Prototype.Datasets import *


class Main:
    def __init__(self):
        logging.basicConfig(filename='pipeline.log', level=logging.DEBUG)
        dataset = 'yelp-dataset'
        load_data = LoadData(dataset=dataset)
        load_data.get_dataset_from_kaggle()
        load_data.get_downloaded_zip_path()
        load_data.unzip_file()

        load_data.get_unzip_folder_path()
        load_data.remove_non_json_files()
        load_data.get_unzip_folder_path()

        transform_data = TransformData(dataset=load_data.dataset, unzip_folder_path=load_data.unzip_folder_path)
        transform_data.init_output_folder()
        transform_data.loop_generic()



if __name__ == '__main__':
    Main()