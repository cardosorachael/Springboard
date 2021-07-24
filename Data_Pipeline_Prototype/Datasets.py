import ast
import time

class Business:
    """
    class of all transformations to be performed on a loaded df chunk for the business dataset
    """
    def __init__(self, dataframe):
        self.dataframe= dataframe

    def remove_non_restaurants(self):
        """
        Drops all rows that don't contain 'Restuarant' in cateogories
        :return: filtered dataframe
        """
        self.dataframe = self.dataframe[self.dataframe['categories'].str.contains('Restaurant')==True]
        return(self.dataframe)

    def aggregate_hours_open(self, df):
        """
        Iterates over each row in the loaded df and aggregates the number of days open into a new column 'days_open'
        :return: filtered dataframe
        """
        df['days_open'] = 0
        for i, row in enumerate(df['hours']):
            res_hours_row = ast.literal_eval(str(row))
            if res_hours_row:
                df['days_open'][i] = len(res_hours_row)
            else:
                continue
        df = df.drop(['hours'], axis=1)
        # self.dataframe = df
        return(df)


    def clean_attributes(self,df):
        for i, row in enumerate(df['attributes']):
            if row:
                try:
                    print(row['RestaurantsPriceRange2'])
                    df['attributes'][i] = row['RestaurantsPriceRange2']
                except KeyError:
                    df['attributes'][i] = 0

        df.rename(columns={'attributes': 'price_range'}, inplace=True)
        return(df)


class User:
    """
    class of all transformations to be performed on a loaded df chunk for the user dataset
    """
    def __init__(self, dataframe):
        self.dataframe = dataframe


    def is_elite(self):
        """
        Changes the value to is_elite column with BOOL True/False if the user ever held an elite status
        :return: filtered dataframe
        """

        for i, row in enumerate(self.dataframe['elite']):
            if row:
                self.dataframe['elite'][i] = True
            else:
                self.dataframe['elite'][i] = False
        return(self.dataframe)



class Review:
    """
    class of all transformations to be performed on a loaded df chunk for the review dataset
    """
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def transform_review_data(self):
        """
        Dummy function for later
        """
        return (self.dataframe)


class Tip:
    """
    class of all transformations to be performed on a loaded df chunk for the tips dataset
    """
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def transform_tip_data(self):
        """
        Dummy function for later
        """
        return(self.dataframe)


class Checkin:
    """
    class of all transformations to be performed on a loaded df chunk for the checkins dataset
    """
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def sum_num_of_checkins(self):
        """
        Sums individual checkin dates to get total number of checkins
        :return: transformed dataset
        """
        for i, row in enumerate(self.dataframe['date']):
            row = row.split(",")
            self.dataframe['date'][i] = len(row)
        return(self.dataframe)

    def change_column_name(self, df):
        df.columns = ['business_id', 'no_of_checkins']
        return(df)

    def transform_business_id_col(self, df):
        """
        Removes leading '--' signs before the business id name to avoid query mismatch
        :return: transformed dataset
        """
        df['business_id'] = df['business_id'].str.replace('-', '')
        return(df)