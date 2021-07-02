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
        #TODO debug why values not populating after 111th entry...
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
        self.dataframe = df
        return(self.dataframe)


    def clean_attributes(self,df):
        #TODO figure out how to clean attributes
        pass

    def clean_categories(self,df):
        #TODO figure out how to handle comma separated categories
        pass


class User:
    """
    class of all transformations to be performed on a loaded df chunk for the user dataset
    """
    def __init__(self, dataframe):
        self.dataframe = dataframe

    def yelping_since(self):
        """
        Replaces the yelping since column with just the year instead of the datetime for faster/more efficient querying
        :return: filtered dataframe
        """
        return(self.dataframe)

    def is_elite(self, df):
        """
        Changes the value to is_elite column with BOOL True/False if the user ever held an elite status
        :return: filtered dataframe
        """
        return(df)



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
        # TODO define data transformations for Tip
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
        #TODO define data transformations for Tip
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

    def transform_business_id_col(self, df):
        """
        Removes leading '--' signs before the business id name to avoid query mismatch
        :return: transformed dataset
        """
        df['business_id'] = df['business_id'].str.replace('-', '')
        return(df)