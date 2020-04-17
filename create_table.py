from files_to_dataframe import ExtractToDF
import pandas as pd
import numpy as np
import sys
import ast
import boto3 # delete later
from FileDictionary import files


class Table:

    def extract(self, folder, file_type='csv'):
        extraction_instance = ExtractToDF(folder)
        try: # in case the specified file_type is incorrect
            if file_type == 'csv':
                df = extraction_instance.from_csv()
            elif file_type == 'txt':
                df = extraction_instance.from_txt()
            elif file_type == 'json':
                df = extraction_instance.from_txt()
            return df
        except ValueError:
            print('The file_type parameter does not match the type of the file')
            sys.exit(1)  # will stop the program if exception raised

    # def add_id_df(self):
    #     np.arange(len(df))

    def load(self):
        pass

class Trainer(Table):

    def __init__(self):
        super().__init__()
        # self.trainerstable()


    def get_trainer(self): # for matching IDs with
        df = self.extract('Academy', file_type='csv')
        df = df[['trainer']]
        df = df.drop_duplicates()
        df['TrainerID'] = np.arange(len(df))
        return df

    # fixme: combine them later id get trainer is not called anywhere else

    def trainerstable(self): # finalised table for sql
        df = self.get_trainer()
        first_last_names = df['trainer'].str.split(" ", 1, expand=True)
        first_last_names.columns =['first_name','last_name']
        df = pd.concat([df, first_last_names], axis=1)
        df = df.drop(['trainer'], axis=1)
        return df

# inst = Trainer()
# print(inst.get_trainer())
class StrengthWeaknessTechnology(Table):

    def __init__(self, from_column): # 'strengths', 'weaknesses' or 'technologies'
        super().__init__()
        self.from_column = from_column
        if self.from_column == 'strengths':
            self.col_name = 'StrengthName'
            self.col_id = 'StrengthID'
        elif self.from_column == 'weaknesses':
            self.col_name = 'WeaknessName'
            self.col_id = 'WeaknessID'
        elif self.from_column == 'technologies':
            self.col_name = 'TechName'
            self.col_id = 'TechID'

    def get_items_df(self):  # pull all characteristic from_column in a df
        items_list= self.get_items_list()
        df = pd.DataFrame(items_list)
        df.columns=[self.col_name]
        df[self.col_id] = np.arange(len(df))  # adding ids to each characteristic
        return df


    def get_items_list(self):  # unique items list of strengths/weaknesses
        df = self.extract('TransformedFiles', file_type='csv')
        unique_item_list = []
        for row in df[self.from_column].iteritems():  # each cand has a list of strengths/weaknesses
            items_list = ast.literal_eval(row[1])  # 0 is index and 1 is a string list of items;
            # literal_eval removes the string quotes of the list
            for item in items_list:  # there are multiple items in a list
                if self.from_column == 'technologies': # the column includes a list of dicts
                    item = item['language']  # only the programming language wanted
                if item not in unique_item_list:
                    unique_item_list.append(item)
        return unique_item_list


# strength_instance = StrengthWeaknessTechnology('technologies')
# print(strength_instance.get_items_df())


class Academy(Table):

    def __init__(self):
        super().__init__()

    def get_academy(self):
        df = self.extract('SpartaDays', file_type='txt')
        df = df[['academy']]
        df = df.drop_duplicates()
        df['AcademyID'] = np.arange(len(df))
        return df

# test_inst = Academy()
# print(test_inst.get_academy())

class Course(Table):

    def __init__(self):
        super().__init__()


    def dataframe(self, bucket, folder):
        s3_client = boto3.client('s3')
        # contents = s3_client.list_objects_v2(Bucket=bucket, Prefix=f'{folder}/')['Contents']
        contents = files(bucket, folder)
        # print(contents)
        df_list = []
        for file_name in contents[folder]:  # file_name ex 'Business_25_2019-03-04.csv'

                data = s3_client.get_object(Bucket='data8-engineering-project',
                                            Key=folder + '/' + file_name)
                single_file_df = pd.read_csv(data['Body'])
                splitted_file_name = file_name.split('_')  # ['Business', '25', '2019-03-04.csv']
                course_type = splitted_file_name[0]
                course_initial = course_type[0]
                course_number = splitted_file_name[1]
                course_id = course_initial + course_number
                start_date = splitted_file_name[2].split('.')[0]  # removes .csv on the end
                column_names = list(single_file_df.columns.values)  # gets the names of all the columns
                last_column = column_names[-1]  # gets the name of the last column for this particular table
                if len(
                        last_column) < 6:  ## the length of the name varies depending on whether its an 8 or 10 week course
                    course_length = last_column[-1]  ##  this is for the 8 week courses
                else:
                    course_length = last_column[-2:] ## this is for the 10 week courses
                single_file_df[
                    'CourseID'] = course_id  # creates an extra column in the dataframe called course that contains the concatenated course name
                single_file_df['Course Type'] = course_type
                single_file_df['Course Number'] = course_number
                single_file_df['Start_Date'] = start_date  # creates an extra column that contains the date
                single_file_df['Course_Length'] = course_length  # makes an extra column that will have the course length
                df2 = single_file_df[['CourseID', 'Course Type', 'Start_Date',
                                      'Course_Length', 'trainer', 'Course Number']].iloc[:1]
                df_list.append(
                    df2)  ## adds data frame to a list of dataframes to all be returned at once when all files have been iterated through
        df = pd.concat(df_list)
        return df

    def dateformat(self,
            col):  # takes in the bucket and folder and creates df using above function & formats column specified to date
        df = self.dataframe('data8-engineering-project', 'Academy')
        df[col] = pd.to_datetime(df[col])
        return df

# ins = Course()
# print(ins.dateformat('Start_Date'))


# import re
# either this
# str1 = 'multiple 10'
# print(re.findall('\d+', str1 ))
# or this
# print(int(filter(str.isdigit, str1)))
# for i in filter(str.isnumeric, str1):
#     print(i)