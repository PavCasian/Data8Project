from files_to_dataframe import ExtractToDF
import pandas as pd
import numpy as np
import sys
import ast
import boto3 # delete later
from FileDictionary import files
from datetime import datetime


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
        self.folder = 'Academy'
        super().__init__()

    def get_course_details(self):
        s3_client = boto3.client('s3')
        file_names = files('data8-engineering-project', self.folder)
        courses_list = []
        spartans_list = []
        for file_name in file_names[self.folder]:  # file_name ex 'Business_25_2019-03-04.csv'
            file_name_split = file_name.split('_')  # ['Business', '25', '2019-03-04.csv']
            course_type = file_name_split[0]
            course_initial = course_type[0]
            course_number = file_name_split[1]
            course_id = course_initial + course_number
            start_date = file_name_split[2].split('.')[0]  # removes .csv at the end
            # we need the trainer's name and the length of course from inside file
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
            data = s3_client.get_object(Bucket='data8-engineering-project',
                                        Key=self.folder + '/' + file_name)
            cohort_performance_df = pd.read_csv(data['Body'])
            trainer_name = cohort_performance_df.at[0, 'trainer']
            column_names = list(cohort_performance_df.columns.values)  # gets the names of all the columns
            last_column = column_names[-1]  # gets the name of the last column for this particular table
            if len(last_column) < 6:  # the length of the name varies depending on whether its 8 or 10 week course
                course_length = last_column[-1]  #  this is for the 8 week courses
            else:
                course_length = last_column[-2:] # this is for the 10 week courses
            course_details = (course_id, course_type, course_number, start_date, course_length, trainer_name)
            courses_list.append(course_details)
            # we also want the courseID and the spartan's name
            spartans_group = cohort_performance_df[['name']]
            spartans_group = spartans_group.assign(CourseId = course_id) # creates column CourseId and assigns value
            spartans_list.append(spartans_group)
        courses_df = pd.DataFrame(courses_list, columns=['CourseID', 'Course_Type', 'Course_Number', 'Start_Date',
                                                         'Course_Length', 'Trainer'])
        spartans_df = pd.concat(spartans_list)
        return courses_df, spartans_df

    # def create_spartan_table(self, df, c_id):
    #     # take the spartan name from the df and append to each the courseID
    #     # our spartan table ID will use the candidate's ID, the mapping between them will be made using course_type and name
    #
    #
    #
    # def create_course_table(self):


ins = Course()
df = ins.get_course_details()
print(df)
