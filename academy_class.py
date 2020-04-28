from files_to_dataframe import ExtractToDF
import pandas as pd
import numpy as np
import sys
import ast
import boto3 # delete later
from FileDictionary import files
from datetime import datetime
from split_name_auto import split_full_name
import distance
from sqlalchemy import create_engine
import urllib


class Table:

    params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0};"
                                     "SERVER=localhost,1433;"
                                     "DATABASE=SpartaDB;"
                                     "UID=SA;"
                                     "PWD=Passw0rd2018")
    engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

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

    def add_ids_col(self, df, column_name):
        return df.assign(**{column_name: np.arange(1, len(df) + 1)})

    def load(self, df, sql_table_name):
        """Note that the columns in the dataframe need to match the column names
        and order of those in the sql table"""
        df.to_sql(sql_table_name, con=Table.engine, if_exists='append', index=False)


# class Academy(Table):




class Course(Table):

    def __init__(self):
        self.folder = 'Academy'
        self.raw_data = self.extract('Academy', file_type='csv')
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
        return courses_df, spartans_df  # first returns [CourseID Course_Type Course_Number Start_Date
                                        # Course_Length Trainer];
                                        # second returns [name CourseId]

    def export(self):
        pass


class Trainer(Table):

    def __init__(self):
        super().__init__()

    def get_trainer(self): # for matching IDs with
        df = self.extract('Academy', file_type='csv')
        df = df[['trainer']]
        df = df.drop_duplicates()
        df = self.add_ids_col(df, 'trainer_ID')
        return df

    # fixme: combine them later id get trainer is not called anywhere else

    def create_trainer_table(self): # finalised table for sql
        df = self.get_trainer()
        first_last_names = df['trainer'].str.split(" ", 1, expand=True)
        first_last_names.columns = ['first_name', 'last_name']
        df = pd.concat([df, first_last_names], axis=1)
        df = df.drop(['trainer'], axis=1)
        return df

    def export(self):
        self.load(self.create_trainer_table(), 'Trainers')

# inst = Trainer()
# print(inst.export())


# ins = Course()
# df = ins.get_course_details()
# print(df[0].head(10))
# print(df[0].info())
# print(df[1].head(10))
# print(df[1].info())


import re
class AcademyCompetency:  #INHERITS FROM (Spartan)

    """ A class that will create a table for one of the following competencies:
    IH = Intellectual Horsepower
    IS = Interpersonal Savvy
    PV = Perseverance
    PS = Problem Solving
    SD = Self Development
    SA = Standing Alone
    It will return a pandas dataframe containing the spartans_id and their performance on one of the above for each
    week in the course.
    """

    def __init__(self):
        super().__init__()


    def extract_competency(self, competency):
        extracted_df = self.extract('Academy')
        # iterate through each column's name if does not start with competencies initials, or it's not 'name'column,
        # then add to redundant_list
        redundant_col_list = [column for column in extracted_df.columns
                   if column != 'name' and not bool(re.match(competency, column))]
        comp_df = extracted_df.drop(redundant_col_list, axis=1)
        return comp_df

    def create_competency_table(self, competency):
        comp_df = self.extract_competency(competency)
        spartan_df = self.add_spartan_ids()  # returns a df consisting of name, courseId and spartan_id
        comp_table = pd.merge(comp_df, spartan_df, how='left', on='name')
        return comp_table.drop(columns=['CourseId', 'name'])


class A:

    def def_A1(self):
        instance_B = B().def_B1()
        return instance_B

    def def_A2(self):
        return 'I am A2'


class B:

    def def_B1(self):
        return 'I am B1'

    def def_B2(self):
        instance_A = A().def_A2()
        return instance_A


print(A().def_A1())

print(B().def_B2())