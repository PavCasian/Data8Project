from files_to_dataframe import ExtractToDF
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import numpy as np


class Table:

    def extract(self, folder, file_type=None):
        extraction_instance = ExtractToDF(folder)
        if file_type == 'csv':
            df = extraction_instance.from_csv()
        elif file_type == 'txt':
            df = extraction_instance.from_txt()
        elif file_type == 'json':
            df = extraction_instance.from_txt()
        return df

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
        name_le = LabelEncoder()
        df['TrainerID'] = name_le.fit_transform(df['trainer'])
        df['TrainerID'] += 1
        return df

    # fixme: combine them later id get trainer is not called anywhere else

    def trainerstable(self): # finalised table for sql
        df = self.get_trainer()
        first_last_names = df['trainer'].str.split(" ", 1, expand=True)
        first_last_names.columns =['first_name','last_name']
        df = pd.concat([df, first_last_names], axis=1)
        df = df.drop(['trainer'], axis=1)
        return df

inst = Trainer()
print(inst.get_trainer())
class StrengthWeakness(Table):

    def __init__(self, type, TypeName, TypeID):
        super().__init__()
        self.type = type
        self.TypeName = TypeName
        self.TypeID = TypeID
        self.get_characteristic()

    def get_characteristic(self):
        strength_list= self.extract(self.type)
        df = pd.DataFrame(strength_list)
        df.columns=[self.TypeName]
        df[self.TypeID] = np.arange(len(df))
        return df


    def extract(self, y):

        df = file.dataframecsv()
        list1 = []
        for row in df[y]:
            chars = ["'", "[", "]"]
            for c in chars:
                row = row.replace(c, '')
            row = row.split(',')
            for x in row:
                x = x.strip()
                if x not in list1:
                    list1.append(x)
        return list1

# strength_instance = StrengthWeakness('strengths', 'StrengthName', 'WeaknessID')


# class Technology:
#
#     def __init__(self):
#
#         self.get_tech()
#
#     def get_tech(self):
#         file = ReadTransformJson('data8-engineering-project', 'Interview Notes')
#         df = file.json_reader('Interview Notes') # uses json reader so can access technologies as a dictionary but will take longer to run
#         tech_list = []
#         for row in df.technologies:
#             for tech in row:
#                 tech = tech['language']
#                 if tech not in tech_list:
#                     tech_list.append(tech)
#         df = pd.DataFrame(tech_list)
#         df.columns=['TechName']
#         df['TechID'] = np.arange(len(df))
#         return df
#
#
# class Academy:
#
#     def __init__(self):
#         self.get_academy()
#
#     def get_academy(self):
#         file= FileToDF('data8-engineering-project', 'SpartaDays')
#         df = file.dataframetxt()
#         df = df[['academy']]
#         academy_le = LabelEncoder()
#         df['AcademyID'] = academy_le.fit_transform(df['academy'])
#         df = df.drop_duplicates()
#         df['AcademyID'] += 1
#         return df
#
#
# class Course:
#
#     def __init__(self):
#
#         pass
#
#     def dataframe(bucket, folder):
#         s3_client = boto3.client('s3')
#         contents = s3_client.list_objects_v2(Bucket=bucket, Prefix=f'{folder}/')['Contents']
#         df_list = []
#         for x in contents:
#             splitkey = x['Key'].split('.')
#             if splitkey[-1] == 'csv':
#                 data = s3_client.get_object(Bucket='data8-engineering-project',
#                                             Key=x['Key'])
#                 df1 = pd.read_csv(data['Body'])
#                 name = x['Key'].split('/')[1]  ####finds the name of the file
#                 course_type = name.split('_')[
#                     0]  ##splits the file up and takes the first part of it i.e. 'business', 'data'
#                 course_initial = course_type[0]
#                 course_number = name.split('_')[1]  ####takes the second entry, i.e. a number
#                 conc = course_initial + course_number  ####concatenates the course type and the number of which group it is
#                 course_start = name.split('_')[
#                     2]  # takes the 'date' from the file name but it still has .csv on the end
#                 start_date = course_start.split('.')[0]  # removes the .csv from the date
#                 column_names = list(df1.columns.values)  # gets the names of all the columns
#                 final_name = column_names[-1]  # gets the name of the last column for this particular table
#                 if len(
#                         final_name) < 6:  ## the length of the name varies depending on whether its an 8 or 10 week course
#                     course_length = final_name[-1]  ##  this is for the 8 week courses
#                 else:
#                     course_length = final_name[-2] + final_name[-1]  ## this is for the 10 week courses
#                 df1[
#                     'CourseID'] = conc  # creates an extra column in the dataframe called course that contains the concatenated course name
#                 df1['Course Type'] = course_type
#                 df1['Course Number'] = course_number
#                 df1['Start_Date'] = start_date  # creates an extra column that contains the date
#                 df1['Course_Length'] = course_length  # makes an extra column that will have the course length
#                 df2 = df1[['CourseID', 'Course Type', 'Start_Date', 'Course_Length', 'trainer', 'Course Number']].iloc[
#                       :1]
#                 df_list.append(
#                     df2)  ## adds data frame to a list of dataframes to all be returned at once when all files have been iterated through
#             else:
#                 pass
#         df = pd.concat(df_list)
#         return df
#
#     def dateformat(
#             col):  # takes in the bucket and folder and creates df using above function & formats column specified to date
#         df = dataframe('data8-engineering-project', 'Academy')
#         df[col] = pd.to_datetime(df[col])
#         return df

