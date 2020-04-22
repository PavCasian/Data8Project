from files_to_dataframe import ExtractToDF
import pandas as pd
import numpy as np
import sys
import ast
import boto3 # delete later
from FileDictionary import files
from datetime import datetime
from split_name_auto import split_full_name


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
        df['AcademyID'] = np.arange(1, len(df) + 1)
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


# ins = Course()
# df = ins.get_course_details()
# print(df[1])



import distance
class Typo:

    def __init__(self, name_series):
        self.name_series = name_series

    def find_typos(self):
        dict_list = {}  # contain the items in the column and their occurrences
        typo_correct_pair = []
        # adding to dict_list
        for _, name in self.name_series.iteritems():  # will get the index and the value for that column
            if name not in dict_list and name is not np.nan:  # not interested in nans
                dict_list[name] = 1
            elif name in dict_list:
                dict_list[name] += 1
        # adding to typo_correct_pair
        for to in dict_list:
            for against in dict_list:
                if distance.levenshtein(to, against) == 1:  # compares the difference in letters bw the 2 strings
                    if (against, to) not in typo_correct_pair:  # checking if same pair but diff order already exists
                        typo_correct_pair.append((to, against))
        return typo_correct_pair, dict_list

    def replace_typos(self):
        pairs, name_dict = self.find_typos()
        # check each pair and see which has more occurences
        for pair in pairs:
            frequentest_name = ''
            max_freq = 0
            for name in pair:
                if name_dict[name] > max_freq:
                    frequentest_name = name
                    max_freq = name_dict[name]
            # replace the less frequent with more frequent in name_series
            self.name_series.replace([nome for nome in pair if nome != frequentest_name], frequentest_name, inplace = True)
        return self.name_series




# tst = Table()
# df = tst.extract('Talent')
# typo_ins = Typo(df['invited_by'])
# typo_df = typo_ins.replace_typos()
# print(typo_df.unique())


class Recruiter(Table):

    def __init__(self):
        super().__init__()

    def prepare_recruiter_table(self):  # this function generates a table which is suitable for doing the merge based on name
        talent_data = self.extract('Talent', file_type='csv')
        recruiter = talent_data['invited_by']  # recruiter full name column
        recruiter = recruiter.dropna()
        recruiter_corrected = Typo(recruiter).replace_typos()  # class for identifying and correcting typos
        recruiter_df = recruiter_corrected.drop_duplicates().to_frame()
        recruiter_df['recruiter_id'] = np.arange(1, len(recruiter_df) + 1) # index starting from 1
        return recruiter_df

    def create_recruiter_table(self):
        recruiter_df = self.prepare_recruiter_table()
        recruiter_df = split_full_name(recruiter_df['invited_by'])
        recruiter_df= recruiter_df.drop(['invited_by'], axis=1)
        return recruiter_df

# ins = Recruiter().prepare_recruiter_table()
# print(ins)



# from link_files import talentfile
# from link_files import merge
# from Talent_Team import talentteammatcher
# from link_files import namecon


class Candidate(Table):

    def __init__(self):
        super().__init__()

    def phonenoformat(self, df, col):
        df = df
        df[col] = df[col].astype(str)
        chars = ' ()-'
        for c in chars:
            df[col] = df[col].str.replace(c, '')
        return df

    def dateformat(self, df,
                   col):  # takes in the bucket and folder and creates df using above function & formats column specified to date
        df = df
        df[col] = pd.to_datetime(df[col])
        return df

    def prepare_candidate_table(self):
        df = self.extract('Talent', file_type='csv')
        df = self.phonenoformat(df, 'phone_number')
        df = self.dateformat(df, 'dob')
        df.drop(['id'], axis = 1, inplace=True)  # in each file in 'Talent' indexing starts from 0
        df['CandidateID'] = np.arange(1, len(df) + 1)
        df.drop_duplicates(inplace=True)
        df = df.fillna(0)
        # cc = namecon(coursecand())
        # cc = cc.drop(['name'], axis=1)
        # df = pd.merge(df, cc, how='left', on= 'namecon')
        return df

    def create_candidate_table(self):
        df = self.prepare_candidate_table()
        recruiter_table = Recruiter().prepare_recruiter_table()
        df = pd.merge(df, recruiter_table, how='left', on='invited_by')
        # print(df.head())
        df1 = split_full_name(df['name'])
        df =  pd.concat([df, df1], axis=1)
        df = df.drop(['id', 'invited_by', 'invited_date', 'month', 'name'], axis=1)
        # df = df.fillna(0)
        return df

# df = Candidate().candidates_table()
# print(df.info())

class Spartan(Course, Candidate):

    def __init__(self):
        super().__init__()

    def create_spartan_table(self):
        spartan_df = self.get_course_details()[1]
        # we need the candidate id to replace spartan name, for this we merge with cand table on name
        candidate_df = self.prepare_candidate_table()[['CandidateID', 'name']]
        df = pd.merge(spartan_df, candidate_df, how='left', on='name')
        df.drop(['name'], axis=1, inplace=True)
        print(df)


# ins = Spartan()
# ins.create_spartan_table()

class Assessment(Candidate):

    def __init__(self):
        super().__init__()

    def prepare_assessment_table(self):
        candidate_df = self.prepare_candidate_table()
        assess_df = self.extract('TransformedFiles')
        assess_df.drop_duplicates(inplace=True)
        assess_df = pd.merge(assess_df, candidate_df, how='left', on='name')


        sparta_day_df = self.extract('SpartaDays', file_type='txt')
        sparta_day_df.drop_duplicates(inplace=True)
        sparta_day_df = pd.merge(sparta_day_df, candidate_df, how='left', on='name')

        assess_df = pd.merge(assess_df[['CandidateID', 'geo_flex', 'self_development', 'financial_support_self','result', 'course_interest']],
                             sparta_day_df[['CandidateID', 'academy', 'psychometrics', 'presentation','date']], on='CandidateID')
        assess_df.to_csv('assessment.csv')
        print(assess_df)


Assessment().prepare_assessment_table()