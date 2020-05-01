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

    # def __init__(self):
        # self.talent_data = self.extract('Talent')
        # self.interview_data = self.extract('Interview Notes', file_type='json')
        # self.sparta_day_data = self.extract('SpartaDays', file_type='txt')
        # self.academy_data = self.extract('Academy')

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
        return df.assign(**{column_name: np.arange(1, len(df) + 1)})  # ids start from 1

    def load(self, df, sql_table_name):
        """Note that the columns in the dataframe need to match the column names
        and order of those in the sql table"""
        df.to_sql(sql_table_name, con=Table.engine, if_exists='append', index=False)


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


class Academy(Table):

    def __init__(self):
        super().__init__()

    def get_academy(self):
        df = self.extract('SpartaDays', file_type='txt')
        df = df[['academy_name']]
        df = df.drop_duplicates()
        df = self.add_ids_col(df, 'academy_ID')
        return df[['academy_ID', 'academy_name']]

    def export(self):
        self.load(self.get_academy(), 'Academies')

# test_inst = Academy()
# test_inst.export()

class Course(Table):

    def __init__(self):
        self.folder = 'Academy'
        self.courses_df, self.spartans_df = self.get_course_details()
        super().__init__()

    def get_course_details(self):
        s3_client = boto3.client('s3')
        file_names = files('data8-engineering-project', self.folder)
        courses_list = []
        spartans_list = []
        for file_name in file_names[self.folder]:  # file_name ex 'Business_25_2019-03-04.csv'
            file_name_split = file_name.split('_')  # ['Business', '25', '2019-03-04.csv']
            course_name = file_name_split[0]
            course_initial = course_name[0]
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
            course_details = (course_id, course_name, course_number, course_length, start_date, trainer_name)
            courses_list.append(course_details)
            # we also want the courseID and the spartan's name
            spartans_group = cohort_performance_df[['name']]
            spartans_group = spartans_group.assign(CourseId=course_id) # creates column CourseId and assigns value
            spartans_group = spartans_group.assign(course_name=course_name) # creates column CourseId and assigns value
            spartans_list.append(spartans_group)
        courses_df = pd.DataFrame(courses_list, columns=['course_ID', 'course_name', 'course_number',
                                                            'course_length_weeks', 'start_date', 'trainer'])
        spartans_df = pd.concat(spartans_list)
        return courses_df, spartans_df  # first returns [CourseID Course_Type Course_Number Start_Date
                                        # Course_Length Trainer];
                                        # second returns [name CourseId]

    def create_course_types_table(self):
        course_type_df = self.courses_df[['course_name']].drop_duplicates()
        course_type_df = self.add_ids_col(course_type_df, 'course_type_ID')
        return course_type_df

    def create_courses_table(self):
        courses_df = pd.merge(self.courses_df, self.create_course_types_table(), how='left', on='course_name')
        courses_df = pd.merge(courses_df, self.get_trainer(), how='left', on='trainer')
        return courses_df[['course_ID', 'course_type_ID', 'course_number', 'course_length_weeks', 'start_date',
                           'trainer_ID']]

    def get_trainer(self): # for matching IDs with
        trainer_df = self.courses_df[['trainer']]
        df = trainer_df.drop_duplicates()
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
        self.load(self.courses_df, 'Courses')

# ins = Course()
# print(ins.create_courses_table())
# print(ins.create_courses_table().info())

# df = ins.get_course_details()
# print(df[0].head(10))
# print(df[0].info())
# print(df[1].head(10))
# print(df[1].info())


class Recruiter(Table):

    def __init__(self):
        super().__init__()

    def prepare_recruiter_table(self):
        """ Returns a table with unique full names, without typos, of recruiters column (i.e. 'invited_by)"""
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
        recruiter_df = recruiter_df.drop(['invited_by'], axis=1)
        return recruiter_df

# ins = Recruiter().prepare_recruiter_table()
# print(ins)


class Candidate(Table):

    def __init__(self):
        super().__init__()
        self.full_name_cand_df = self.prepare_candidate_table()

    def phonenoformat(self, df, col):
        df = df
        df[col] = df[col].astype(str)
        chars = ' ()-'
        for c in chars:
            df[col] = df[col].str.replace(c, '')
        return df

    def prepare_candidate_table(self):
        df = self.extract('Talent', file_type='csv')
        df = self.phonenoformat(df, 'phone_number')
        df['interview_date'] = pd.to_datetime(df['invited_date'].astype('Int64').map(str) + ' ' + df['month'].map(str),
                                              errors='coerce')  # format='%d %B %Y'  &  '%d %b %Y'
        df.drop(['id'], axis=1, inplace=True)  # in each file in 'Talent' indexing starts from 0
        df.drop_duplicates(inplace=True)
        df = self.add_ids_col(df, 'candidate_ID')
        return df

    def create_candidate_table(self):
        recruiter_table = Recruiter().prepare_recruiter_table()
        df = pd.merge(self.full_name_cand_df, recruiter_table, how='left', on='invited_by')
        df1 = split_full_name(df['name'])
        df = pd.concat([df, df1], axis=1)
        df = df.drop(['id', 'invited_by', 'invited_date', 'month', 'name'], axis=1)
        return df

    def add_candidate_id(self, to_df):
        merged_df = pd.merge(to_df, self.full_name_cand_df[['candidate_ID', 'name']], how='left', on='name')
        return merged_df


# df = Candidate().candidates_table()
# print(df.info())

class Spartan(Course, Candidate):

    def __init__(self):
        super().__init__()


    def add_spartan_ids(self):
        spartan_df = self.get_course_details()[1]
        # print(spartan_df.head(10))
        # print('##', spartan_df.info())
        # we need the candidate id to replace spartan name, for this we merge with cand table on name
        candidate_df = self.prepare_candidate_table()[['candidate_ID', 'name']]
        spartan_df = pd.merge(spartan_df, candidate_df, how='left', on='name')
        return spartan_df

    def create_spartan_table(self):
        spartan_df = self.add_spartan_ids()
        return spartan_df.drop(['name'], axis=1, inplace=True)


# ins = Spartan()
# ins.create_spartan_table()

class Assessment(Candidate):

    def __init__(self):
        super().__init__()
        self.sparta_day_df = self.extract('SpartaDays', file_type='txt')


    def prepare_assessment_table(self):
        interview_df = self.extract('TransformedFiles')
        # to merge on date, both columns in the merging dfs need to be of the same type
        interview_df['date'] = pd.to_datetime(interview_df['date'], format="%d/%m/%Y")
        # interview_df has duplicates, same name and interview date,however strengths/weak/techs differ so will remove
        # duplicates based on the 'name' and 'date' column. It's highly unlikely to have same name on the same date
        interview_df = interview_df.drop_duplicates(subset=['name', 'date'])
        # Will merge SpartaDays and Interview Notes as both include candidate assessment. Note that there are some
        # candidates in the sparta_day_df which are not in the interview_notes files, hence outer join used
        assess_df = pd.merge(interview_df, self.sparta_day_df, on=['name', 'date'], how='outer',
                             validate='one_to_one')
        # To obtain candidate_ID
        assess_df = pd.merge(assess_df, self.full_name_cand_df[['candidate_ID', 'name', 'interview_date']],
                             left_on=['name', 'date'], right_on=['name', 'interview_date'], how='left')
        # To obtain academy_ID
        assess_df = pd.merge(assess_df, self.create_academy_table(), on='academy_name', how='left')
        # To obtain course_type_ID
        assess_df = pd.merge(assess_df, Course().create_course_types_table(), left_on='course_interest',
                             right_on='course_name', how='left')
        # rename some columns
        {### continue her}
        return assess_df[['candidate_ID', 'psychometrics_score', 'presentation_score', 'geo_flex', 'self-development',
                            'financial_support', 'result', 'course_type_ID', 'academy_ID', 'interview_date']]

    def create_academy_table(self):
        df = self.sparta_day_df[['academy_name']]
        df = df.drop_duplicates()
        df = self.add_ids_col(df, 'academy_ID')
        return df[['academy_ID', 'academy_name']]

    def export(self):
        self.load(self.prepare_assessment_table(), 'Interview_Assessment')


Assessment().export()

class StrengthWeaknessTechnology(Candidate):

    def __init__(self, from_column): # 'strengths', 'weaknesses' or 'technologies'
        super().__init__()
        self.from_column = from_column
        self.assess_df = self.extract('TransformedFiles', file_type='csv')
        self.unpacked_df = pd.DataFrame()
        if self.from_column == 'strengths':
            self.col_name = 'StrengthName'
            self.col_id = 'StrengthID'
        elif self.from_column == 'weaknesses':
            self.col_name = 'WeaknessName'
            self.col_id = 'WeaknessID'
        elif self.from_column == 'technologies':
            self.col_name = 'TechName'
            self.col_id = 'TechID'

    def create_characteristic_table(self):  # pull all characteristic from_column in a df
        self.unpack_characteristic()  # need to unpack first and then able to work with them
        characteristic_df = self.unpacked_df[[self.from_column]]
        characteristic_df = characteristic_df.drop_duplicates()  # returns a series of unique values
        characteristic_df.rename(columns={self.from_column: self.col_name}, inplace=True)
        characteristic_df = self.add_ids_col(characteristic_df, self.col_id)
        return characteristic_df

    def unpack_characteristic(self):
        # each candidate has str list for strengths/weaknesses e.g. '['Intolerant', 'Impulsive']' and list of dicts for
        # for technologies [{'language': 'PHP', 'self_score': 4}, {'language': 'Python', 'self_score': 4}]
        # literal_eval removes the string quotes of the list
        self.assess_df[self.from_column] = self.assess_df[self.from_column].apply(ast.literal_eval)
        assess_df_ids = self.add_candidate_id(self.assess_df)
        # take each column in df, except from_column, and repeat each row based on the number of list items in the
        # from_column row; to this assign the corresponding from_column values which were flatten (into a single list)
        self.unpacked_df = pd.DataFrame({col: np.repeat(assess_df_ids[col].values,
                                                        assess_df_ids[self.from_column].str.len())
                               for col in assess_df_ids.columns.drop(self.from_column)}
                              ).assign(**{self.from_column: np.concatenate(assess_df_ids[self.from_column].values)})
        # now for technologies, each row contains one dict, we need to split each dictionary into 2 columns
        # (e.g. 'language' and 'self-score')
        if self.from_column == 'technologies':
            self.unpacked_df = pd.concat([self.unpacked_df.drop(['technologies'], axis=1),
                                     self.unpacked_df['technologies'].apply(pd.Series)], axis=1)
            self.from_column = 'language'  # for use in create_characteristic table as 'technologies' is not valid more


    def create_characteristic_candidate_table(self):
        charact_df = self.create_characteristic_table()
        charact_candidate_df = pd.merge(self.unpacked_df, charact_df, how='left', left_on=self.from_column,
                                        right_on=self.col_name)
        return charact_candidate_df

import re
class AcademyCompetency(Spartan):

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


# ins = AcademyCompetency()
# print(ins.create_competency_table('IS'))
