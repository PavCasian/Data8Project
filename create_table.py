from files_to_dataframe import ExtractToDF
import pandas as pd
import numpy as np
import sys
import ast
import boto3 # delete later
from FileDictionary import files
from datetime import datetime
from name_workshop_module import SplitName, Typo
from sqlalchemy import create_engine
import urllib
import re
import time

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
        try:  # in case the specified file_type is incorrect
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
        df.to_sql(sql_table_name, con=Table.engine, if_exists='replace', index=False)


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
            spartans_group = spartans_group.assign(course_ID=course_id) # creates column CourseId and assigns value
            spartans_group = spartans_group.assign(course_name=course_name) # creates column CourseId and assigns value
            spartans_list.append(spartans_group)
        courses_df = pd.DataFrame(courses_list, columns=['course_ID', 'course_name', 'course_number',
                                                            'course_length_weeks', 'start_date', 'trainer'])
        # to make sure that there are no anomalies in trainers' names
        courses_df = ExtractToDF('Academy').filter_name_col(courses_df, 'trainer')

        spartans_df = pd.concat(spartans_list)
        # to match the name column with dfs from other files
        spartans_df = ExtractToDF('Academy').filter_name_col(spartans_df, 'name').drop_duplicates()
        return courses_df, spartans_df  # first returns [CourseID Course_Type Course_Number Start_Date
                                        # Course_Length Trainer];
                                        # second returns [name CourseId]

    def create_course_types_table(self):
        course_type_df = self.courses_df[['course_name']].drop_duplicates()
        course_type_df = self.add_ids_col(course_type_df, 'course_type_ID')
        return course_type_df[['course_type_ID', 'course_name']]

    def create_courses_table(self):
        courses_df = pd.merge(self.courses_df, self.create_course_types_table(), how='left', on='course_name')
        courses_df = pd.merge(courses_df, self.get_trainer(), how='left', on='trainer')
        return courses_df[['course_ID', 'course_type_ID', 'course_number', 'course_length_weeks', 'start_date',
                           'trainer_ID']]

    def get_trainer(self):  # trainer_ID with full name
        trainer_df = self.courses_df[['trainer']]
        df = trainer_df.drop_duplicates()
        df = self.add_ids_col(df, 'trainer_ID')
        return df

    def create_trainer_table(self):
        trainer_df = self.get_trainer().reset_index()  # without resetting,concatenation will create a df with number of
        # rows=nr of indices of prev 2 dfs,because some indices are present only in one df, those rows will contain nans
        trainer_df = pd.concat([trainer_df['trainer_ID'],
                               SplitName().split_full_name(trainer_df['trainer'])], axis=1)
        return trainer_df  # [trainer_ID, first_name, last_name]

    def export(self):
        self.load(self.create_courses_table(), 'Courses')
        self.load(self.create_trainer_table(), 'Trainers')
        self.load(self.create_course_types_table(), 'Course_Types')

# ins = Course().create_trainer_table()
# ins.export()
# print(ins.create_courses_table().info())

# df = ins.get_course_details()
# print(df[0].head(10))
# print(df[0].info())
# print(df[1].head(10))
# print(df[1].info())


class TalentTeam(Table):

    def __init__(self):
        super().__init__()

    def prepare_talent_team_table(self):
        """ Returns a table with unique full names, without typos, of recruiters column (i.e. 'invited_by)"""
        talent_data = self.extract('Talent', file_type='csv')
        recruiter = talent_data['invited_by']  # recruiter full name column
        recruiter_df = recruiter.dropna().drop_duplicates().to_frame()
        recruiter_df = self.add_ids_col(recruiter_df, 'talent_person_ID')
        return recruiter_df  # [invited_by, talent_person_ID]

    def create_talent_team_table(self):
        recruiter_df = self.prepare_talent_team_table().reset_index()  # without resetting, concatenation will produce
        # a df with number of rows =nr of indices of prev 2 dfs, because some indices are present only in one df, those
        # rows will contain nans
        # We want the talent_person_ID and their name to be separated into first and last name
        recruiter_df = pd.concat([recruiter_df['talent_person_ID'],
                                  SplitName().split_full_name(recruiter_df['invited_by'])], axis=1)
        return recruiter_df  # [talent_person_ID, first_name,   last_name]

    def export(self):
        self.load(self.create_talent_team_table(), 'Talent_Team')

ins = TalentTeam().prepare_talent_team_table()
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
        recruiter_table = TalentTeam().prepare_talent_team_table()
        df = pd.merge(self.full_name_cand_df, recruiter_table, how='left', on='invited_by')
        # print(df.head(50))
        df1 = SplitName().split_full_name(df['name'])
        df = pd.concat([df, df1], axis=1)
        # print(df.head(50))
        df.rename(columns={'dob': 'date_of_birth', 'address': 'candidate_address', 'phone_number': 'phone',
                           'uni': 'university'}, inplace=True)
        return df.loc[:, ['candidate_ID', 'first_name', 'last_name', 'gender', 'email', 'city', 'candidate_address',
                          'postcode', 'phone', 'university', 'degree', 'talent_person_ID']]

    def export(self):
        self.load(self.create_candidate_table(), 'Candidates')


# df = Candidate().create_candidate_table()
# print(time.perf_counter())
# Candidate().export()
# print(time.perf_counter())

class Assessment(Candidate):

    def __init__(self):
        super().__init__()
        self.sparta_day_df = self.extract('SpartaDays', file_type='txt')
        self.interview_df = self.prepare_assessment_table()

    def prepare_assessment_table(self):
        interview_df = self.extract('TransformedFiles') # replace with extract from json folder
        # to merge on date, both columns in the merging dfs need to be of the same type
        interview_df['date'] = pd.to_datetime(interview_df['date'], format="%d/%m/%Y")
        # interview_df has duplicates, same name and interview date,however strengths/weak/techs differ so will remove
        # duplicates based on the 'name' and 'date' column. It's highly unlikely to have same name on the same date
        interview_df.drop_duplicates(subset=['name', 'date'], inplace=True)
        # Will merge SpartaDays and Interview Notes as both include candidate assessment. Note that there are some
        # candidates in the sparta_day_df which are not in the interview_notes files, hence outer join used
        interview_df = pd.merge(interview_df, self.sparta_day_df, on=['name', 'date'], how='outer',
                                validate='one_to_one')
        # To obtain candidate_ID
        interview_df = pd.merge(interview_df, self.full_name_cand_df[['candidate_ID', 'name', 'interview_date']],
                                left_on=['name', 'date'], right_on=['name', 'interview_date'], how='left')
        return interview_df  # all the columns from Interview Notes(inc. strength, weakness, tech) and Sparta Day +
                             # candidate_ID, candidate_name and interview_date

    def create_interview_assessment_table(self):
        # To obtain academy_ID
        assess_df = pd.merge(self.interview_df, self.create_academy_table(), on='academy_name', how='left')
        # To obtain course_type_ID
        assess_df = pd.merge(assess_df, Course().create_course_types_table(), left_on='course_interest',
                             right_on='course_name', how='left')
        # rename columns
        assess_df.rename(columns={'psychometrics': 'psychometrics_score_max_100',
                                  'presentation': 'presentation_score_max_32',
                                  'financial_support_self': 'financial_support',
                                  'self-development': 'self_development'}, inplace=True)
        return assess_df[['candidate_ID', 'psychometrics_score_max_100', 'presentation_score_max_32', 'geo_flex',
                          'self_development', 'financial_support', 'result', 'course_type_ID', 'academy_ID',
                          'interview_date']]

    def create_academy_table(self):
        df = self.sparta_day_df[['academy_name']]
        df = df.drop_duplicates()
        df = self.add_ids_col(df, 'academy_ID')
        return df[['academy_ID', 'academy_name']]

    def export(self):
        self.load(self.create_interview_assessment_table(), 'Interview_Assessment')
        self.load(self.create_academy_table(), 'Academies')


# Assessment().export()


class StrengthWeaknessTechnology(Assessment):

    """Creates the following tables:
        Strengths, Weaknesses, Technologies, Candidate_Strengths, Candidate_Weaknesses, Candidate_Technologies
        Load them all in the sql database by calling the export function.

    """
    def __init__(self):
        super().__init__()
        self.unpacked_df = pd.DataFrame()
        self.from_column = ''  # 'strengths', 'weaknesses' or 'technologies'  specified by specify_col_names()
        self.col_name = ''  # depends on from_column
        self.col_id = ''  # depends on from_column

    def create_characteristic_candidate_table(self, characteristic_col):
        self.specify_col_names(characteristic_col)
        characteristic_df = self.create_characteristic_table(characteristic_col)
        characteristic_candidate_df = pd.merge(self.unpacked_df, characteristic_df, how='left', left_on=self.from_column,
                                        right_on=self.col_name)
        # to accommodate the self_score column in technologies
        if characteristic_col != 'technologies':
            characteristic_candidate_df = characteristic_candidate_df[['candidate_ID', self.col_id]]
        else:
            characteristic_candidate_df = characteristic_candidate_df[['candidate_ID', self.col_id, 'self_score']]
        return characteristic_candidate_df

    def specify_col_names(self, characteristic_col):
        if characteristic_col == 'strengths':
            self.from_column = 'strengths'
            self.col_name = 'strength_name'
            self.col_id = 'strength_ID'
        elif characteristic_col == 'weaknesses':
            self.from_column = 'weaknesses'
            self.col_name = 'weakness_name'
            self.col_id = 'weakness_ID'
        elif characteristic_col == 'technologies':
            self.from_column = 'technologies'
            self.col_name = 'technology_name'
            self.col_id = 'technology_ID'

    def create_characteristic_table(self, characteristic_col):  # pull all characteristic from_column in a df
        self.specify_col_names(characteristic_col)  # although the right column names were specified in the create_
        # _candidate_characteristic_table() already, specifying again makes the current function independent of the
        # order functions are called
        self.unpack_characteristic()  # need to unpack first and then able to work with them
        characteristic_df = self.unpacked_df.loc[:, [self.from_column]]
        characteristic_df = characteristic_df.drop_duplicates()  # returns a series of unique values
        characteristic_df.rename(columns={self.from_column: self.col_name}, inplace=True)
        characteristic_df = self.add_ids_col(characteristic_df, self.col_id)
        return characteristic_df  # characteristic name and its ID

    def unpack_characteristic(self):
        # creating a copy of relevant info from interview_df
        interview_df_with_cand_ids = self.interview_df.loc[:, ['candidate_ID', 'strengths', 'weaknesses', 'technologies']]
        # each candidate has str list for strengths/weaknesses e.g. '['Intolerant', 'Impulsive']' and list of dicts for
        # technologies [{'language': 'PHP', 'self_score': 4}, {'language': 'Python', 'self_score': 4}]
        # Few rows include nan, which cannt be parse by literal_eval, which removes the string quotes of the list, so we
        # replace nan with '[]' for consistency
        interview_df_with_cand_ids[self.from_column] = interview_df_with_cand_ids[self.from_column].fillna('[]').apply(ast.literal_eval)
        # take each column in df, except from_column, and repeat each row based on the number of list items in the
        # from_column row; to this assign the corresponding from_column values which were flatten (into a single list)
        self.unpacked_df = pd.DataFrame({col: np.repeat(interview_df_with_cand_ids[col].values,
                                                        interview_df_with_cand_ids[self.from_column].str.len())
                                        for col in interview_df_with_cand_ids.columns.drop(self.from_column)}
                                        ).assign(**{self.from_column:
                                                    np.concatenate(interview_df_with_cand_ids[self.from_column].values)})
        # now for technologies, each row contains one dict, we need to split each dictionary into 2 columns
        # (e.g. 'language' and 'self-score')
        if self.from_column == 'technologies':
            self.unpacked_df = pd.concat([self.unpacked_df.drop(['technologies'], axis=1),
                                         self.unpacked_df['technologies'].apply(pd.Series)], axis=1)
            self.from_column = 'language'  # for use in create_characteristic table as 'technologies' is not valid more

    def export(self):
        self.load(self.create_characteristic_candidate_table('strengths'), 'Candidate_Strengths')
        self.load(self.create_characteristic_table('strengths'), 'Strengths')
        self.load(self.create_characteristic_candidate_table('weaknesses'), 'Candidate_Weaknesses')
        self.load(self.create_characteristic_table('weaknesses'), 'Weaknesses')
        self.load(self.create_characteristic_candidate_table('technologies'), 'Candidate_Technologies')
        self.load(self.create_characteristic_table('technologies'), 'Technologies')

# print(StrengthWeaknessTechnology().export())


class Spartan(Course, Assessment):

    def __init__(self):
        super().__init__()

    def add_spartan_ids(self):
        spartan_df = self.get_course_details()[1]
        # we need the candidate id to replace spartan name, which are integrated already in interview_df
        candidate_df = self.interview_df.loc[:, ['candidate_ID', 'name', 'course_interest']]
        # to increase reliability merging could be done on both course_name and candidate name, however, not every
        # candidate in interview df has the course name specified (e.g. 'Matty Ceney'), as well as there are differences
        # between the candidates' course_interest in the interview and the actual course that they got
        # into (e.g. 'Hi Romke')
        spartan_df = pd.merge(spartan_df, candidate_df, how='left', on='name')
        spartan_df.rename(columns={'candidate_ID': 'spartan_ID'}, inplace=True)
        return spartan_df.loc[:, ['spartan_ID', 'name']]

    def create_spartan_table(self):
        spartan_df = self.add_spartan_ids()
        return spartan_df.loc[:, ['spartan_ID', 'course_ID']]

    def export(self):
        self.load(self.create_spartan_table(), 'Spartans')

# ins = Spartan()
# ins.export()

class AcademyCompetency(Spartan):

    """ A class that creates a table for one of the following competencies:
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
        comp_table = pd.merge(spartan_df, comp_df, how='right', on='name')
        return comp_table.drop(columns=['name'])

    def export(self):
        self.load(self.create_competency_table('IH'), 'Horsepower')
        self.load(self.create_competency_table('IS'), 'Interpersonal_Savvy')
        self.load(self.create_competency_table('PV'), 'Perseverance')
        self.load(self.create_competency_table('PS'), 'Problem_Solving')
        self.load(self.create_competency_table('SA'), 'Standing_Alone')
        self.load(self.create_competency_table('SD'), 'Self_Development')

# ins = AcademyCompetency()
# print(ins.export())
