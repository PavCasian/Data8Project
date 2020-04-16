import boto3
import pandas as pd
import pyodbc
#### to change for a class
from strength_weakness_tech import *
from AcademiesTable import *
from Trainers import *
from Talent_Team import *
from courses_table import *
from interview_assessment import *
from BehavioursTablesFINAL import *


#### export tables class ###

class ExportTables:
    ### connect to SQL server ###
    server = 'localhost,1433'
    database = 'sparta'
    username = 'SA'
    password = 'Passw0rd2018'
    docker_sparta = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = docker_sparta.cursor()

    cursor.execute("SELECT @@version;")

    ### to inharitate
    def __init__(self, function, insert_statement, *args):
        self.function = function
        self.insert_statement = insert_statement

        self.export_tables()

    def export_tables(self):
        for index, row in self.function.iterrows():
            # print(index,row) #Test option
            self.cursor.execute(self.insert_statement)

        self.docker_sparta.commit()

export_strengths = ExportTables(strengths(), "INSERT INTO SpartaDB.dbo.Strengths (strength_ID, strength_name) "
                                             "VALUES (?,?)", row['StrengthID'], row['StrengthName'])
export_weaknesses = ExportTables(weaknesses(),"INSERT INTO SpartaDB.dbo.Weaknesses (weakness_ID, weakness_name) "
                                              "VALUES (?,?)", row['WeaknessID'], row['WeaknessName'])
export_technologies = ExportTables(tech(), "INSERT INTO SpartaDB.dbo.Technologies (technology_ID, technology_name) "
                                           "VALUES (?,?)", row['TechID'], row['TechName'] )
export_academies = ExportTables(academiestable(), "INSERT INTO SpartaDB.dbo.Academies (academy_ID, academy_name) "
                                                 "VALUES (?,?)", row['AcademyID'], row['academy'])
export_trainers = ExportTables(trainerstable(), "INSERT INTO SpartaDB.dbo.Trainers (trainer_ID, first_name, last_name) "
                                                "VALUES (?,?,?)", row['TrainerID'], row['first_name'], row['last_name'])
export_talent = ExportTables(talentteamtable(), "INSERT INTO SpartaDB.dbo.Talent_Team (talent_person_ID, first_name, "
                                                "last_name) VALUES (?,?,?)", row['Talent_Team_ID'], row['first_name'], row['last_name'])
export_course_types = ExportTables(course_types(), "INSERT INTO SpartaDB.dbo.Course_Types (course_type_ID, course_name) "
                                                   "VALUES (?,?)", row['course_type_ID'], row['Course Type'])
export_courses = ExportTables(courses_table(), "INSERT INTO SpartaDB.dbo.Courses (course_ID, course_type_ID, "
                                               "course_number, course_length_weeks, start_date, trainer_ID) "
                                               "VALUES (?,?,?,?,?,?)", row['CourseID'], row['course_type_ID'],
                              row['Course Number'], row['Course_Length'], row['Start_Date'], row['TrainerID'])
export_interview_assesment = ExportTables(interviewnotes(), "INSERT INTO SpartaDB.dbo.Interview_Assesment (candidate_ID,"
                                                            " psychometrics_score, presentation_score, geo_flex, "
                                                            "self_development, financial_support, result, course_type_ID,"
                                                            " academy_ID, interview_date) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                          row['CandidateID'], row['psychometrics'], row['presentation'],
                                          row['geo_flex'], row['self_development'], row['financial_support_self'],
                                          row['result'], row['course_type_ID'], row['AcademyID'], row['date'])
export_IH = ExportTables(addIDs('IH'), "INSERT INTO SpartaDB.dbo.Horsepower (candidate_id, week_1, week_2, week_3, "
                                       "week_4, week_5, week_6, week_7, week_8, week_9, week_10) "
                                       "VALUES (?,?,?,?,?,?,?,?,?,?,?)", row['CandidateID'], row['IH_W1'], row['IH_W2'],
                         row['IH_W3'], row['IH_W4'], row['IH_W5'], row['IH_W6'], row['IH_W7'], row['IH_W8'],
                         row['IH_W9'], row['IH_W10'])
export_IS = ExportTables(addIDs('IS'), "INSERT INTO SpartaDB.dbo.Interpersonal_Savvy (candidate_id, week_1, week_2, "
                                       "week_3, week_4, week_5, week_6, week_7, week_8, week_9, week_10) "
                                       "VALUES (?,?,?,?,?,?,?,?,?,?,?)", row['CandidateID'], row['IS_W1'], row['IS_W2'],
                         row['IS_W3'], row['IS_W4'], row['IS_W5'], row['IS_W6'], row['IS_W7'], row['IS_W8'], row['IS_W9'],
                         row['IS_W10'])
export_PV = ExportTables(addIDs('PV'), "INSERT INTO SpartaDB.dbo.Perseverance (candidate_id, week_1, week_2, week_3, "
                                       "week_4, week_5, week_6, week_7, week_8, week_9, week_10) "
                                       "VALUES (?,?,?,?,?,?,?,?,?,?,?)", row['CandidateID'], row['PV_W1'], row['PV_W2'],
                         row['PV_W3'], row['PV_W4'], row['PV_W5'], row['PV_W6'], row['PV_W7'], row['PV_W8'],
                         row['PV_W9'], row['PV_W10'])
export_PS = ExportTables(addIDs('PS'), "INSERT INTO SpartaDB.dbo.Perseverance (candidate_id, week_1, week_2, week_3, "
                                       "week_4, week_5, week_6, week_7, week_8, week_9, week_10) "
                                       "VALUES (?,?,?,?,?,?,?,?,?,?,?)", row['CandidateID'], row['PS_W1'], row['PS_W2'],
                         row['PS_W3'], row['PS_W4'], row['PS_W5'], row['PS_W6'], row['PS_W7'], row['PV_W8'],
                         row['PV_W9'], row['PV_W10'])

