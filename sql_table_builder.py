import pyodbc


def create_sql_tables():
    # this is the connection:
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost,1433;DATABASE=master;UID=SA;'
                          'PWD=Passw0rd2018')
    cnxn.autocommit = True  # cannot drop database if it's False (default). Note that True means that some changes may be
    # saved but others not if a connection problem occurs during the transaction

    cursor = cnxn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS SpartaDB;"
                   "CREATE DATABASE SpartaDB;")  # When in the same transaction with USE SpartaDB, cannot identify db

    cursor.execute("""
    USE SpartaDB;
    
    -- Academies table
    DROP TABLE IF EXISTS Academies
    CREATE TABLE Academies(
        academy_ID INT,
        academy_name VARCHAR(20),
    );
    
    -- Trainers table
    DROP TABLE IF EXISTS Trainers
    CREATE TABLE Trainers(
        trainer_ID INT,
        first_name VARCHAR(30),
        last_name VARCHAR(30)
    );
    
    -- Course_Type table
    DROP TABLE IF EXISTS Course_Types
    CREATE TABLE Course_Types(
        course_type_ID INT,
        course_name VARCHAR(20)
    );
    
    -- Courses table
    DROP TABLE IF EXISTS Courses
    CREATE TABLE Courses(
        course_ID VARCHAR(10),
        course_type_ID INT,
        course_number INT,
        course_length_weeks INT,
        start_date DATE,
        trainer_ID INT
    );
    
    -- Talent Team table
    DROP TABLE IF EXISTS Talent_Team
    CREATE TABLE Talent_Team(
        talent_person_ID INT,
        first_name VARCHAR(30),
        last_name VARCHAR(30),
    );
    
    -- Candidates table
    DROP TABLE IF EXISTS Candidates
    CREATE TABLE Candidates (
        candidate_ID INT,
        first_name VARCHAR(30), -- in original separete the full name column to get this
        last_name VARCHAR(40),
        gender VARCHAR(10),
        date_of_birth DATE,
        email VARCHAR(100),
        city VARCHAR(40),
        candidate_address VARCHAR(100),
        postcode VARCHAR(10),
        phone VARCHAR(30),
        university VARCHAR(150),
        degree VARCHAR(10),
        talent_person_ID INT, -- in original is invited_by
        
    );
    
    -- Technologies table
    DROP TABLE IF EXISTS Technologies
    CREATE TABLE Technologies(
        technology_ID INT,
        technology_name VARCHAR(40)
    );
    
    -- Strenghths table
    DROP TABLE IF EXISTS Strengths
    CREATE TABLE Strengths(
        strength_ID INT,
        strength_name VARCHAR(40)
    );
    
    -- Weaknesses table
    DROP TABLE IF EXISTS Weaknesses
    CREATE TABLE Weaknesses(
        weakness_ID INT,
        weakness_name VARCHAR(40)
    );
    
    -- Candidate Technologies table
    DROP TABLE IF EXISTS Candidate_Technologies
    CREATE TABLE Candidate_Technologies(
        candidate_ID INT,
        technology_ID INT,
        self_score INT
    );
    
    -- strenghths table
    DROP TABLE IF EXISTS Candidate_Strengths
    CREATE TABLE Candidate_Strengths(
        candidate_ID INT,
        strength_ID INT
    );
    
    -- candidates weaknesses table
    DROP TABLE IF EXISTS Candidate_Weaknesses
    CREATE TABLE Candidate_Weaknesses(
        candidate_ID INT,
        weakness_ID INT
    );
    
    
    -- Interview Assessment table
    DROP TABLE IF EXISTS Interview_Assessment
    CREATE TABLE Interview_Assessment (
        candidate_ID INT,
        psychometrics_score_max_100 INT,
        presentation_score_max_32 INT,
        geo_flex VARCHAR(10),
        self_development VARCHAR(10),
        financial_support VARCHAR(10),
        result VARCHAR(10),
        course_type_ID INT,
        academy_ID INT,
        interview_date DATE
    );
    
    DROP TABLE IF EXISTS Spartans
    CREATE TABLE Spartans (
    spartan_ID INT,
    course_ID VARCHAR(10)
    );
    
    -- tables for trainee accessement 
    DROP TABLE IF EXISTS Horsepower
    CREATE TABLE Horsepower(
        candidate_id INT,
        week_1 INT,
        week_2 INT,
        week_3 INT,
        week_4 INT,
        week_5 INT,
        week_6 INT,
        week_7 INT,
        week_8 INT,
        week_9 INT,
        week_10 INT
    );
    
    DROP TABLE IF EXISTS Interpersonal_Savvy
    CREATE TABLE Interpersonal_Savvy(
        candidate_id INT,
        week_1 INT,
        week_2 INT,
        week_3 INT,
        week_4 INT,
        week_5 INT,
        week_6 INT,
        week_7 INT,
        week_8 INT,
        week_9 INT,
        week_10 INT
    );
    DROP TABLE IF EXISTS Perseverance
    CREATE TABLE Perseverance(
        candidate_id INT,
        week_1 INT,
        week_2 INT,
        week_3 INT,
        week_4 INT,
        week_5 INT,
        week_6 INT,
        week_7 INT,
        week_8 INT,
        week_9 INT,
        week_10 INT
    );
    
    
    DROP TABLE IF EXISTS Self_Development
    CREATE TABLE Self_Development(
        candidate_id INT,
        week_1 INT,
        week_2 INT,
        week_3 INT,
        week_4 INT,
        week_5 INT,
        week_6 INT,
        week_7 INT,
        week_8 INT,
        week_9 INT,
        week_10 INT
    );
    
    DROP TABLE IF EXISTS Standing_Alone
    CREATE TABLE Standing_Alone(
        candidate_id INT,
        week_1 INT,
        week_2 INT,
        week_3 INT,
        week_4 INT,
        week_5 INT,
        week_6 INT,
        week_7 INT,
        week_8 INT,
        week_9 INT,
        week_10 INT
    );
    
    DROP TABLE IF EXISTS Problem_Solving
    CREATE TABLE Problem_Solving(
        candidate_id INT,
        week_1 INT,
        week_2 INT,
        week_3 INT,
        week_4 INT,
        week_5 INT,
        week_6 INT,
        week_7 INT,
        week_8 INT,
        week_9 INT,
        week_10 INT
    );
    """)
    cnxn.close()

