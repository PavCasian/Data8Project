from files_to_dataframe import FileToDF
import pandas as pd
import numpy as np

def namecon(df):  # filters the names
    name_list = []
    for row in df.name:
        x = row.lower()
        name = ''
        for y in x:
            if y.isalnum():
                name += y
        name_list.append(name)
    df['namecon'] = name_list
    return df

def talentfile():  # returns a dataframe of the talent file with the candidate ID column
    file = FileToDF('data8-engineering-project', 'Talent')
    df = file.dataframecsv()
    df = namecon(df)
    df.drop('id', axis = 1)
    df['CandidateID'] = np.arange(len(df))
    return df


def merge(folder):  # merges the candidate ID onto whichever folder you are creating a dataframe from
    dfmerge = pd.merge(dataframe(folder), talentfile()[['CandidateID', 'namecon']], on='namecon')
    dfmerge = dfmerge.drop('namecon', axis =1)
    return dfmerge


def dataframe(x):  # creates a dataframe for whichever folder wanted (called within the merge function)
    file = FileToDF('data8-engineering-project', x)
    if x == 'TransformedFiles' or x == 'Academy':
        df = file.dataframecsv()
    elif x == 'SpartaDays':
        df = file.dataframetxt()
    else:
        pass
    return namecon(df)