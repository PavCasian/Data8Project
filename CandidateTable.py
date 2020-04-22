from link_files import talentfile
from link_files import merge
import boto3
import pandas as pd
from Talent_Team import talentteammatcher
from split_name_auto import splitname
from link_files import namecon



class Candidate:

    def __init__(self):
        pass

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

    def candidates_table():
        df=talentfile()
        df = phonenoformat(df, 'phone_number')
        df = dateformat(df, 'dob')
        cc = namecon(coursecand())
        cc = cc.drop(['name'], axis=1)
        df = pd.merge(df, cc, how='left', on= 'namecon')
        df = df.replace('Bruno Belbrook', 'Bruno Bellbrook')
        df = df.replace('Fifi Etton', 'Fifi Eton')
        TT = talentteammatcher()
        df = pd.merge(df, TT, how='left', on='invited_by')
        df1 = splitname(df['name'])
        df1.columns = ['first_name', 'last_name']
        df =  pd.concat([df, df1], axis=1)
        df = df.drop(['namecon', 'id', 'invited_by', 'invited_date', 'month', 'name'], axis=1)
        df = df.fillna(0)
        return df
