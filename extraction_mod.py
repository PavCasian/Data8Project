import boto3
import pandas as pd
from datetime import datetime
from FileDictionary import get_file_names
from io import StringIO
import json
import re
from name_workshop_module import Typo


class ExtractToDF:

    """
    A class that imports from 'data8-engineering-project' bucket from amazon s3 cloud storage. The files in the
        specified folder will be concatenated to the same pandas dataframe.
        Input: a valid folder from 'data8-engineering-project' bucket
        Functions:
            from_csv - concatenates csv files from s3
            from_txt - concatenates txt files from s3
            from_json - concatenates json files from s3
            filter_name_col - asks for a dataframe and a string column of names to make sure that the names follow a
                standardised pattern (i.e. in title case, no special characters, surnames starting in patterns such as
                'Mc' or 'Mac' to have the first letter after the patterns capitalised, between-names patterns such as
                'Van', 'Der', 'De', 'Den' to be in lower case, removes unnecessary spaces)
    """

    def __init__(self, folder):
        self.folder = folder
        self.bucket = 'data8-engineering-project'
        self.file_names_dict = get_file_names(self.bucket, folder)
        self.s3_client = boto3.client('s3')

    def from_csv(self):
        df_list = []
        for record in self.file_names_dict[self.folder]:
            s3object = self.s3_client.get_object(Bucket=self.bucket,
                                            Key=self.folder + '/' + record)
            csv_string = s3object['Body'].read().decode('utf-8')  # utf-8 is a format
            df = pd.read_csv(StringIO(csv_string))  # read_csv wants a filepath or buffer(i.e. StringIO)
            df_list.append(df)
        main_df = pd.concat(df_list)
        main_df = self.filter_name_col(main_df, 'name').drop_duplicates()
        if self.folder == 'Talent':
            main_df['invited_by'] = Typo(main_df['invited_by']).replace_typos()  # class for identifying and correcting
            # typos, returns a pd series
            main_df = self.filter_name_col(main_df, 'invited_by')
        return main_df

    def from_txt(self):
        df_list = []
        for record in self.file_names_dict[self.folder]:
            data_dict = self.s3_client.get_object(Bucket=self.bucket,
                                        Key=self.folder + '/' + record)
            file = data_dict['Body']
            df1 = pd.read_csv(file, sep="\t", header=None,
                              skiprows=3)  # gets psychometrics and presentation marks for each name
            file.close()  # Have to  close and reassign file to read from it again to get date and academy
            data = self.s3_client.get_object(Bucket=self.bucket, Key=self.folder + '/' + record)
            file = data['Body']
            dateacdf = pd.read_csv(file, sep="\t", header=None,
                                   skiprows=lambda x: x not in [0, 1])  # gets the date and academy
            date = dateacdf.iloc[0][0]  # gets the date (which is in row 1 of dataframe created)
            date = datetime.strptime(date, "%A %d %B %Y")  # formats date
            academy = dateacdf.iloc[1][0]  # gets the academy name (second row of dataframe created)
            academy = academy.split(' ')[0]  # gets just the location name (takes out the word academy)
            df1['date'] = date
            df1['academy_name'] = academy
            splitdf = df1[0].str.rsplit("-", 1,
                                        expand=True)  # splits full name from everything else (splits on last - so '-' in names don't affect it)
            # expand into different columns
            scoredf = splitdf[1].str.split("/|:|,",
                                           expand=True)  # splits all the stats up score, max.score, name of assessment
            fulldf = pd.concat([splitdf[0], scoredf, df1['academy_name'], df1['date']],
                               axis=1)  # concatenates all the formatted created above so all columns are correct
            df_list.append(fulldf)  # adds the dataframe for this file to a list
        df = pd.concat(df_list)  # creates one dataframe for all the files that have been processed above
        df.columns = ["name", 'col1', 'psychometrics', 'psycho.max', 'col2', 'presentation', 'present.max',
                      'academy_name', 'date']
        df.drop(['col1', 'col2'], axis=1, inplace=True)
        df[['psychometrics', 'psycho.max', 'presentation', 'present.max']] = df[
            ['psychometrics', 'psycho.max', 'presentation',
             'present.max']].apply(pd.to_numeric)
        df = self.filter_name_col(df, 'name').drop_duplicates()
        return df

    def from_json(self):
        dict_list = []
        for _, record in enumerate(self.file_names_dict[self.folder]):
            s3object = self.s3_client.get_object(Bucket=self.bucket,
                                            Key=self.folder + '/' + record)  # retrieving data from s3 record by record
            bson_dict_file = s3object['Body'].read().decode('utf-8')  # reads in binary
            json_dict_file = json.loads(bson_dict_file)
            dict_list.append(json_dict_file)
        main_df = pd.DataFrame(dict_list)
        main_df = self.filter_name_col(main_df, 'name')
        return main_df

    # @staticmethod
    def filter_name_col(self, df, col):
        """the values in the name column in one file may not match with the other files' name columns"""
        list_names = []
        for _, name in df[col].iteritems():
            try:
                # eliminate the digits, dots, underscores in the name, make it title case and strip of padding
                name = re.sub(r'[.;0-9_]+', '', name).title().strip()
                # we want to capitalise the letter after word patterns such as Mac or Mc
                name = re.sub(r'\b(Mac|Mc)([a-z])', lambda pat: pat.group(1) + pat.group(2).upper(), name)
                # Dutch or French name patterns, such as van, de, den, should be lower case if between names
                name = re.sub(r'(\w\s)(Van(?:\sDer|\sDen)?|De)(\s\w)', lambda pat: pat.group(1) + pat.group(2).lower() +
                              pat.group(3), name)  # groups are marked by parentheses in the expression
                # removes spaces between '-': Lester Weddeburn - Scrimgeour
                name = re.sub("\s*(\W)\s*", r'\1', name)  # \W will match any non-word characters

            except TypeError:  # nans raise the exception, we still want to add them to the list
                pass
            list_names.append(name)
        df[col] = list_names
        return df

