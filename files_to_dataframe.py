import boto3
import pandas as pd
from datetime import datetime
from FileDictionary import files
from io import StringIO
import json

class ExtractToDF:

    def __init__(self, folder):
        self.folder = folder
        self.bucket = 'data8-engineering-project'
        self.file_names_dict = files(self.bucket, folder)
        self.s3_client = boto3.client('s3')


    def from_csv(self):
        df_list =[]
        for record in self.file_names_dict[self.folder]:
            s3object = self.s3_client.get_object(Bucket=self.bucket,
                                            Key=self.folder + '/' + record)
            csv_string = s3object['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_string))  # read_csv wants a filepath or buffer(i.e. StringIO)
            df_list.append(df)
        main_df = pd.concat(df_list)
        return main_df

    def from_txt(self):
        df_list =[]
        for record in self.file_names_dict[self.folder]:
            data_dict = self.s3_client.get_object(Bucket=self.bucket,
                                        Key=self.folder + '/' + record)
            file = data_dict['Body']
            df1 = pd.read_csv(file, sep="\t", header=None,
                              skiprows=3)  # gets psychometrics and presentation marks for each name
            # for file in data['Body']:
            file.close()  # Have to  close and reassign file to read from it again to get date and academy
            data = self.s3_client.get_object(Bucket=self.bucket,
                                        Key=self.folder + '/' + record)
            file = data['Body']
            dateacdf = pd.read_csv(file, sep="\t", header=None,
                                   skiprows=lambda x: x not in [0, 1])  # gets the date and academy
            date = dateacdf.iloc[0][0]  # gets the date (which is in row 1 of dataframe created)
            date = datetime.strptime(date, "%A %d %B %Y")  # formats date
            academy = dateacdf.iloc[1][0]  # gets the academy name (second row of dataframe created)
            academy = academy.split(' ')[0]  # gets just the location name (takes out the word academy)
            df1['date'] = date
            df1['academy'] = academy
            splitdf = df1[0].str.rsplit("-", 1,
                                        expand=True)  # splits full name from everything else (splits on last - so '-' in names don't affect it)
            # expand into different columns
            scoredf = splitdf[1].str.split("/|:|,",
                                           expand=True)  # splits all the stats up score, max.score, name of assessment
            fulldf = pd.concat([splitdf[0], scoredf, df1['academy'], df1['date']],
                               axis=1)  # concatenates all the formatted created above so all columns are correct
            df_list.append(fulldf)  # adds the dataframe for this file to a list
        df = pd.concat(df_list)  # creates one dataframe for all the files that have been processed above
        df.columns = ["name", 'col1', 'psychometrics', 'psycho.max', 'col2', 'presentation', 'present.max',
                      'academy', 'date']
        df.drop(['col1', 'col2'], axis=1, inplace=True)
        df[['psychometrics', 'psycho.max', 'presentation', 'present.max']] = df[
            ['psychometrics', 'psycho.max', 'presentation',
             'present.max']].apply(pd.to_numeric)
        return df

    def from_json(self):
        df_list = []
        for index, record in enumerate(self.file_names_dict[self.folder]):
            s3object = self.s3_client.get_object(Bucket=self.bucket,
                                            Key=self.folder + '/' + record)
            bson_dict_file = s3object['Body'].read().decode('utf-8')  # reads in binary
            json_dict_file = json.loads(bson_dict_file)
            df_list.append(json_dict_file)
            # if index % 100 == 0:
            #     print(f"JSON Reader Progress: {index / len(self.file_names_dict['Interview Notes']) * 100:.2f}%")
        main_df = pd.DataFrame(df_list)
        return main_df


if __name__ == '__main__':
    test_instance = ExtractToDF('SpartaDays')
    # print(test_instance.filenames())
    test_csv = test_instance.from_txt()
    print(test_csv)
    # test_csv.to_csv('test_csv.csv')
