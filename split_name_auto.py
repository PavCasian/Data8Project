import pandas as pd
import re
from files_to_dataframe import ExtractToDF

# optimization method: use pandas str.split function which accepts a regex
split_before_list = ['O', "O'", 'Mc', 'Van', 'Von', 'Di', 'Degli', 'Dell', 'De', 'Le', 'Du', 'St', 'La', 'Ten']

def common_item(a, b):
    a_set = set(a)
    b_set = set(b)
    if len(a_set.intersection(b_set)) > 0:
        return(True)
    return(False)

def split_full_name(name_series):  # takes in the name column of a dataframe
    name_list=[]
    for row in name_series:  # goes through each row of the file being uploaded
        row = row.strip()  # strips any whitespace off beginning and end
        row = row.title()
        row = re.sub("\s*(\W)\s*",r"\1",row)  # removes spaces between '-': Lester Weddeburn - Scrimgeour
        row = re.sub("(.)\..*",r"\1",row) # removes dots Chrisse Santostefano.
        if row.count(' ') > 1:  # if there are more than 2 spaces
            # if the name contains words from this list split before them
            ## first we split by spaces and then if includes what's in the list then split before it
            # how to identify the number of the space?
            splitted_name = row.split(' ')
            if common_item(split_before_list, splitted_name):
                for i, part in enumerate(splitted_name):
                    if part in split_before_list:

                        name_list.append([' '.join(splitted_name[:i]), ' '.join(splitted_name[i:])])
                        break
            else:
                name_list.append([' '.join(splitted_name[:-1]), splitted_name[-1]])
        else:
            name_list.append(row.split(" ", 1))  # automatically splits name into first and last and adds to list

    namedf = pd.DataFrame(name_list)  # creates a dataframe from the name list created above
    namedf.columns =['first_name','last_name']
    return namedf  # returns a dataframe of first and last name



if __name__ == '__main__':
    extraction_instance = ExtractToDF('Talent')
    df = extraction_instance.from_csv()
    print(split_full_name(df['name']))



