import pandas as pd
import re
# from files_to_dataframe import ExtractToDF
import distance
import numpy as np


class Typo:

    """ A class for checking one-letter typos in a series that contains names usually.

        find_typos() will iterate through each row in the series and compute the frequency of each value, using a dictionary as a registry.
         Then compares each dict key(i.e. the name) against other keys in the dict. If there is a discrepancy of 1
         character between the two, then this will be recorded in a list of tuples of both keys.
                Returns both the registry dictionary and the list of tuples

        replace_typos() makes use of find_typos's output. Iterates through each pair in the tuple list and decides which
         is the typo based on which is least frequent in the registry dictionary. Next, the typo in the series is
         replaced with its more frequent pair.
                Returns a pandas series
          """
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
            most_frequent_name = ''
            max_freq = 0
            for name in pair:
                if name_dict[name] > max_freq:
                    most_frequent_name = name
                    max_freq = name_dict[name]
            # replace the less frequent with more frequent in name_series
            self.name_series.replace([nome for nome in pair if nome != most_frequent_name], most_frequent_name, inplace=True)
        return self.name_series

# optimization method: use pandas str.split function which accepts a regex


class SplitName:

    def __init__(self):
        self.split_before_list = ['O', "O'", 'Mc', 'Van', 'Von', 'Di', 'Degli', 'Dell', 'De', 'Le', 'Du', 'St', 'La',
                                  'Ten']

    @staticmethod
    def common_item(a, b):
        a_set = set(a)
        b_set = set(b)
        if len(a_set.intersection(b_set)) > 0:
            return True
        return False

    def split_full_name(self, name_series):  # takes in the name column of a dataframe
        name_list = []
        for row in name_series:  # goes through each row of the file being uploaded
            row = re.sub(r"\s*(\W)\s*", r"\1", row)  # removes spaces between '-': Lester Weddeburn - Scrimgeour
            row = re.sub(r"(.)\..*", r"\1", row)  # removes dots Chrisse Santostefano.
            if row.count(' ') > 1:  # if there are more than 2 spaces
                # if the name contains words from this list split before them
                splitted_name = row.split(' ')
                if SplitName.common_item(self.split_before_list, splitted_name):
                    for i, part in enumerate(splitted_name):
                        if part in self.split_before_list:

                            name_list.append([' '.join(splitted_name[:i]), ' '.join(splitted_name[i:])])
                            break
                else:
                    name_list.append([' '.join(splitted_name[:-1]), splitted_name[-1]])
            else:
                name_list.append(row.split(" ", 1))  # automatically splits name into first and last and adds to list

        namedf = pd.DataFrame(name_list)  # creates a dataframe from the name list created above
        namedf.columns =['first_name', 'last_name']
        return namedf  # returns a dataframe of first and last name


if __name__ == '__main__':
    extraction_instance = ExtractToDF('Talent')
    df = extraction_instance.from_csv()
    print(SplitName().split_full_name(df['name']))