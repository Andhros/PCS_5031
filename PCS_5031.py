import glob # glob is a file catcher for Python that enables path names as file names.

import pandas as pd # pandas is a DataFrame manipulation tool, in our case we will use pandas to extract a dataframe from
# our "messy" .csv files. Here we imported pandas "as pd" but it cand be any other predicate. It was only chosen to improve 
# code readability.

import numpy as np # importing NumPy even thou we're not using it yet. We'll probably use it if we need to do more advanced 
# calculations.

def processFile(filename): # defining a function to parse our .csv files
    print 'processing', filename
    # create DataFrame from CSV
    df = pd.read_csv(filename, sep=';') # our separators are the ';' in the csv files. This will remove them and understand 
    # them as the column separators itself
    return df

# get all filenames from the 'csv' directory
files = glob.glob('csv/*')

# files are now: ['file1.csv', 'file2.csv']

# process all files; here master is still a Python list.
master = map(processFile, files)
type(master) # checks the type of master object

# concatenate dataframes. This will concatenate all columns in a one single ungrouped dataframe
dataframe = pd.concat(master)
type(dataframe) # now the list is a pandas DataFrame

# print dataframe info summary
dataframe.info()

dataset = dataframe.groupby('CNPJ_FUNDO') # groups the dataframe by the CNPJ of the stakeholders
type(dataset) # now the dataframe is a "grouped dataframe object" (a 3 dimensional matrix)

# print mean values for every grouped CNPJ for all values regarding all months
print dataset.mean()

# print the 10 biggest mean values of 'VL_TOTAL' for every grouped CNPJ for all values regarding all months
print dataset.mean().sort_values('VL_TOTAL', ascending=False).head(10).filter(['CNPJ_FUNDO','VL_TOTAL'])

# separate the dataset in accessible CNPJ groups by number including all months
groups_cnpj = dict(list(dataset))

# print mean of X CNPJ for all values regarding all months
print groups_cnpj['23.731.397/0001-97'].mean()

# print statistics basics to all CNPJ groups
print dataset.describe()
