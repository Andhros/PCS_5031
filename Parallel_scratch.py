import glob

import pandas as pd

import numpy as np

import matplotlib.pyplot as plt

import os #not used

import multiprocessing # Package for parallelization

#os.chdir('/home/andhros/PythonProjects/DataScience_PCS_5031') 

def processFile(filename):
    print('parsing', filename)
    df = pd.read_csv(filename, sep=';')
    return df

files = glob.glob('csv/*')

df_denom = pd.read_csv('denom.csv', sep=',')

df_denom = df_denom.iloc[:, [0,1,5,8,17,20,21]]

master = map(processFile, files)

df_cvm = pd.concat(master)

df = pd.merge(df_cvm, df_denom, how = 'left', left_on = 'CNPJ_FUNDO', right_on = 'CNPJ_FUNDO')

df['DT_COMPTC'] = pd.to_datetime(df['DT_COMPTC'])

# Counting the number of threads minus 1 (sparing one processor to prevent cluttering the pc)
num_processes = multiprocessing.cpu_count()-1 

# calculating the size of the dataframe divisions named as "chunks"
chunk_size = int(df.shape[0]/num_processes) 

# actually dividing the dataframe into chunks (works even if the total index is not divisible by "num_processes")
chunks = [df.ix[df.index[i:i + chunk_size]] for i in range(0, df.shape[0], chunk_size)] 

# # ------------------------------------------------------------------------------------------------- #
# # This is the answer to Question 1 - do we need to sum the values or to apply mean to values?

df_1 = pd.DataFrame([])

data_holes_1 = 0

# Function to solve Question 1 only
def func(d) :
# removing dead funds
    for ind, row in d.iterrows() :
        if np.logical_and(row['SIT'] == 'EM FUNCIONAMENTO NORMAL', np.logical_and(row['CAPTC_DIA'] != 0,
                                                                              row['RESG_DIA'] != 0)) :
            df_1.loc[ind, 'DENOM_SOCIAL'] = row['DENOM_SOCIAL']
            df_1.loc[ind, 'VL_TOTAL'] = row['VL_TOTAL']
        else :
            data_holes = data_holes_1 + 1
    return(d)

# creation of multiprocessing Pool
pool = multiprocessing.Pool(processes=num_processes)

# applying the function to the chunks of the dataframe. Each chunk will generate its own process.
result = pool.map(func, chunks)

# converting the calculated chunks back into original dataframe format
for i in range(len(result)):

    df.ix[result[i].index] = result[i]

Pergunta_1 = df_1.groupby('DENOM_SOCIAL').mean().sort_values('VL_TOTAL', ascending=False).head(10).\
    filter(['DENOM_SOCIAL','VL_TOTAL'])

Pergunta_1.to_csv('Pergunta_1.csv')
