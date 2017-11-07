import glob

import pandas as pd

import numpy as np

import matplotlib.pyplot as plt

import os

os.chdir('/home/andhros/PythonProjects/DataScience_PCS_5031')

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

# df_grouped = df.groupby('DENOM_SOCIAL')
# ------------------------------------------------------------------------------------------------- #
# This is the answer to Question 1 - do we need to sum the values or to apply mean to values?

df_1 = pd.DataFrame([])

data_holes_1 = 0

# removing dead funds
for ind, row in df.sample(1000).iterrows() :
    if np.logical_and(row['SIT'] == 'EM FUNCIONAMENTO NORMAL', np.logical_and(row['CAPTC_DIA'] != 0,
                                                                              row['RESG_DIA'] != 0)) :
        df_1.loc[ind, 'DENOM_SOCIAL'] = row['DENOM_SOCIAL']
        df_1.loc[ind, 'VL_TOTAL'] = row['VL_TOTAL']
    else :
        data_holes = data_holes_1 + 1

Pergunta_1 = df_1.groupby('DENOM_SOCIAL').mean().sort_values('VL_TOTAL', ascending=False).head(10).\
    filter(['DENOM_SOCIAL','VL_TOTAL'])

# Pergunta_1 = df_1.groupby('DENOM_SOCIAL').sum().sort_values('VL_TOTAL', ascending=False).head(10).\
#     filter(['DENOM_SOCIAL','VL_TOTAL'])

Pergunta_1.to_csv('Pergunta_1.csv')
# ------------------------------------------------------------------------------------------------- #
# This is the Answer to Question 2

df_2 = pd.DataFrame([])

data_holes_2 = 0

# removing dead funds
for ind, row in df.iterrows() :
    if row['SIT'] == 'EM FUNCIONAMENTO NORMAL' :
        df_2.loc[ind, 'DENOM_SOCIAL'] = row['DENOM_SOCIAL']
        df_2.loc[ind, 'NR_COTST'] = row['NR_COTST']
    else :
        data_holes_2 = data_holes_2 + 1

Pergunta_2 = df_2.groupby('DENOM_SOCIAL').mean().sort_values('NR_COTST', ascending=False).head(10).\
    filter(['DENOM_SOCIAL','NR_COTST'])

Pergunta_2.to_csv('Pergunta_2.csv')
# ------------------------------------------------------------------------------------------------- #
# This is the Answer to Question 3

df_3 = pd.DataFrame([])

data_holes_3 = 0

# removing dead funds
for ind, row in df.sample(1000).iterrows() :
    if row['SIT'] == 'EM FUNCIONAMENTO NORMAL' :
        df_3.loc[ind, 'DENOM_SOCIAL'] = row['DENOM_SOCIAL']
        df_3.loc[ind, 'NR_COTST'] = row['NR_COTST']
    else :
        data_holes_3 = data_holes_3 + 1

Pergunta_3 = df_3.groupby('DENOM_SOCIAL').mean().sort_values('NR_COTST', ascending=False).head(10).\
    filter(['DENOM_SOCIAL','NR_COTST'])

# print(df.describe())
#
# df_3.boxplot()
# plt.show()

# It is possible to find outliers on this metric

Pergunta_3.to_csv('Pergunta_3.csv')
# ------------------------------------------------------------------------------------------------- #
# This is the Answer to Question 4

df_4 = pd.DataFrame([])

data_holes_4 = 0

# removing dead funds
for ind, row in df.iterrows() :
    if np.logical_and(row['SIT'] == 'EM FUNCIONAMENTO NORMAL', row['RESG_DIA'] - row['CAPTC_DIA'] < 0) :
        df_4.loc[ind, 'DENOM_SOCIAL'] = row['DENOM_SOCIAL']
        df_4.loc[ind, 'OSC_NEG'] = row['RESG_DIA'] - row['CAPTC_DIA']
    else :
        data_holes_4 = data_holes_4 + 1

Pergunta_4 = df_4.groupby('DENOM_SOCIAL').mean().sort_values('OSC_NEG', ascending=True).head(10).\
    filter(['DENOM_SOCIAL','OSC_NEG'])

Pergunta_4.to_csv('Pergunta_4.csv')
# ------------------------------------------------------------------------------------------------- #
# This is the Answer to Question 5 - The top 10 biggest withdraw by monthy mean

df_5 = pd.DataFrame([])

data_holes_5 = 0

# removing dead funds
for ind, row in df.iterrows() :
    if np.logical_and(row['SIT'] == 'EM FUNCIONAMENTO NORMAL', row['RESG_DIA'] > 0) :
        df_5.loc[ind, 'DENOM_SOCIAL'] = row['DENOM_SOCIAL']
        df_5.loc[ind, 'DT_COMPTC'] = row['DT_COMPTC']
        df_5.loc[ind, 'VOL_RES'] = row['RESG_DIA']
    else :
        data_holes_5 = data_holes_5 + 1

df_5 = df_5.set_index('DT_COMPTC')

Pergunta_5 = df_5.groupby('DENOM_SOCIAL').resample("M").mean().sort_values('VOL_RES', ascending=False).head(10)

Pergunta_5.to_csv('Pergunta_5.csv')
# ------------------------------------------------------------------------------------------------- #
# This is the Answer to Question 6 - The top 10 biggest yield by monthly mean

df_6 = pd.DataFrame([])

data_holes_6 = 0

# removing dead funds
for ind, row in df.iterrows() :
    if np.logical_and(row['SIT'] == 'EM FUNCIONAMENTO NORMAL', row['CAPTC_DIA'] > 0) :
        df_6.loc[ind, 'DENOM_SOCIAL'] = row['DENOM_SOCIAL']
        df_6.loc[ind, 'DT_COMPTC'] = row['DT_COMPTC']
        df_6.loc[ind, 'VOL_CAPT'] = row['CAPTC_DIA']
    else :
        data_holes_6 = data_holes_6 + 1

df_6 = df_6.set_index('DT_COMPTC')

Pergunta_6 = df_6.groupby('DENOM_SOCIAL').resample("M").mean().sort_values('VOL_CAPT', ascending=False).head(10)

Pergunta_6.to_csv('Pergunta_6.csv')
# ------------------------------------------------------------------------------------------------- #
# This is the Answer to Question 7

df_7 = pd.DataFrame([])

data_holes_7 = 0