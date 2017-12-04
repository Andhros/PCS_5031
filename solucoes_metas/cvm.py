import pandas as pd
from string import ascii_letters
import numpy as np
import seaborn as sns
import matplotlib.pyplot as pl


import os
os.chdir('PATH_TO_CSV_FILES')


# https://www.shanelynn.ie/summarising-aggregation-and-grouping-data-in-python-pandas/
# http://chris.friedline.net/2015-12-15-rutgers/lessons/python2/03-data-types-and-format.html
# https://stackoverflow.com/questions/14059094/i-want-to-multiply-two-columns-in-a-pandas-dataframe-and-add-the-result-into-a-n
# https://stackoverflow.com/questions/33282119/pandas-filter-dataframe-by-another-dataframe-by-row-elements
# https://community.modeanalytics.com/python/tutorial/pandas-groupby-and-python-lambda-functions/

inf_diario_2017 = pd.read_csv("__inf_diario_2017_JOINED__.csv", sep=";", encoding="ISO-8859-1") # inf_diario_2017.shape # (2860856, 9)
fdo_invsti_regstr = pd.read_csv("__fdo_invsti_regstr_UNIQUE_JOINED__.csv", sep=";", encoding="ISO-8859-1")
fdo_invsti_regstr_summary = fdo_invsti_regstr[['CNPJ_FUNDO', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'ADMIN']]


# FILTRANDO numero de cotistas
inf_diario_2017['NR_COTST'] = inf_diario_2017['NR_COTST'].str.replace(',', '')
inf_diario_2017['NR_COTST'] = inf_diario_2017['NR_COTST'].astype(int)

inf_diario_2017['CAPTC_DIA'] = inf_diario_2017['CAPTC_DIA'].str.replace(',', '')
inf_diario_2017['CAPTC_DIA'] = inf_diario_2017['CAPTC_DIA'].astype('float64')

inf_diario_2017['RESG_DIA'] = inf_diario_2017['RESG_DIA'].str.replace(',', '')
inf_diario_2017['RESG_DIA'] = inf_diario_2017['RESG_DIA'].astype('float64')

inf_diario_2017_FILTERED = inf_diario_2017.groupby('CNPJ_FUNDO', as_index=False).agg( {"NR_COTST": ['mean'], "CAPTC_DIA": ['mean'], "RESG_DIA": ['mean']} )
inf_diario_2017_FILTERED.columns = inf_diario_2017_FILTERED.columns.droplevel(level=1)
inf_diario_2017_FILTERED = inf_diario_2017_FILTERED.rename(columns={"CAPTC_DIA": "CAPTC_DIA_MEAN", "RESG_DIA": "RESG_DIA_MEAN", "NR_COTST": "NR_COTST_MEAN"})


inf_diario_2017_FILTERED = inf_diario_2017_FILTERED.loc[inf_diario_2017_FILTERED['NR_COTST_MEAN'] >= 10 ]
inf_diario_2017_FILTERED = inf_diario_2017_FILTERED.loc[inf_diario_2017_FILTERED['CAPTC_DIA_MEAN'] > 0.0 ]
inf_diario_2017_FILTERED = inf_diario_2017_FILTERED.loc[inf_diario_2017_FILTERED['RESG_DIA_MEAN'] > 0.0 ]


inf_diario_2017_FILTERED = inf_diario_2017_FILTERED.round( { 'NR_COTST_MEAN': 2 } )

# NR_COTST Mean
ctstMean = inf_diario_2017_FILTERED.copy()

ix1 = inf_diario_2017.set_index(['CNPJ_FUNDO']).index
ix2 = inf_diario_2017_FILTERED.set_index(['CNPJ_FUNDO']).index

temp = inf_diario_2017[ix1.isin(ix2)]
temp = temp.drop('ID', axis=1)


# ctstMean.sort_values(by=['NR_COTST_MEAN'], ascending=True, inplace=True)
# one = ctstMean.loc[ctstMean['CNPJ_FUNDO'] == '13.199.100/0001-30']
# one = inf_diario_2017_FILTERED.loc[inf_diario_2017_FILTERED['CNPJ_FUNDO'] == '13.199.100/0001-30']

# ---------------------------------------------- Questao 2 ----------------------------------------------
#                                                                                                       #
# -------------------------------------------------------------------------------------------------------

grouped = temp.groupby('CNPJ_FUNDO', as_index=False).agg({"DT_COMPTC": [min, max]})
grouped = grouped.rename(columns={"min": "DT_COMPTC", "max": "DT_COMPTC_MAX", "": "CNPJ_FUNDO"})
grouped.columns = grouped.columns.droplevel(level=0)

i1 = inf_diario_2017.set_index(['CNPJ_FUNDO', 'DT_COMPTC']).index
i2 = grouped.set_index(['CNPJ_FUNDO',  'DT_COMPTC']).index

min_vl_quota = inf_diario_2017[i1.isin(i2)] #min_vl_quota.sort_values(by=['DT_COMPTC'], ascending=False)

grouped.reset_index() # normalized_group.reset_index(drop=True)

grouped = grouped.rename(columns={"DT_COMPTC": "DT_COMPTC_MIN"})
grouped = grouped.rename(columns={"DT_COMPTC_MAX": "DT_COMPTC"})

i2 = grouped.set_index(['CNPJ_FUNDO', 'DT_COMPTC']).index

max_vl_quota = inf_diario_2017[i1.isin(i2)] #max_vl_quota.sort_values(by=['DT_COMPTC'], ascending=False)

# TODO select the interests columns on process beginning.
min_vl_quota = min_vl_quota.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC', 'VL_TOTAL', 'VL_QUOTA', 'VL_PATRIM_LIQ']]
max_vl_quota = max_vl_quota.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC', 'VL_TOTAL', 'VL_QUOTA', 'VL_PATRIM_LIQ']]

result = min_vl_quota.merge(max_vl_quota, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='inner', suffixes=('_MIN', '_MAX'))

result['VL_QUOTA_MIN'] = result['VL_QUOTA_MIN'].str.replace(',', '')
result['VL_QUOTA_MAX'] = result['VL_QUOTA_MAX'].str.replace(',', '')

result['VL_QUOTA_MIN'] = result['VL_QUOTA_MIN'].astype('float64')
result['VL_QUOTA_MAX'] = result['VL_QUOTA_MAX'].astype('float64')

result = result.round( { 'VL_QUOTA_MIN': 2, 'VL_QUOTA_MAX' : 2 } )
result = result.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC_MIN', 'VL_QUOTA_MIN', 'DT_COMPTC_MAX', 'VL_QUOTA_MAX']]

result_copy = result.copy()

result = result.loc[result['VL_QUOTA_MIN'] > 0.0 ]
result = result.loc[result['VL_QUOTA_MAX'] > 0.0 ]

# Calculating
result['RENTAB_BRUTA_ACMULAD'] = result.VL_QUOTA_MAX - result.VL_QUOTA_MIN
result['RENTAB_BRUTA_ACMULAD'] = result['RENTAB_BRUTA_ACMULAD'].astype('float64')
result = result.round( { 'RENTAB_BRUTA_ACMULAD': 2 } )

result['RENTAB_BRUTA_ACMULAD_PERCENT'] = result['RENTAB_BRUTA_ACMULAD'] * 100 / result.VL_QUOTA_MIN
result = result.round( { 'RENTAB_BRUTA_ACMULAD_PERCENT': 2 } )
result.sort_values(by=['RENTAB_BRUTA_ACMULAD_PERCENT'], ascending=False, inplace=True)


# Merge with NR_COTST Mean
pergunta2 = result.merge(ctstMean, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='inner')


# Merge with Denominacao do Fundo
pergunta2 = pergunta2.merge(fdo_invsti_regstr, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
pergunta2 = pergunta2.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC_MIN', 'VL_QUOTA_MIN', 'DT_COMPTC_MAX', 'VL_QUOTA_MAX', 'RENTAB_BRUTA_ACMULAD','RENTAB_BRUTA_ACMULAD_PERCENT', 'NR_COTST_MEAN', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'RENTAB_FUNDO', 'ADMIN']]
pergunta2 = pergunta2.loc[pergunta2['SIT'] == 'EM FUNCIONAMENTO NORMAL' ]
pergunta2.sort_values(by=['RENTAB_BRUTA_ACMULAD_PERCENT'], ascending=False, inplace=True )


pergunta2.head(20).to_csv("__Pergunta_2__.csv", sep=";", encoding="ISO-8859-1")


# Generate csv for each top 20
def generatecsv(CNPJ_FUNDO):
    suffix = CNPJ_FUNDO.replace(".", "_").replace("-", "_").replace("/", "_") + ".csv"
    csvName = "__Pergunta_2__" +  suffix
    res = inf_diario_2017.loc[inf_diario_2017['CNPJ_FUNDO'] == CNPJ_FUNDO]
    res.to_csv(csvName, sep=";", encoding="ISO-8859-1")


for index, row in pergunta2.head(20).iterrows(): generatecsv ( row['CNPJ_FUNDO'] )





# ---------------------------------------------- Questao 4 ----------------------------------------------
#                                                                                                       #
# -------------------------------------------------------------------------------------------------------


inf_diario_2017_oscl_neg = temp.copy()

inf_diario_2017_oscl_neg = inf_diario_2017_oscl_neg.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA']]
inf_diario_2017_oscl_neg.sort_values(by=['CNPJ_FUNDO', 'DT_COMPTC'], inplace=True)
inf_diario_2017_oscl_neg['VL_QUOTA'] = inf_diario_2017_oscl_neg['VL_QUOTA'].str.replace(',', '')
inf_diario_2017_oscl_neg['VL_QUOTA'] = inf_diario_2017_oscl_neg['VL_QUOTA'].astype('float64')

# inf_diario_2017_oscl['VL_QUOTA_OSC_NEG_PERCENT'] = inf_diario_2017_oscl.groupby(['CNPJ_FUNDO'])['VL_QUOTA'].transform(pd.Series.diff)
inf_diario_2017_oscl_neg['VL_QUOTA_OSC_NEG_PERCENT'] = inf_diario_2017_oscl_neg.groupby(['CNPJ_FUNDO'])['VL_QUOTA'].pct_change()
inf_diario_2017_oscl_neg['VL_QUOTA_OSC_NEG_PERCENT'] = inf_diario_2017_oscl_neg['VL_QUOTA_OSC_NEG_PERCENT'].fillna(0)

# one = inf_diario_2017_oscl_neg.loc[inf_diario_2017_oscl_neg['CNPJ_FUNDO'] == '97.711.812/0001-87']


# Summarizing


a = inf_diario_2017_oscl_neg.groupby('CNPJ_FUNDO').apply(lambda x: pd.Series(dict(QTD_OCCUR = x.CNPJ_FUNDO.count())))
b = inf_diario_2017_oscl_neg[inf_diario_2017_oscl_neg.VL_QUOTA_OSC_NEG_PERCENT < 0.0].groupby('CNPJ_FUNDO').apply(
    lambda x: pd.Series(
        dict(
            QTD_VL_QUOTA_OSC_NEG_PERCENT = x.VL_QUOTA_OSC_NEG_PERCENT.count(),
            TOTAL_VL_QUOTA_OSC_NEG_PERCENT = x.VL_QUOTA_OSC_NEG_PERCENT.sum()
        )
    )
)


a = a.reset_index()
b = b.reset_index()

one = inf_diario_2017_oscl_neg.loc[inf_diario_2017_oscl_neg['CNPJ_FUNDO'] == '09.454.944/0001-03']

inf_diario_2017_oscl_neg_summary = b.merge(a, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
inf_diario_2017_oscl_neg_summary['QTD_VL_QUOTA_OSC_NEG_PERCENT'] = inf_diario_2017_oscl_neg_summary['QTD_VL_QUOTA_OSC_NEG_PERCENT'].fillna(0)
inf_diario_2017_oscl_neg_summary['TOTAL_VL_QUOTA_OSC_NEG_PERCENT'] = inf_diario_2017_oscl_neg_summary['TOTAL_VL_QUOTA_OSC_NEG_PERCENT'].fillna(0)

inf_diario_2017_oscl_neg_summary['QTD_VL_QUOTA_OSC_NEG_PERCENT'] = inf_diario_2017_oscl_neg_summary['QTD_VL_QUOTA_OSC_NEG_PERCENT'].astype(int)
inf_diario_2017_oscl_neg_summary['TOTAL_VL_QUOTA_OSC_NEG_PERCENT'] = inf_diario_2017_oscl_neg_summary['TOTAL_VL_QUOTA_OSC_NEG_PERCENT'].astype('float64')

inf_diario_2017_oscl_neg_summary = inf_diario_2017_oscl_neg_summary[inf_diario_2017_oscl_neg_summary.QTD_VL_QUOTA_OSC_NEG_PERCENT > 0]
inf_diario_2017_oscl_neg_summary = inf_diario_2017_oscl_neg_summary[inf_diario_2017_oscl_neg_summary.TOTAL_VL_QUOTA_OSC_NEG_PERCENT < 0.0]

# inf_diario_2017_oscl_neg_summary.sort_values(by=['CNPJ_FUNDO'])


# Merge with NR_COTST Mean
pergunta4 = inf_diario_2017_oscl_neg_summary.merge(ctstMean, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='inner')


# Merge with Denominacao do Fundo
pergunta4 = pergunta4.merge(fdo_invsti_regstr, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
pergunta4 = pergunta4.loc[pergunta4['SIT'] == 'EM FUNCIONAMENTO NORMAL' ]
pergunta4 = pergunta4.loc[0:,['CNPJ_FUNDO', 'QTD_OCCUR', 'QTD_VL_QUOTA_OSC_NEG_PERCENT', 'TOTAL_VL_QUOTA_OSC_NEG_PERCENT', 'NR_COTST_MEAN', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'RENTAB_FUNDO', 'ADMIN']]

# pergunta4.sort_values(by=['TOTAL_VL_QUOTA_OSC_NEG_PERCENT'], ascending=False, inplace=True)
# pergunta4.head(20).to_csv("__Pergunta_4__DESC.csv", sep=";", encoding="ISO-8859-1")

pergunta4.sort_values(by=['TOTAL_VL_QUOTA_OSC_NEG_PERCENT'], ascending=True, inplace=True)
pergunta4 = pergunta4.round( { 'TOTAL_VL_QUOTA_OSC_NEG_PERCENT': 2 } )
pergunta4.head(20).to_csv("__Pergunta_4__ASC.csv", sep=";", encoding="ISO-8859-1")


def generatecsvoscneg(CNPJ_FUNDO):
    suffix = CNPJ_FUNDO.replace(".", "_").replace("-", "_").replace("/", "_") + ".csv"
    csvName = "__Pergunta_4__" +  suffix
    res = inf_diario_2017_oscl_neg.loc[inf_diario_2017_oscl_neg['CNPJ_FUNDO'] == CNPJ_FUNDO]
    res.to_csv(csvName, sep=";", encoding="ISO-8859-1")


pergunta4.sort_values(by=['TOTAL_VL_QUOTA_OSC_NEG_PERCENT'], ascending=True, inplace=True)

for index, row in pergunta4.head(20).iterrows(): generatecsvoscneg( row['CNPJ_FUNDO'] )

# ---------------------------------------------- Questao 7 ----------------------------------------------
#                                                                                                       #
# -------------------------------------------------------------------------------------------------------

aggregated = temp.copy()

aggregated['VL_PATRIM_LIQ'] = aggregated['VL_PATRIM_LIQ'].str.replace(',', '')
aggregated['VL_PATRIM_LIQ'] = aggregated['VL_PATRIM_LIQ'].astype('float64')
a = aggregated.groupby('CNPJ_FUNDO', as_index=False).agg({"VL_PATRIM_LIQ": ['mean', 'std'], 'RESG_DIA': ['mean'], 'CAPTC_DIA': ['mean']})
a = a.rename(columns={"": "VL_PATRIM_LIQ_STD", "VL_PATRIM_LIQ": "VL_PATRIM_LIQ_MEAN", "CAPTC_DIA": "CAPTC_DIA_MEAN", "RESG_DIA": "RESG_DIA_MEAN" })
a.columns = a.columns.droplevel(level=1)
a.columns.values[2] = 'VL_PATRIM_LIQ_STD'
a['VL_PATRIM_LIQ_VAR_RENT'] = a['VL_PATRIM_LIQ_STD'] / a['VL_PATRIM_LIQ_MEAN'] * 100

########################################## Trecho da Pergunta 2 #######################################

# RENTAB_BRUTA_ACMULAD_PERCENT
b = pergunta2.copy()

summary = a.merge(b, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='inner')

# Merge with Denominacao do Fundo
pergunta7 = summary.loc[summary['SIT'] == 'EM FUNCIONAMENTO NORMAL' ]


pergunta7 = pergunta7[['CNPJ_FUNDO', 'VL_PATRIM_LIQ_VAR_RENT', 'RESG_DIA_MEAN', 'CAPTC_DIA_MEAN', 'NR_COTST_MEAN', 'RENTAB_BRUTA_ACMULAD_PERCENT']]


pergunta7 = pergunta7.loc[pergunta7['NR_COTST_MEAN'] >= 10 ]
pergunta7 = pergunta7.loc[pergunta7['CAPTC_DIA_MEAN'] > 0.0 ]
pergunta7 = pergunta7.loc[pergunta7['RESG_DIA_MEAN'] > 0.0 ]


pergunta7['VL_PATRIM_LIQ_VAR_RENT'] = pergunta7['VL_PATRIM_LIQ_VAR_RENT'].astype('float64')
pergunta7.sort_values(by=['VL_PATRIM_LIQ_VAR_RENT'], ascending=False, inplace=True)


#######################################################################################################

pergunta7 = pergunta7.replace([np.inf, -np.inf], np.nan)
pergunta7['VL_PATRIM_LIQ_VAR_RENT'] = pergunta7['VL_PATRIM_LIQ_VAR_RENT'].fillna(0)
pergunta7 = pergunta7.loc[pergunta7['VL_PATRIM_LIQ_VAR_RENT'] > 0.0 ]
pergunta7.to_csv("__Pergunta_7__.csv", sep=";", encoding="ISO-8859-1")

# Correlation Matrix
# pergunta7 = pd.read_csv("__Pergunta_7__.csv", sep=";", encoding="ISO-8859-1")

correlation = pergunta7.copy()
correlation = correlation[['VL_PATRIM_LIQ_VAR_RENT', 'RESG_DIA_MEAN', 'CAPTC_DIA_MEAN', 'NR_COTST_MEAN', 'RENTAB_BRUTA_ACMULAD_PERCENT']]
correlation = correlation.rename(
    columns={"VL_PATRIM_LIQ_VAR_RENT": "PATRIM_LIQ_VRENT",
             "RESG_DIA_MEAN": "RESG_MEAN",
             "CAPTC_DIA_MEAN": "CAPTC_MEAN",
             "RENTAB_BRUTA_ACMULAD_PERCENT": "RENTAB_BRUTA" })


corr = correlation.corr()
corr.to_csv("__Pergunta_7__CORR_ALL.csv", sep=";", encoding="ISO-8859-1")


correlationTOP20 = pergunta7.copy()
correlationTOP20 = correlationTOP20[['VL_PATRIM_LIQ_VAR_RENT', 'RESG_DIA_MEAN', 'CAPTC_DIA_MEAN', 'NR_COTST_MEAN', 'RENTAB_BRUTA_ACMULAD_PERCENT']]
correlationTOP20 = correlationTOP20.rename(
    columns={"VL_PATRIM_LIQ_VAR_RENT": "PATRIM_LIQ_VRENT",
             "RESG_DIA_MEAN": "RESG_MEAN",
             "CAPTC_DIA_MEAN": "CAPTC_MEAN",
             "RENTAB_BRUTA_ACMULAD_PERCENT": "RENTAB_BRUTA" })

correlationTOP20.sort_values(by=['RENTAB_BRUTA'], ascending=False, inplace=True)
correlationTOP20.head(20).to_csv("__Pergunta_7__TOP20.csv", sep=";", encoding="ISO-8859-1")


corrTop20 = correlationTOP20.head(20).corr()
corrTop20.to_csv("__Pergunta_7__CORR_TOP20.csv", sep=";", encoding="ISO-8859-1")


# sns.heatmap(corr)
sns.heatmap(corrTop20)


# def plotCorrelation(dataset):
#     sns.set(style="white")
#     corr = dataset.corr()
#     mask = np.zeros_like(corr, dtype=np.bool)
#     mask[np.triu_indices_from(mask)] = True
#     f, ax = pl.subplots(figsize=(15, 12))
#     cmap = sns.diverging_palette(220, 10, as_cmap=True)
#     sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3, center=0, square=False, linewidths=.6, cbar_kws={"shrink": .5})
#
# plotCorrelation(correlation)
pl.show()