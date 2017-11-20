import pandas as pd
from string import ascii_letters
import numpy as np
import seaborn as sns
import matplotlib.pyplot as pl


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

inf_diario_2017_FILTERED = inf_diario_2017.groupby('CNPJ_FUNDO', as_index=False).agg( {"NR_COTST": [sum], "CAPTC_DIA": [sum], "RESG_DIA": [sum]} )

inf_diario_2017_FILTERED = inf_diario_2017_FILTERED.loc[inf_diario_2017['NR_COTST'] >= 10 ]
inf_diario_2017_FILTERED = inf_diario_2017_FILTERED.loc[inf_diario_2017['CAPTC_DIA'] > 0.0 ]
inf_diario_2017_FILTERED = inf_diario_2017_FILTERED.loc[inf_diario_2017['RESG_DIA'] > 0.0 ]

inf_diario_2017_FILTERED.columns = inf_diario_2017_FILTERED.columns.droplevel(level=0)
inf_diario_2017_FILTERED.columns.values[0] = 'CNPJ_FUNDO'
inf_diario_2017_FILTERED.columns.values[1] = 'CAPTC_DIA_AGGSUM'
inf_diario_2017_FILTERED.columns.values[2] = 'RESG_DIA_AGGSUM'
inf_diario_2017_FILTERED.columns.values[3] = 'NR_COTST_AGGSUM'

ix1 = inf_diario_2017.set_index(['CNPJ_FUNDO']).index
ix2 = inf_diario_2017_FILTERED.set_index(['CNPJ_FUNDO']).index

inf_diario_2017_FILTERED = inf_diario_2017[ix1.isin(ix2)]
inf_diario_2017_FILTERED = inf_diario_2017_FILTERED.drop('ID', axis=1)



# ---------------------------------------------- Questao 2 ----------------------------------------------
#                                                                                                       #
# -------------------------------------------------------------------------------------------------------

grouped = inf_diario_2017_FILTERED.groupby('CNPJ_FUNDO', as_index=False).agg({"DT_COMPTC": [min, max]})
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

# Merge with Denominacao do Fundo
pergunta2 = result.merge(fdo_invsti_regstr, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
pergunta2 = pergunta2.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC_MIN', 'VL_QUOTA_MIN', 'DT_COMPTC_MAX', 'VL_QUOTA_MAX', 'RENTAB_BRUTA_ACMULAD','RENTAB_BRUTA_ACMULAD_PERCENT', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'RENTAB_FUNDO', 'ADMIN']]
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


inf_diario_2017_oscl = inf_diario_2017_FILTERED.copy()

inf_diario_2017_oscl = inf_diario_2017_oscl.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA']]
inf_diario_2017_oscl.sort_values(by=['CNPJ_FUNDO', 'DT_COMPTC'], inplace=True)
inf_diario_2017_oscl['VL_QUOTA'] = inf_diario_2017_oscl['VL_QUOTA'].str.replace(',', '')
inf_diario_2017_oscl['VL_QUOTA'] = inf_diario_2017_oscl['VL_QUOTA'].astype('float64')
inf_diario_2017_oscl['VL_QUOTA_OSCL'] = inf_diario_2017_oscl.groupby(['CNPJ_FUNDO'])['VL_QUOTA'].transform(pd.Series.diff)
inf_diario_2017_oscl.sort_values(by=['CNPJ_FUNDO', 'DT_COMPTC'], inplace=True)
inf_diario_2017_oscl['VL_QUOTA_OSCL'] = inf_diario_2017_oscl['VL_QUOTA_OSCL'].fillna(0)

# Summarizing

#as_index=False ommits the CNPJ_FUNDO
a = inf_diario_2017_oscl.groupby('CNPJ_FUNDO').apply(lambda x: pd.Series(dict(OCCUR = x.CNPJ_FUNDO.count())))
b = inf_diario_2017_oscl[inf_diario_2017_oscl.VL_QUOTA_OSCL < 0.0].groupby('CNPJ_FUNDO').apply(
    lambda x: pd.Series(
        dict(
            OCCUR_NEG = x.VL_QUOTA_OSCL.count(),
            OSCL_OCCUR_NEG = x.VL_QUOTA_OSCL.sum()
        )
    )
)

a = a.reset_index()
b = b.reset_index()

inf_diario_2017_oscl_summary = a.merge(b, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
inf_diario_2017_oscl_summary['OSCL_OCCUR_NEG'] = inf_diario_2017_oscl_summary['OSCL_OCCUR_NEG'].fillna(0)
inf_diario_2017_oscl_summary['OCCUR_NEG'] = inf_diario_2017_oscl_summary['OCCUR_NEG'].fillna(0)
inf_diario_2017_oscl_summary['OCCUR_NEG'] = inf_diario_2017_oscl_summary['OCCUR_NEG'].astype(int)
inf_diario_2017_oscl_summary = inf_diario_2017_oscl_summary[inf_diario_2017_oscl_summary.OSCL_OCCUR_NEG < 0.0]


# Merge with Denominacao do Fundo
pergunta4 = inf_diario_2017_oscl_summary.merge(fdo_invsti_regstr, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
pergunta4 = pergunta4.loc[pergunta4['SIT'] == 'EM FUNCIONAMENTO NORMAL' ]


pergunta4.sort_values(by=['OSCL_OCCUR_NEG'], ascending=False, inplace=True)
pergunta4.head(20).to_csv("__Pergunta_4__DESC.csv", sep=";", encoding="ISO-8859-1")

pergunta4.sort_values(by=['OSCL_OCCUR_NEG'], ascending=True, inplace=True)
pergunta4.head(20).to_csv("__Pergunta_4__ASC.csv", sep=";", encoding="ISO-8859-1")


# ---------------------------------------------- Questao 7 ----------------------------------------------
#                                                                                                       #
# -------------------------------------------------------------------------------------------------------

aggregated = inf_diario_2017_FILTERED.copy()

aggregated['VL_PATRIM_LIQ'] = aggregated['VL_PATRIM_LIQ'].str.replace(',', '')
aggregated['VL_PATRIM_LIQ'] = aggregated['VL_PATRIM_LIQ'].astype('float64')

a = aggregated.groupby('CNPJ_FUNDO', as_index=False).agg({"VL_PATRIM_LIQ": ['mean', 'std'], 'NR_COTST': ['mean'], 'RESG_DIA': ['mean'], 'CAPTC_DIA': ['mean']})
a.columns = a.columns.droplevel(level=1)

a.columns.values[1] = 'VL_PATRIM_LIQ_MEAN'
a.columns.values[2] = 'VL_PATRIM_LIQ_STD'
a.columns.values[3] = 'RESG_DIA_MEAN'
a.columns.values[4] = 'CAPTC_DIA_MEAN'
a.columns.values[5] = 'NR_COTST_MEAN'

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
correlation.columns.values[0] = 'PATRIM_LIQ_VRENT'
correlation.columns.values[1] = 'RESG_MEAN'
correlation.columns.values[2] = 'CAPTC_MEAN'
correlation.columns.values[3] = 'NR_COTST_MEAN'
correlation.columns.values[4] = 'RENTAB_BRUTA'
correlationTOP20.sort_values(by=['RENTAB_BRUTA'], ascending=False, inplace=True)

corr = correlation.corr()
corr.to_csv("__Pergunta_7__CORR_ALL.csv", sep=";", encoding="ISO-8859-1")


correlationTOP20 = pergunta7.copy()
correlationTOP20 = correlationTOP20[['VL_PATRIM_LIQ_VAR_RENT', 'RESG_DIA_MEAN', 'CAPTC_DIA_MEAN', 'NR_COTST_MEAN', 'RENTAB_BRUTA_ACMULAD_PERCENT']]
correlationTOP20.columns.values[0] = 'PATRIM_LIQ_VRENT'
correlationTOP20.columns.values[1] = 'RESG_MEAN'
correlationTOP20.columns.values[2] = 'CAPTC_MEAN'
correlationTOP20.columns.values[3] = 'NR_COTST_MEAN'
correlationTOP20.columns.values[4] = 'RENTAB_BRUTA'
correlationTOP20.sort_values(by=['RENTAB_BRUTA'], ascending=False, inplace=True)


correlationTOP20.head(20).to_csv("__Pergunta_7__TOP20.csv", sep=";", encoding="ISO-8859-1")

corrTop20 = correlationTOP20.head(20).corr()
corrTop20.to_csv("__Pergunta_7__CORR_TOP20.csv", sep=";", encoding="ISO-8859-1")


sns.heatmap(corr)
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