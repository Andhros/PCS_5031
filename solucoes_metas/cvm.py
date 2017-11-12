import pandas as pd

# https://www.shanelynn.ie/summarising-aggregation-and-grouping-data-in-python-pandas/
# http://chris.friedline.net/2015-12-15-rutgers/lessons/python2/03-data-types-and-format.html
# https://stackoverflow.com/questions/14059094/i-want-to-multiply-two-columns-in-a-pandas-dataframe-and-add-the-result-into-a-n
# https://stackoverflow.com/questions/33282119/pandas-filter-dataframe-by-another-dataframe-by-row-elements
# https://community.modeanalytics.com/python/tutorial/pandas-groupby-and-python-lambda-functions/

inf_diario_2017 = pd.read_csv("__inf_diario_2017_JOINED__.csv", sep=";") # inf_diario_2017.shape # (2860856, 9)
fdo_invsti_regstr = pd.read_csv("__fdo_invsti_regstr_UNIQUE_JOINED__.csv", sep=";", encoding="ISO-8859-1")
fdo_invsti_regstr_summary = fdo_invsti_regstr[['CNPJ_FUNDO', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'ADMIN']]


# ---------------------------------------------- Questao 2 ----------------------------------------------
#                                                                                                       #
# -------------------------------------------------------------------------------------------------------

grouped = inf_diario_2017.groupby('CNPJ_FUNDO', as_index=False).agg({"DT_COMPTC": [min, max]})
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

merge_inner = min_vl_quota.merge(max_vl_quota, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='inner', suffixes=('_MIN', '_MAX'))

result = merge_inner.copy()

result['VL_QUOTA_MIN'] = result['VL_QUOTA_MIN'].str.replace(',', '')
result['VL_QUOTA_MAX'] = result['VL_QUOTA_MAX'].str.replace(',', '')

result['VL_QUOTA_MIN'] = result['VL_QUOTA_MIN'].astype('float64')
result['VL_QUOTA_MAX'] = result['VL_QUOTA_MAX'].astype('float64')

result = result.round( { 'VL_QUOTA_MIN': 2, 'VL_QUOTA_MAX' : 2 } )
result = result.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC_MIN', 'VL_QUOTA_MIN', 'DT_COMPTC_MAX', 'VL_QUOTA_MAX']]


# Calculating
result = result.loc[result['VL_QUOTA_MIN'] > 0.0 ]

result['RENTAB_BRUTA_ACMULAD'] = result.VL_QUOTA_MAX - result.VL_QUOTA_MIN
result['RENTAB_BRUTA_ACMULAD'] = result['RENTAB_BRUTA_ACMULAD'].astype('float64')
result = result.round( { 'RENTAB_BRUTA_ACMULAD': 2 } )

result['RENTAB_BRUTA_ACMULAD_PERCENT'] = result['RENTAB_BRUTA_ACMULAD'] * 100 / result.VL_QUOTA_MIN
result = result.round( { 'RENTAB_BRUTA_ACMULAD_PERCENT': 2 } )
result.sort_values(by=['RENTAB_BRUTA_ACMULAD_PERCENT'], ascending=False, inplace=True)

# Merge with Denominacao do Fundo
pergunta2 = result.head(20)
pergunta2 = pergunta2.merge(fdo_invsti_regstr, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
pergunta2 = pergunta2.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC_MIN', 'VL_QUOTA_MIN', 'DT_COMPTC_MAX', 'VL_QUOTA_MAX', 'RENTAB_BRUTA_ACMULAD','RENTAB_BRUTA_ACMULAD_PERCENT', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'RENTAB_FUNDO', 'ADMIN']]

pergunta2 = pergunta2.loc[pergunta2['SIT'] == 'EM FUNCIONAMENTO NORMAL' ]
pergunta2 = pergunta2.head(20)

pergunta2.to_csv("__Pergunta_2__.csv", sep=";", encoding="ISO-8859-1")


# ---------------------------------------------- Questao 4 ----------------------------------------------
#                                                                                                       #
# -------------------------------------------------------------------------------------------------------


inf_diario_2017_oscl = inf_diario_2017.copy()

inf_diario_2017_oscl = inf_diario_2017_oscl.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC', 'VL_QUOTA']]
inf_diario_2017_oscl.sort_values(by=['CNPJ_FUNDO', 'DT_COMPTC'], inplace=True)
inf_diario_2017_oscl['VL_QUOTA'] = inf_diario_2017_oscl['VL_QUOTA'].str.replace(',', '')
inf_diario_2017_oscl['VL_QUOTA'] = inf_diario_2017_oscl['VL_QUOTA'].astype('float64')
inf_diario_2017_oscl['VL_QUOTA_OSCL'] = inf_diario_2017_oscl.groupby(['CNPJ_FUNDO'])['VL_QUOTA'].transform(pd.Series.diff)
inf_diario_2017_oscl.sort_values(by=['CNPJ_FUNDO', 'DT_COMPTC'], inplace=True)
inf_diario_2017_oscl['VL_QUOTA_OSCL'] = inf_diario_2017_oscl['VL_QUOTA_OSCL'].fillna(0)
# inf_diario_2017_oscl.to_csv("__Pergunta_4__.csv", sep=";", encoding="ISO-8859-1")

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
inf_diario_2017_oscl_summary.sort_values(by=['OSCL_OCCUR_NEG'], ascending=False, inplace=True)
# inf_diario_2017_oscl_summary.to_csv("__Pergunta_4_SUMMARIZED__.csv", sep=";", encoding="ISO-8859-1")


# Merge with Denominacao do Fundo
pergunta4 = inf_diario_2017_oscl_summary.head(20)

pergunta4 = pergunta4.merge(fdo_invsti_regstr, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
pergunta4 = pergunta4.loc[pergunta4['SIT'] == 'EM FUNCIONAMENTO NORMAL' ]

pergunta4 = pergunta4.head(20)

pergunta4.to_csv("__Pergunta_4__.csv", sep=";", encoding="ISO-8859-1")


# ---------------------------------------------- Questao 7 ----------------------------------------------
#                                                                                                       #
# -------------------------------------------------------------------------------------------------------

aggregated = inf_diario_2017.copy()

aggregated['VL_PATRIM_LIQ'] = aggregated['VL_PATRIM_LIQ'].str.replace(',', '')
aggregated['VL_PATRIM_LIQ'] = aggregated['VL_PATRIM_LIQ'].astype('float64')

aggregated['RESG_DIA'] = aggregated['RESG_DIA'].str.replace(',', '')
aggregated['RESG_DIA'] = aggregated['RESG_DIA'].astype('float64')

aggregated['CAPTC_DIA'] = aggregated['CAPTC_DIA'].str.replace(',', '')
aggregated['CAPTC_DIA'] = aggregated['CAPTC_DIA'].astype('float64')


aggregated['NR_COTST'] = aggregated['NR_COTST'].str.replace(',', '')
aggregated['NR_COTST'] = aggregated['NR_COTST'].astype('float64')

x = aggregated.groupby('CNPJ_FUNDO', as_index=False).agg({"VL_PATRIM_LIQ": ['mean', 'std'], 'NR_COTST': ['mean'], 'RESG_DIA': ['mean'], 'CAPTC_DIA': ['mean']})
x.columns = x.columns.droplevel(level=1)

x.columns.values[1] = 'VL_PATRIM_LIQ_MEAN'
x.columns.values[2] = 'VL_PATRIM_LIQ_STD'
x.columns.values[3] = 'RESG_DIA_MEAN'
x.columns.values[4] = 'CAPTC_DIA_MEAN'
x.columns.values[5] = 'NR_COTST_MEAN'

x['VL_PATRIM_LIQ_VAR_RENT'] = x['VL_PATRIM_LIQ_STD'] / x['VL_PATRIM_LIQ_MEAN'] * 100



########################################## Trecho da Pergunta 2 #######################################

b = inf_diario_2017_oscl.groupby('CNPJ_FUNDO').apply(
    lambda x: pd.Series(
        dict(
            OSCL_OCCUR = x.VL_QUOTA_OSCL.sum()
        )
    )
)

a = a.reset_index()
b = b.reset_index()

inf_diario_2017_oscl_summary = a.merge(b, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
inf_diario_2017_oscl_summary['OSCL_OCCUR'] = inf_diario_2017_oscl_summary['OSCL_OCCUR'].fillna(0)

######################################################################################################


summary = x.merge(inf_diario_2017_oscl_summary, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='inner')
summary = summary[['CNPJ_FUNDO', 'VL_PATRIM_LIQ_MEAN', 'VL_PATRIM_LIQ_STD', 'VL_PATRIM_LIQ_VAR_RENT', 'RESG_DIA_MEAN', 'CAPTC_DIA_MEAN', 'NR_COTST_MEAN', 'OSCL_OCCUR']]
summary.sort_values(by=['VL_PATRIM_LIQ_MEAN'], ascending=False, inplace=True)


# Merge with Denominacao do Fundo
pergunta7 = summary.head(20)


pergunta7 = pergunta7.merge(fdo_invsti_regstr_summary, left_on='CNPJ_FUNDO', right_on='CNPJ_FUNDO', how='left')
pergunta7 = pergunta7.loc[pergunta7['SIT'] == 'EM FUNCIONAMENTO NORMAL' ]

pergunta7 = pergunta7.head(20)
pergunta7.sort_values(by=['VL_PATRIM_LIQ_MEAN'], ascending=False, inplace=True)

# pergunta7 = pergunta7.loc[0:,['CNPJ_FUNDO', 'DT_COMPTC_MIN', 'VL_QUOTA_MIN', 'DT_COMPTC_MAX', 'VL_QUOTA_MAX', 'RENTAB_BRUTA_ACMULAD','RENTAB_BRUTA_ACMULAD_PERCENT', 'DENOM_SOCIAL', 'SIT', 'CLASSE', 'RENTAB_FUNDO', 'ADMIN']]
pergunta7.to_csv("__Pergunta_7__.csv", sep=";", encoding="ISO-8859-1")

