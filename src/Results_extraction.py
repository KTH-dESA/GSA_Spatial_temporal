import pandas as pd

def read_data(data, years):
    """
    Creates a dictionary of param in results text file and returns it
    :param data:
    :return:
    """
    col =['param','region','tech','f'] + years
    df = pd.read_csv(data, delimiter='\t', header=None, names=col)
    param = df.param.unique()
    dict_results = {}

    for c in param:
        subdf = df.loc[df['param']==c]
        cols = years
        subdf['sumall'] = df[cols].sum(axis=1)
        subdf.drop(subdf.loc[subdf.sumall==0].index, inplace=True)
        dict_results[c] = subdf
        subdf.to_csv(data +c+'.csv')

    return dict_results

data = 'src/run/Ben_0.txt'
year_array = ['2020', '2021', '2022','2023','2024','2025','2026','2027','2028','2029',	'2030',	'2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040', '2041']

read_data(data, year_array)



