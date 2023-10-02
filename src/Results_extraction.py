"""
Module: Results_morris
=============================

A module for extracting the results

---------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se> and morris calculation is adapted from
https://github.com/KTH-dESA/esom_gsa/blob/envs/workflow/scripts/calculate_SA_results.py
and https://github.com/KTH-dESA/esom_gsa/blob/envs/workflow/scripts/utils.py 

"""

import pandas as pd
from os import listdir
from os.path import isfile, join
import os
import numpy as np
import matplotlib.pyplot as plt
from math import ceil
from SALib.analyze import morris as analyze_morris
from SALib.plotting import morris as plot_morris
import csv
import sys
import matplotlib.lines as mlines
from pathlib import Path
from typing import List
import seaborn as sns
from itertools import cycle

from logging import getLogger

logger = getLogger(__name__)

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def load_csvs(paths, years):
    """Creates a dataframe dictionary from the csv files in /data : dict_df

    Arguments
    ---------
    param_file : paths
        Path to the data files (/data)
    """
    filepaths = [f for f in listdir(paths) if isfile(join(paths, f))]
    onlyfiles = [os.path.join(paths, f) for f in filepaths]
    dict_df = {}
    
    col =['param','region','tech','f'] + years
    for files in onlyfiles:
    #validate that the files are csv. Else the read function will not work
        _, filename = os.path.split(files)
        name, ending = os.path.splitext(filename)
        if ending == '.txt':
            dict_df[name] = pd.read_csv(files, delimiter='\t', header=None, names=col)
        else:
            print('You have mixed file types in you directory, please make sure all are .csv type! {}'.format(files))

    return dict_df

def read_data(dict_df, years, path):
    """
    Creates a dictionary of param in results text file and returns it
    :param data:
    :return:
    """
    dict_results = {}
    for i in dict_df.keys():
        start, sample, end = i.split("_")
        df = dict_df[i]
        df['sumall'] = df[years].sum(axis=1)
        df_final = df.loc[df.sumall!=0]
        param = df_final.param.unique()
        dict_re = {}

        for c in param:
            subdf = df_final.loc[df['param']==c]
            dict_re[c] = subdf
            subdf.to_csv(path + i +c+'.csv')
        dict_results[sample] = dict_re
    return dict_results

def creating_Y_to_morris(dict, path, years, masterdata):
    
    Y_totalcost = {}
    #TotalDiscountedCostByTechnology[r,t,y]+sum{s in STORAGE} TotalDiscountedStorageCost[r,s,y] = TotalDiscountedCost[r,y] There is no storage in this model
    for i in dict.keys():
        totaldiscounted_cost = {}
        totaldiscounted_cost['TotalDiscountedCost'] = dict[i]['TotalDiscountedCostByTechnology']['sumall'].sum(axis=0)
        Y_totalcost[i] = totaldiscounted_cost

    df = pd.DataFrame.from_dict(Y_totalcost, orient="index")

    df.to_csv(os.path.join(path,'totaldiscounted_results.csv'))

    Y_transmshare = {}
    #NewCapacity for TRLV CANNOT BE DONE AS THE PEAKDEMAND AND km AFFECT THE NEWCAPACITY! However peakdemand*km is the equivalent
    for i in dict.keys():
        transmission = {}
        newcap = dict[i]['NewCapacity'][['tech','sumall']] #.sum(axis=0)
        #This function selects the transmission lines
        transmission_select = newcap[newcap['tech'].str.startswith('TRHV')]
        transmission['Transmission_capacity'] = transmission_select['sumall'].sum(axis=0)
        Y_transmshare[i] = transmission
    
    df = pd.DataFrame.from_dict(Y_transmshare, orient="index")
    df.to_csv(os.path.join(path,'New_capacity_transmission.csv'))

    #RET share production per year
    YRET_share = {}
    # cost of all
    for i in dict.keys():
        RET_share = {}
        production = dict[i]['ProductionByTechnologyAnnual'][['tech','f', 'sumall']] #.sum(axis=0)
        # Read the CSV file into a dataframe
        master_df = pd.read_csv(masterdata, encoding='latin-1')
        # Split the 'tech' column into three columns
        production[['tech', 'cell', 'elec']] = production['tech'].str.split('_', expand=True)
        merged_df = pd.merge(production, master_df, how='inner', on='tech')
        totalprod = merged_df.loc[(merged_df['Type'] =='Power plant') | (merged_df['Type'] == 'Mini grid') | (merged_df['Type'] == 'Stand alone')]
        ret = totalprod[totalprod['RET']== 'Y']
        RET_share['RETshare'] = ret['sumall'].sum(axis=0)/totalprod['sumall'].sum(axis=0)
        YRET_share[i] = RET_share
    
    df = pd.DataFrame.from_dict(YRET_share, orient="index")
    df.to_csv(os.path.join(path,'RETShare.csv'))

    #PV share production per year
    YPV_share = {}
    # cost of all
    for i in dict.keys():
        PV_share = {}
        production = dict[i]['ProductionByTechnologyAnnual'][['tech','f', 'sumall']] #.sum(axis=0)
        # Read the CSV file into a dataframe
        master_df = pd.read_csv(masterdata, encoding='latin-1')
        # Split the 'tech' column into three columns
        production[['tech', 'cell', 'elec']] = production['tech'].str.split('_', expand=True)
        merged_df = pd.merge(production, master_df, how='inner', on='tech')
        totalprod = merged_df.loc[(merged_df['Type'] =='Power plant') | (merged_df['Type'] == 'Mini grid') | (merged_df['Type'] == 'Stand alone')]
        PV = totalprod[(totalprod['tech']).str.contains('PV')]
        PV_share['PVShare'] = PV['sumall'].sum(axis=0)/totalprod['sumall'].sum(axis=0)
        YPV_share[i] = PV_share
    
    df = pd.DataFrame.from_dict(YPV_share, orient="index")
    df.to_csv(os.path.join(path,'PV_share.csv'))
     
    Ykm_share = {}
    # cost of all
    for i in dict.keys():
        km_share = {}
        try:
            km = dict[i]['km'][['tech','sumall']] #.sum(axis=0)
            #This function selects the LV lines
            xy = km[km['tech'].str.startswith('TRLV')]
            km_share['km_share'] = xy['sumall'].sum(axis=0)
        except:
            km_share ['km_share'] = 0
        Ykm_share[i] = km_share
    
    df = pd.DataFrame.from_dict(Ykm_share, orient="index")
    df.to_csv(os.path.join(path,'Km_distribution_lines.csv'))


    Y_capitalcost = {}
    for i in dict.keys():
        capitalcost = {}
        capitalcost['Capitalcost'] = dict[i]['CapitalInvestment']['sumall'].sum(axis=0)
        Y_capitalcost[i] = capitalcost

    df = pd.DataFrame.from_dict( Y_capitalcost, orient="index")
    df.to_csv(os.path.join(path,'Capitalinvestment_results.csv'))

    #TODO: Add PV and battery to the analysis
    Y_PVbattery = {}
    # Y_PVbattery[i] = 

    # df = pd.DataFrame.from_dict( Y_PVBattery, orient="index")
    # df.to_csv(os.path.join(path,'Capitalinvestment_results.csv'))

    return Y_totalcost, Y_transmshare, Y_capitalcost, YRET_share, Ykm_share, YPV_share, Y_PVbattery

def run_morris(dict_y, paramvalues_path, nom_path, problem_path, save_file, unit, scale=False):

    # Perform the sensitivity analysis using the model output
    # Specify which column of the output file to analyze (zero-indexed)
    # Si = morris.analyze(problem, param_values_, Y, conf_level=0.95, print_to_console=True, num_levels=4, num_resamples=100)
    # Returns a dictionary with keys 'mu', 'mu_star', 'sigma', and 'mu_star_conf'
    # e.g. Si['mu_star'] contains the mu* value for each parameter, in the
    # same order as the parameter file
    """Analyzes objective value results from model

    Arguments
    ---------
    path_to_parameters : str
        File containing the parameters for generated sample
    model_inputs : str
        File path to sample model inputs
    model_outputs : str
        File path to model outputs
    location_to_save : str
        File path to save results
    result_type : str
        True for Objective result type
        False for user defined result type 

    Usage
    -----
    To run the script on the command line, type::

        python analyze_results.py path/to/parameters.csv path/to/inputs.txt 
            path/to/model/results.csv path/to/save/SA/results.csv

    The `parameters.csv` CSV file should be formatted as follows::

        name,group,indexes,min_value,max_value,dist,interpolation_index,action
        CapitalCost,pvcapex,"GLOBAL,GCPSOUT0N",500,1900,unif,YEAR,interpolate
        DiscountRate,discountrate,"GLOBAL,GCIELEX0N",0.05,0.20,unif,None,fixed

    The `inputs.txt` should be the output from SALib.sample.morris.sample

    """

    def create_salib_problem(parameters: List) -> dict:
        """Creates SALib problem from scenario configuration.
        
        Arguments
        ---------
        parameters: List
            List of dictionaries describing problem. Each dictionary must have
            'name', 'indexes', 'group' keys
        Returns
        -------
        problem: dict
            SALib formatted problem dictionary
        Raises
        ------
        ValueError
            If only one variable is givin, OR 
            If only one group is given
        """

        problem = {}
        problem['num_vars'] = len(parameters)
        if problem['num_vars'] <= 1:
            raise ValueError(
                f"Must define at least two variables in problem. User defined "
                f"{problem['num_vars']} variable(s).")

        names = []
        bounds = []
        groups = []
        for parameter in parameters:
            names.append(parameter['name'] + ";" + parameter['indexes'])
            groups.append(parameter['group'])
            min_value = 0
            max_value = 1
            bounds.append([min_value, max_value])

        problem['names'] = names
        problem['bounds'] = bounds
        problem['groups'] = groups
        num_groups = len(set(groups))
        if num_groups <= 1:
            raise ValueError(
                f"Must define at least two groups in problem. User defined "
                f"{num_groups} group(s).")

        return problem

    def sa_results(parameters: dict, X: np.array, Y: np.array, NOM_X: np.array, save_file: str, unit: str):
        """Performs SA and plots results. 

        Parameters
        ----------
        parameters : Dict
            Parameters for generated sample
        X : np.array
            Input Sample
        Y : np.array
            Results 
        save_file : str
            File path to save results
        """
        if scale == True:
            S_X = NOM_X 
        else:
            S_X = X

        problem = create_salib_problem(parameters)

        Si = analyze_morris.analyze(problem, S_X, Y, scaled = scale, print_to_console=False)

        # Save text based results
        Si.to_df().to_csv(f'{save_file}.csv')
        
        # save graphical resutls 
        title = Path(save_file).stem#.capitalize()
        fig, axs = plt.subplots(2, figsize=(10,15))

        fig.suptitle(title, fontsize=24)
        plot_morris.horizontal_bar_plot(axs[0], Si, unit="(%s)" %(unit))
        plot_morris.covariance_plot(axs[1], Si, unit="(%s)" %(unit))
        axs[0].tick_params(axis='both', which='major', labelsize=16)
        axs[1].tick_params(axis='both', which='major', labelsize=16)
        fig.delaxes(axs[1])
        fig.savefig(f'{save_file}.png', bbox_inches='tight')
    
    with open(problem_path, 'r') as csv_file:
        parameters = list(csv.DictReader(csv_file))

    X = np.loadtxt(paramvalues_path, delimiter=",")
    SX = np.loadtxt(nom_path, delimiter=",")
    df = pd.DataFrame.from_dict(dict_y)
    df_2 = df.T
    df_2.index = df_2.index.astype(int)
    df_2 = df_2.sort_index()
    Y = df_2.to_numpy()

    sa_results(parameters, X, Y, SX, save_file, unit)


def join_results(kenyatotalcost_csv, benintotalcost_csv, kenya_distr, benin_distr, kenya_renewa, benin_renewa, save_file):
    
    kenyatotalcost = pd.read_csv(kenyatotalcost_csv)
    benintotalcost = pd.read_csv(benintotalcost_csv)
    kenyakm = pd.read_csv(kenya_distr)
    beninkm = pd.read_csv(benin_distr)
    kenyaRET = pd.read_csv(kenya_renewa)
    beninRET = pd.read_csv(benin_renewa)

    dfs = [kenyatotalcost, benintotalcost, kenyakm, beninkm, kenyaRET, beninRET]
    param_names = ['Kenya Total Discounted Cost', 'Benin Total Discounted Cost', 'Kenya installed distribution lines', 'Benin installed distribution lines', 'Kenya Renewable energy production share', 'Benin Renewable energy production share']

    # Join the dataframes
    joined_df = pd.concat([df.assign(OutputVariable=param_name) for df, param_name in zip(dfs, param_names)])
    joined_df = joined_df.reset_index(drop=True)
    joined_df = joined_df.rename(columns = {'Unnamed: 0':'Parameter'})
    df = joined_df.pivot(index='OutputVariable', columns='Parameter', values='mu_star')

    df.to_csv(f'{save_file}.csv')

    ax = sns.heatmap(df)
    fig = ax.get_figure()
    #plt.title("Kenya ParametermSensitivities")

    fig.savefig(f'{save_file}.png')
    plt.show()


def main(folder, masterdata):
    years = ['2020', '2021', '2022','2023','2024','2025','2026','2027','2028','2029','2030','2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040', '2041']
    years_included_analysis =  ['2020','2021', '2022','2023','2024','2025','2026','2027','2028','2029','2030','2031','2032','2033',	'2034',	'2035',	'2036',	'2037']
    dict_df = load_csvs(folder, years)
    dict_results = read_data(dict_df, years_included_analysis, '%ssensitivity/latest_run/' %(country))
    totaldiscountedcost, transmcap, capitalinvestment, RET_share, km, PV_share, PV_battery= creating_Y_to_morris(dict_results, '%ssensitivity'%(country), years, masterdata)
    run_morris(totaldiscountedcost, '%ssensitivity/sample_morris.csv' %(country), '%ssensitivity/sample_morris_nominal.csv' %(country), 'config/%snominal_parameters.csv'%(country), '%ssensitivity/Total discounted cost %s'%(country, country), '\$', True)
    run_morris(capitalinvestment, '%ssensitivity/sample_morris.csv'%(country), '%ssensitivity/sample_morris_nominal.csv' %(country),'config/%snominal_parameters.csv'%(country), '%ssensitivity/Capital investment %s'%(country, country), '\$', True)
    run_morris(transmcap, '%ssensitivity/sample_morris.csv'%(country),'%ssensitivity/sample_morris_nominal.csv' %(country), 'config/%snominal_parameters.csv'%(country), '%ssensitivity/Expansion of transmission %s'%(country, country), 'kW', True)
    run_morris(RET_share, '%ssensitivity/sample_morris.csv'%(country), '%ssensitivity/sample_morris_nominal.csv' %(country),'config/%snominal_parameters.csv'%(country), '%ssensitivity/Renewable energy production share %s'%(country, country), '%', True)
    run_morris(km, '%ssensitivity/sample_morris.csv'%(country),'%ssensitivity/sample_morris_nominal.csv' %(country), 'config/%snominal_parameters.csv'%(country), '%ssensitivity/number of km distributionlines %s'%(country, country), 'km', True) 
    run_morris(PV_share, '%ssensitivity/sample_morris.csv'%(country),'%ssensitivity/sample_morris_nominal.csv' %(country), 'config/%snominal_parameters.csv'%(country), '%ssensitivity/PV panel share %s'%(country, country), '%', True)

    run_morris(totaldiscountedcost, '%ssensitivity/sample_morris.csv' %(country), '%ssensitivity/sample_morris_nominal.csv' %(country), 'config/%snominal_parameters.csv'%(country), '%ssensitivity/Total discounted cost unscaled %s'%(country, country), '\$')
    run_morris(capitalinvestment, '%ssensitivity/sample_morris.csv'%(country), '%ssensitivity/sample_morris_nominal.csv' %(country),'config/%snominal_parameters.csv'%(country), '%ssensitivity/Capital investment unscaled %s'%(country, country), '\$')
    run_morris(transmcap, '%ssensitivity/sample_morris.csv'%(country),'%ssensitivity/sample_morris_nominal.csv' %(country), 'config/%snominal_parameters.csv'%(country), '%ssensitivity/Expansion of transmission unscaled %s'%(country, country), 'kW')
    run_morris(RET_share, '%ssensitivity/sample_morris.csv'%(country), '%ssensitivity/sample_morris_nominal.csv' %(country),'config/%snominal_parameters.csv'%(country), '%ssensitivity/Renewable energy production share unscaled %s'%(country, country), '%')
    run_morris(km, '%ssensitivity/sample_morris.csv'%(country),'%ssensitivity/sample_morris_nominal.csv' %(country), 'config/%snominal_parameters.csv'%(country), '%ssensitivity/number of km distributionlines unscaled %s'%(country, country), 'km') 
    run_morris(PV_share, '%ssensitivity/sample_morris.csv'%(country),'%ssensitivity/sample_morris_nominal.csv' %(country), 'config/%snominal_parameters.csv'%(country), '%ssensitivity/PV panel share unscaled %s'%(country, country), '%')

countries = ['Kenya', 'Benin']
for country in countries:
    # File which contains all the abbrevations
    masterdata = 'config/masterdata.csv'

    main('%s_run/results' %(country), masterdata)
join_results('Kenyasensitivity/Total discounted cost Kenya.csv', 'Beninsensitivity/Total discounted cost Benin.csv', 'Kenyasensitivity/number of km distributionlines Kenya.csv','Beninsensitivity/number of km distributionlines Benin.csv', 'Kenyasensitivity/Renewable energy production share Kenya.csv' ,'Beninsensitivity/Renewable energy production share Benin.csv', 'matrix_Sin_mu')
