"""
Module: Results_morris
=============================

A module for building the logic around peakdemand, transmissionlines and distributionlines.

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
import numpy as np
import pandas as pd
import csv
import sys
import matplotlib.lines as mlines
from pathlib import Path
from typing import List

from logging import getLogger

logger = getLogger(__name__)

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

def read_data(dict_df, years):
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
            subdf.to_csv(i +c+'.csv')
        dict_results[sample] = dict_re
    return dict_results

def creating_Y_to_morris(dict, path, years):
    
    #TODO add totaldiscountedcost, capacity by technology, transmission lines, Pre-optimisation results (battery size?), Share of total cost distribution and transmission,
    #TODO add Solution time (computation)

    Y_totalcost = {}
    #TotalDiscountedCostByTechnology[r,t,y]+sum{s in STORAGE} TotalDiscountedStorageCost[r,s,y] = TotalDiscountedCost[r,y] There is no storage in this model
    for i in dict.keys():
        totaldiscounted_cost = {}
        totaldiscounted_cost['TotalDiscountedCost'] = dict[i]['TotalDiscountedCostByTechnology']['sumall'].sum(axis=0)
        Y_totalcost[i] = totaldiscounted_cost

    df = pd.DataFrame.from_dict(Y_totalcost, orient="index")
    df.to_csv('src/sensitivity/totaldiscounted_results.csv')

    Y_capacity = {}
    #NewCapacity
    for i in dict.keys():
        newcap = {}
        newcap['NewCapacity'] = dict[i]['NewCapacity']['sumall'].sum(axis=0)
        Y_capacity[i] = newcap

    df = pd.DataFrame.from_dict(Y_capacity, orient="index")
    df.to_csv('src/sensitivity/New_capacity.csv')

    return Y_totalcost, Y_capacity

def run_morris(dict_y, paramvalues_path, problem_path, save_file):

    # Perform the sensitivity analysis using the model output
    # Specify which column of the output file to analyze (zero-indexed)
    #Si = morris.analyze(problem, param_values_, Y, conf_level=0.95, print_to_console=True, num_levels=4, num_resamples=100)
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

    def sa_results(parameters: dict, X: np.array, Y: np.array, save_file: str):
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

        problem = create_salib_problem(parameters)

        Si = analyze_morris.analyze(problem, X, Y, print_to_console=False)

        # Save text based results
        Si.to_df().to_csv(f'{save_file}.csv')
        
        # save graphical resutls 
        title = Path(save_file).stem.capitalize()
        fig, axs = plt.subplots(2, figsize=(10,8))
        fig.suptitle(title, fontsize=20)
        plot_morris.horizontal_bar_plot(axs[0], Si, unit="(\$)")
        plot_morris.covariance_plot(axs[1], Si, unit="(\$)")

        fig.savefig(f'{save_file}.png', bbox_inches='tight')
    
    with open(problem_path, 'r') as csv_file:
        parameters = list(csv.DictReader(csv_file))

    #for i in dict_y.keys():
    X = np.loadtxt(paramvalues_path, delimiter=",")
    df = pd.DataFrame.from_dict(dict_y, orient="index")
    Y = df.to_numpy()

    sa_results(parameters, X, Y, save_file)
    

def main(folder, outputdataframe):
    years = ['2020', '2021', '2022','2023','2024','2025','2026','2027','2028','2029','2030','2031',	'2032',	'2033',	'2034',	'2035',	'2036',	'2037',	'2038',	'2039',	'2040', '2041']
    dict_df = load_csvs(folder, years)
    dict_results = read_data(dict_df, years)
    totaldiscountedcost, capacity = creating_Y_to_morris(dict_results, 'src/sensitivity', years)
    run_morris(totaldiscountedcost, 'src/sensitivity/sample_morris.csv', 'src/config/parameters.csv', 'src/sensitivity/totaldiscountedcost')
    run_morris(capacity, 'src/sensitivity/sample_morris.csv', 'src/config/parameters.csv', 'src/sensitivity/newcapacity')


main('src/run/temp/results', 'src/run/temp/results')


