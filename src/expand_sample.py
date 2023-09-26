"""
Author: Will Usher, modified by Nandi Moksnes
Generates a sample from a list of parameters, and a sample file of the mean nominal values of x as input to Scaled elementary effect


Arguments
---------
replicates : int
    The number of model runs to generate
path_to_parameters : str
    File containing the parameters to generate a sample for

Usage
-----
To run the script on the command line, type::

    python create_sample.py 10 path/to/parameters.csv

The ``parameters.csv`` CSV input file should be formatted as follows::

    name,group,indexes,min_value_base_year,max_value_base_year,min_value_end_year,max_value_end_year,dist,interpolation_index,floor
    DiscountRate,discountrate,"REGION",0.05,0.15,0.05,0.15,unif,None,fixed, Y
    CapitalCost,CapitalCostNG,"REGION,AONGCCC01N",2100,3100,742,1800,unif,YEAR,interpolate, N

The output sample file is formatted as follows::

   'name', 'indexes', 'value_base_year', 'value_end_year', 'action', 'interpolation_index', 'floor'
    DiscountRate,"REGION",0.05,0.05,fixed,None
    CapitalCost,"REGION,AONGCCC01N",2100,742,interpolate,YEAR

"""
import os
import csv
import numpy as np
from typing import List
import sys
import pandas as pd

from logging import getLogger

logger = getLogger(__name__)

def expand(morris_sample, parameters, output_files, path_sensitivity):
    #Create a dataframe for the nominal x mean values for Scaled elementary Effects (Sin and Gearney, 2009)
    num_rows, num_cols = morris_sample.shape
    df = pd.DataFrame(index=range(num_rows),columns=range(num_cols))
    sample_list = []
    for model_run, sample_row in enumerate(morris_sample):
        nominal_modelrun_x = []
        filepath = output_files + '/sample_'+str(int(model_run))+".txt"
        sample_list += [filepath]
        with open(filepath, 'w') as csvfile:

            fieldnames = ['name', 'indexes', 'value_base_year', 'value_end_year', 'action', 'interpolation_index', 'floor']
            writer = csv.DictWriter(csvfile, fieldnames)
            writer.writeheader()

            for column, param in zip(sample_row, parameters):

                try:
                    min_by = float(param['min_value_base_year'])
                    max_by = float(param['max_value_base_year'])
                    min_ey = float(param['min_value_end_year'])
                    max_ey = float(param['max_value_end_year'])
                except ValueError as ex:
                    print(param)
                    raise ValueError(str(ex))
                if param['floor'] == 'Y':
                    value_base_year = round((max_by - min_by) * column + min_by)
                    value_end_year =  round((max_ey - min_ey) * column + min_ey)
                else:
                    value_base_year = (max_by - min_by) * column + min_by
                    value_end_year =  (max_ey - min_ey) * column + min_ey

                data = {'name': param['name'],
                        'indexes': param['indexes'],
                        'value_base_year': value_base_year,
                        'value_end_year': value_end_year,
                        'action': param['action'],
                        'interpolation_index': param['interpolation_index'],
                        'floor': param['floor']}
                writer.writerow(data)

                # create sample nominal values
                if (value_end_year-value_base_year)==0:
                    mean = value_base_year
                else:
                    mean = value_base_year+(value_end_year-value_base_year)/2
                nominal_modelrun_x.append(mean)
        df.loc[model_run] = nominal_modelrun_x
    df.to_csv(path_sensitivity, index=False, header=False)
    return sample_list
# sample_file = 'kenyasensitivity/sample_morris.csv'
# parameters_file = 'config/kenyaparameters.csv'
# with open(parameters_file, 'r') as csv_file:
#     parameter_list = list(csv.DictReader(csv_file))
# output_files_sample = 'kenya_run/sensitivity_range'
# path_sensitivity = 'kenyasensitivity/sample_morris_nominal.csv'
# morris_sample = np.loadtxt(sample_file, delimiter=",")
# samplelist = expand(morris_sample, parameter_list, output_files_sample, path_sensitivity)