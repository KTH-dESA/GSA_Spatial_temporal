"""
Author: Will Usher, modified by Nandi Moksnes
Generates a sample from a list of parameters


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

    name,group,indexes,min_value_base_year,max_value_base_year,min_value_end_year,max_value_end_year,dist,interpolation_index,action
    DiscountRate,discountrate,"REGION",0.05,0.15,0.05,0.15,unif,None,fixed
    CapitalCost,CapitalCostNG,"REGION,AONGCCC01N",2100,3100,742,1800,unif,YEAR,interpolate

The output sample file is formatted as follows::

   'name', 'indexes', 'value_base_year', 'value_end_year', 'action', 'interpolation_index'
    DiscountRate,"REGION",0.05,0.05,fixed,None
    CapitalCost,"REGION,AONGCCC01N",2100,742,interpolate,YEAR

"""
import os
import csv
import numpy as np
from typing import List
import sys

from logging import getLogger

logger = getLogger(__name__)

def expand(morris_sample, parameters, output_files):
    sample_list = []
    for model_run, sample_row in enumerate(morris_sample):
        filepath = output_files + '/sample_'+str(int(model_run))+".txt"
        sample_list += [filepath]
        with open(filepath, 'w') as csvfile:

            fieldnames = ['name', 'indexes', 'value_base_year', 'value_end_year', 'action', 'interpolation_index']
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

                value_base_year = (max_by - min_by) * column + min_by
                value_end_year =  (max_ey - min_ey) * column + min_ey

                data = {'name': param['name'],
                        'indexes': param['indexes'],
                        'value_base_year': value_base_year,
                        'value_end_year': value_end_year,
                        'action': param['action'],
                        'interpolation_index': param['interpolation_index']}
                writer.writerow(data)

    return sample_list