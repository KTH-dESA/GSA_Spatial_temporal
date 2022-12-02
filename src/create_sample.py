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

The ``parameters.csv`` CSV file should be formatted as follows::

    name,group,indexes,min_value_base_year,max_value_base_year,min_value_end_year,max_value_end_year,dist,interpolation_index,action
    DiscountRate,discountrate,"REGION",0.05,0.15,0.05,0.15,unif,None,fixed
    CapitalCost,CapitalCostNG,"REGION,AONGCCC01N",2100,3100,742,1800,unif,YEAR,interpolate

"""
from SALib.sample import morris
import os
import numpy as np
import csv
from typing import List
import sys

from logging import getLogger

logger = getLogger(__name__)

def createsample(parameters: List, sample_file: str, replicates: int):

    problem = {}
    problem['num_vars'] = len(parameters)

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

    sample = morris.sample(problem, N=50, optimal_trajectories=replicates,
                           local_optimization=True, seed=42)

    #modified_values = sample.copy()

    # We then `floor` the values for the categorical inputs
    # e.g., everything < 1 will be mapped to 0, between 1.0 and 2.0 as 1, etc



    #modified_values[:, 0:len(parameters)] = np.floor(modified_values[:, 0:len(parameters)])
    np.savetxt(sample_file, sample, delimiter=',')


