"""Creates model runs from a sample file and master model datapackage

Arguments
---------
<input_filepath>
    Path to the master model datapackage
<output_filepath>
    Path to the new model file
<sample_filepath>
    Path the sample file

The expected format of the input sample file is a CSV file with the following structure::

    name,indexes,value_base_year,value_end_year,action,interpolation_index
    CapitalCost,"GLOBAL,GCPSOUT0N",1024.3561663863075,2949.23939,interpolate,YEAR

It is very similar to the overall ``parameter.csv`` configuration file, except holds a sample value
rather than a range

To run this script on the command line, use the following::

    python create_modelrun.py <input_filepath> <output_filepath> <sample_filepath>

"""
import sys

from typing import Dict, List, Union, Tuple
import os
import csv
import pandas as pd
import numpy as np

from logging import getLogger

logger = getLogger(__name__)

def process_data(
                 start_year_value: float,
                 end_year_value: float,
                 first_year: int,
                 last_year: int
                 ) -> pd.DataFrame:
    """Interpolate data between min and max years

    Arguments
    ---------
    start_year_value: float
        Value of the parameter in the start year
    end_year_value: float
        Value of the parameter in the end year
    first_year: int
        First year of the range to interpolate
    last_year: int
        Last year of the range to interpolate
    """
    # df.index = df.index.sortlevel(level=0)[0]

    values = np.interp([range(int(first_year), int(last_year) + 1)],
                       np.array([int(first_year), int(last_year)]),
                       np.array([float(start_year_value), float(end_year_value)])).T

    # df.loc[tuple(index + [first_year]):tuple(index + [last_year])] = values
    return values


def get_types_from_tuple(index: list, param: str, config: Dict) -> Tuple:
    depth = len(index)
    names = config[param]['indices'][0:depth]
    typed_index = []
    dtypes = config[param]['index_dtypes']
    for name, element in zip(names, index):
        this_type = dtypes[name]
        if this_type == 'str':
            typed_index.append(str(element))
        elif this_type == 'float':
            typed_index.append(float(element))
        elif this_type == 'int':
            typed_index.append(int(element))

    return typed_index


def modify_parameters(
        inputdata,
        parameters: List[Dict[str, Union[str, int, float]]], 
        sample,
        tofolder):
    """
    """
    first_year = inputdata['startyear'][0]
    end_year = inputdata['endyear'][0]
    expanded_data = []
    name_ = []
    for parameter in parameters:

        name = parameter['name']
        #df = model_params[name]
        untyped_index = parameter['indexes'].split(",")
        #index = get_types_from_tuple(untyped_index, name, config)
        start_year_value = float(parameter['value_base_year'])
        end_year_value = parameter['value_end_year']
        action = parameter['action']
        inter_index = parameter['interpolation_index']
        if action == 'interpolate':
            new_values = process_data(start_year_value, end_year_value, first_year, end_year)
        elif action == 'fixed':
            if inter_index == 'None':
                # Create new object and update inplace
                new_values = [start_year_value]
            elif inter_index == 'YEAR':
                # Create new object and update inplace
                new_values = np.full((end_year + 1 - first_year, 1), start_year_value)

        if inter_index == 'YEAR':
            #logger.info("Updating values for {} in {}".format(index, name))
            try:
                expanded_data.append(new_values.tolist())
                name_.append(name)
            except ValueError as ex:
                msg = "Error raised in parameter {} by index {}"
        else:
            try:
                expanded_data.append(new_values[0])
                name_.append(name)

            except ValueError as ex:
                msg = "Error raised in parameter {} by index {}"
                raise ValueError(ex)
    df = pd.DataFrame(expanded_data, index=name_)
    df.to_csv(os.path.join(tofolder+sample+'.csv'))
    return expanded_data


