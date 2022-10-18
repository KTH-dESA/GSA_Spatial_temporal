"""
This script is to build a OSeMOSYS data file where you have many locations/countries.
The script is user defined in the geographical, technology, year, timeslice dimesions, which makes the script flexible.

__author__ = "Nandi Moksnes, Sebastian Moksnes", "Will Usher"
__copyright__ = "Nandi Moksnes"
__licence__ = "mit"
"""

import pandas as pd
import numpy as np
import os
import csv
import argparse
import sys
import logging
from datetime import datetime
from run.build_osemosysdatafiles import load_csvs, make_outputfile, functions_to_run, write_to_file
sys.path.insert(1, '../')
from create_sample import createsample
from expand_sample import expand

logger = logging.getLogger(__name__)

def parse_args(args):
    """Parse command line parameters
    Args:
      args ([str]): command line parameters as list of strings
    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace

    param_file = '/osemosys_shell_param.txt'

    """
    parser = argparse.ArgumentParser(
        description="Location specific OSeMOSYS datafile generator")
    parser.add_argument(
        dest="data_path",
        help="Folder containing CSV data files",
        type=str,
        metavar="STRING")
    parser.add_argument(
        dest="template",
        help="Path to the OSeMOSYS template",
        type=str,
        metavar="STRING")
    parser.add_argument(
        dest="output_path",
        help="Name and path of the output file",
        type=str,
        metavar="STRING")

    return parser.parse_args(args)


def run(): #argv
    """Entry point for console_scripts
    """
    #args = parse_args(argv)
    dict_df = load_csvs("src/run/scenarios") #args.data_path) #
    outPutFile = make_outputfile("src/run/Benin.txt")#args.template) #

    ### Scenario settings ###
    
    #TODO Integrate the scenario generator with SNAKEMAKE file. To understand is if I send a number or if I send a file.
    sample_file = 'src/sensitivity/sample_morris.csv'
    parameters_file = 'src/config/parameters.csv'
    replicates = 10
    with open(parameters_file, 'r') as csv_file:
        reader = list(csv.DictReader(csv_file))
    createsample(reader, sample_file, replicates)
    
    output_files = 'src/sensitivity/runs'
    with open(parameters_file, 'r') as csv_file:
        parameter_list = list(csv.DictReader(csv_file))
    morris_sample = np.loadtxt(sample_file, delimiter=",")
    expand(morris_sample, parameter_list, output_files)

    scenario = pd.read_csv(sample_file, header=None)
    for m in range(0,len(scenario.index)):
        spatial = int(scenario[0][m])
        demand_scenario = int(scenario[1][m])
        discountrate = int(scenario[2][m])
        outPutFile = functions_to_run(dict_df, outPutFile, spatial, demand_scenario, discountrate)

        #write data file
        if not os.path.exists('src/run/output'):
            os.makedirs('src/run/output')

        #write to DD-file
        comb = 'Benin'+str(spatial)+str(demand_scenario)+str(discountrate)+'.txt'
        write_to_file('src/run/output', outPutFile, comb)      #args.output_path


if __name__ == "__main__":
    run() #sys.argv[1:]
    