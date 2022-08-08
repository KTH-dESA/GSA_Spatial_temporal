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
import argparse
import sys
import logging
from datetime import datetime
from build_data_file_ref import load_csvs, make_outputfile, functions_to_run, write_to_file

logger = logging.getLogger(__name__)

def parse_args(args):
    """Parse command line parameters
    Args:
      args ([str]): command line parameters as list of strings
    Returns:
      :obj:`argparse.Namespace`: command line parameters namespace

    # paths = (os.getcwd() + '\data')
    # path = os.getcwd()
    # file_object= os.getcwd() + r'\results\GIS.txt'
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


def run(argv):
    """Entry point for console_scripts
    """
    args = parse_args(argv)

    dict_df = load_csvs(args.data_path)
    outPutFile = make_outputfile(args.template)
    outPutFile = functions_to_run(dict_df, outPutFile)
    #write data file
    write_to_file(args.output_path, outPutFile)


if __name__ == "__main__":
    run(sys.argv[1:])
