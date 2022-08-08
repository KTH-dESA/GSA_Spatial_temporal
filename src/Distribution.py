"""
Module: Distribution
=============================

A module for building the logic around peakdemand, transmissionlines and distributionlines.

---------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""
import pandas as pd
import os
pd.options.mode.chained_assignment = None
from typing import Any, Union
from numpy import ndarray
from pandas import Series, DataFrame
from pandas.core.arrays import ExtensionArray

def transmission_matrix(path, noHV_file, HV_file, minigridcsv, topath):
    """
    This function creates transmission lines in both directions for each cell and connects the adjacent cells to grid to the central grid.
    :param path:
    :param noHV_file:
    :param HV_file:
    :param minigridcsv:
    :param topath:
    :return:
    """
    noHV = pd.read_csv(noHV_file)
    HV = pd.read_csv(HV_file)
    minigrid = pd.read_csv(minigridcsv)
    neartable = pd.read_csv(path)
    # The table includes the raw data from ArcMap function
    near_adj_points: Union[Union[Series, ExtensionArray, ndarray, DataFrame, None], Any] = neartable[neartable["DISTANCE"] > 0]

    near_adj_points.loc[(near_adj_points.SENDID.isin(HV.pointid)), 'SendTech'] = 'KEEL00t00'

    #add input fuel and inputtech to central exisiting grid
    central = near_adj_points.loc[(near_adj_points.SENDID.isin(HV.pointid))]
    central_nogrid = central.loc[central.NEARID.isin(noHV.pointid)]
    for m in central_nogrid.index:
        near_adj_points.loc[near_adj_points.index == m, 'INFUEL'] = 'KEEL2'
        near_adj_points.loc[(near_adj_points.index == m , 'INTECH')] =  "TRHV_"+ str(int(near_adj_points.SENDID[m])) + "_" + str(int(near_adj_points.NEARID[m]))
        near_adj_points.loc[near_adj_points.index == m, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[m]))

    central = near_adj_points.loc[(near_adj_points.SENDID.isin(HV.pointid))]

    central_minigrid = central.loc[central.NEARID.isin(minigrid.pointid)]
    for m in central_minigrid.index:
        near_adj_points.loc[near_adj_points.index == m, 'INFUEL'] = 'KEEL2'
        near_adj_points.loc[(near_adj_points.index == m , 'INTECH')] = "TRHV_"+ str(int(near_adj_points.SENDID[m])) + "_" + str(int(near_adj_points.NEARID[m]))
        near_adj_points.loc[near_adj_points.index == m, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[m]))

    #select where no inputfuel is present and their recieving cell has no HV in baseyear
    nan_intech = near_adj_points.loc[near_adj_points.INFUEL.isnull()]
    nan_intech_nogrid = nan_intech.loc[nan_intech.NEARID.isin(noHV.pointid)]
    #add input fuel to the (isnan INFUEL + isin noHV) selection
    m = 0
    for l in nan_intech_nogrid.index:
        near_adj_points.loc[near_adj_points.index == l, 'INFUEL'] = "EL2_" + str(int(near_adj_points.SENDID[l]))
        near_adj_points.loc[near_adj_points.index == l , 'INTECH'] = "TRHV_" + str(int(near_adj_points.SENDID[l])) + "_" + str(int(near_adj_points.NEARID[l]))
        near_adj_points.loc[near_adj_points.index == l, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[l]))

    nan_intech_minigr = near_adj_points.loc[near_adj_points.INFUEL.isnull()]
    nan_intech_minigrid = nan_intech_minigr.loc[nan_intech_minigr.NEARID.isin(minigrid.pointid)]
    #add input fuel to the (isnan INFUEL + isin noHV) selection

    for l in nan_intech_minigrid.index:
        near_adj_points.loc[near_adj_points.index == l, 'INFUEL'] = "EL2_" + str(int(near_adj_points.SENDID[l]))
        near_adj_points.loc[near_adj_points.index == l , 'INTECH'] = "TRHV_" + str(int(near_adj_points.SENDID[l])) + "_" + str(int(near_adj_points.NEARID[l]))
        near_adj_points.loc[near_adj_points.index == l, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[l]))

    #Allow for connections over cells with no population ("nan")
    not_grid = near_adj_points[~near_adj_points.SENDID.isin(HV.pointid)]
    nan = not_grid.loc[not_grid.INFUEL.isnull()]
    not_grid_reciever = nan[~nan.NEARID.isin(HV.pointid)]
    for j in not_grid_reciever.index:
        near_adj_points.loc[near_adj_points.index == j, 'INFUEL'] = "EL2_" + str(int(near_adj_points.SENDID[j]))
        near_adj_points.loc[near_adj_points.index == j , 'INTECH'] = "TRHV_" + str(int(near_adj_points.SENDID[j])) + "_" + str(int(near_adj_points.NEARID[j]))
        near_adj_points.loc[near_adj_points.index == j, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[j]))

    nan_matrix = near_adj_points.loc[near_adj_points.INTECH.notnull()]
    #concat the two dataframes with the transmissionlines to one
    final_matrix = nan_matrix.drop(['OBJECTID *','INPUT_FID','NEAR_FID','NEARID','SENDID'], axis=1)
    final_matrix = final_matrix.drop_duplicates()

    final_matrix.to_csv(os.path.join(topath,'adjacencymatrix.csv'))
    return(final_matrix)

def peakdemand_csv(demand_csv, specifieddemand,capacitytoactivity, yearsplit_csv, distr_losses, HV_csv, distributionlines_file, distributioncelllength_file, tofolder):
    """
    This function calculates the peakdemand per year and demand and divides it with the estimated km.
    :param demand_csv:
    :param specifieddemand:
    :param capacitytoactivity:
    :param yearsplit_csv:
    :param distr_losses:
    :param distributionlines_file:
    :param distributioncelllength_file:
    :param tofolder:
    :return:
    """
    profile = pd.read_csv(specifieddemand,index_col='Timeslice', header=0)
    demand = pd.read_csv(demand_csv, header=0)
    HV = pd.read_csv(HV_csv, header=0)
    #demand['cell'] = demand['Fuel'].apply(lambda row: row.split("_")[1])
    demand.index = demand['Fuel']
    demand = demand.drop(['Fuel'], axis=1)
    yearsplit = pd.read_csv(yearsplit_csv, index_col='Timeslice', header=0)
    distributionlines = pd.read_csv(distributionlines_file)
    distributioncelllength= pd.read_csv(distributioncelllength_file)

    #The peakdemand is defined as the peak demand over km per cell
    # Peakdemand = specifiedannualdemand*specifieddemandprofile/(capacitytoactivityunit*yearsplit)/km_cell

    demand_capacitytoact = demand.apply(lambda row: row/capacitytoactivity,axis=1)
    profile_yearsplit = profile.divide(yearsplit)
    max_share_peryear = profile_yearsplit.max()/distr_losses

    peak_demand_all = demand_capacitytoact.apply(lambda row: row * max_share_peryear, axis=1)
    peakdemand = peak_demand_all.loc[peak_demand_all.index.str.endswith('_0', na=False)]
    peakdemand.index = peakdemand.index.str.replace('EL3','TRLV')
    peakdemand['cell'] = peakdemand.index.to_series().apply(lambda row: int(row.split("_")[1]))

    distributionlines = distributionlines.set_index(distributionlines.iloc[:, 0])
    distribution = distributionlines.drop(columns ='Unnamed: 0')

    distributioncelllength.index = distributioncelllength['pointid']
    distribtionlength = distributioncelllength.drop(['Unnamed: 0', 'pointid', 'elec'], axis = 1)

    distribution_total = distribution.multiply(distribtionlength.LV_km, axis = "rows")
    peakdemand.index = peakdemand[('cell')]
    a = distribution_total.index
    peakdemand_divided_km = peakdemand.apply(lambda x: (x/distribution_total.loc[x['cell']][0] if (x['cell']==a).any() else print('not same')), axis=1)
    peakdemand_divided_km['Fuel'] = peakdemand.index.to_series().apply(lambda row: 'TRLV_'+str(row)+'_0')
    peakdemand_divided_km.index = peakdemand_divided_km['Fuel']
    peakdemand_divided_km_cleaned = peakdemand_divided_km.drop(['cell', 'Fuel'], axis=1)

    peakdemandLVM_ = peakdemand.loc[peakdemand['cell'].isin(HV.pointid)]
    peakdemandLVM_divided_km = peakdemandLVM_.apply(lambda x: (x/distribution_total.loc[x['cell']][0] if (x['cell']==a).any() else print('not same')), axis=1)
    peakdemandLVM_divided_km['Fuel'] = peakdemandLVM_divided_km.index.to_series().apply(lambda row: 'TRLVM_'+str(row)+'_0')
    peakdemandLVM_divided_km.index = peakdemandLVM_divided_km['Fuel']
    peakdemandLVM_divided_km_cleaned = peakdemandLVM_divided_km.drop(['cell', 'Fuel'], axis=1)

    TRLV_TRLVM = peakdemand_divided_km_cleaned.append(peakdemandLVM_divided_km_cleaned)

    TRLV_TRLVM.to_csv(os.path.join(tofolder,'peakdemand.csv'))
