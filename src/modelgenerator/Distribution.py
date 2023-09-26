"""
Module: Distribution
=============================

A module for building the logic around peakdemand, transmissionlines and distributionlines.

---------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""
import pandas as pd
import math
import numpy as np
import os
pd.options.mode.chained_assignment = None
from typing import Any, Union
from numpy import ndarray
from pandas import Series, DataFrame
from pandas.core.arrays import ExtensionArray

def transmission_matrix(path, noHV_file, HV_file, minigridcsv, topath, spatial, country):
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
    near_adj_points["INFUEL"] = np.nan
    near_adj_points["INTECH"] = np.nan

    if country == 'Benin':
        near_adj_points.loc[(near_adj_points.SENDID.isin(HV.id)), 'SendTech'] = 'BENEL1TRP00X'
    else:
        near_adj_points.loc[(near_adj_points.SENDID.isin(HV.id)), 'SendTech'] = 'KEEL00t00'

    #add input fuel and inputtech to central exisiting grid
    central = near_adj_points.loc[(near_adj_points.SENDID.isin(HV.id))]
    central_nogrid = central.loc[central.NEARID.isin(noHV.id)]
    for m in central_nogrid.index:
        near_adj_points.loc[near_adj_points.index == m, 'INFUEL'] = 'TREL2'
        near_adj_points.loc[(near_adj_points.index == m , 'INTECH')] =  "TRHV_"+ str(int(near_adj_points.SENDID[m])) + "_" + str(int(near_adj_points.NEARID[m]))
        near_adj_points.loc[near_adj_points.index == m, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[m]))

    central = near_adj_points.loc[(near_adj_points.SENDID.isin(HV.id))]

    # central_minigrid = central.loc[central.NEARID.isin(minigrid.id)]
    # m=0
    # for m in central_minigrid.index:
    #     near_adj_points.loc[near_adj_points.index == m, 'INFUEL'] = 'TREL2'
    #     near_adj_points.loc[(near_adj_points.index == m , 'INTECH')] = "TRHV_"+ str(int(near_adj_points.SENDID[m])) + "_" + str(int(near_adj_points.NEARID[m]))
    #     near_adj_points.loc[near_adj_points.index == m, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[m]))

    #select where no inputfuel is present and their recieving cell has no HV in baseyear
    nan_intech = near_adj_points.loc[near_adj_points.INFUEL.isnull()]
    nan_intech_nogrid = nan_intech.loc[nan_intech.NEARID.isin(noHV.id)]
    #add input fuel to the (isnan INFUEL + isin noHV) selection
    m = 0
    for l in nan_intech_nogrid.index:
        near_adj_points.loc[near_adj_points.index == l, 'INFUEL'] = "EL2_" + str(int(near_adj_points.SENDID[l]))
        near_adj_points.loc[near_adj_points.index == l , 'INTECH'] = "TRHV_" + str(int(near_adj_points.SENDID[l])) + "_" + str(int(near_adj_points.NEARID[l]))
        near_adj_points.loc[near_adj_points.index == l, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[l]))

    # nan_intech_minigr = near_adj_points.loc[near_adj_points.INFUEL.isnull()]
    # nan_intech_minigrid = nan_intech_minigr.loc[nan_intech_minigr.NEARID.isin(minigrid.id)]
    # #add input fuel to the (isnan INFUEL + isin noHV) selection

    # for l in nan_intech_minigrid.index:
    #     near_adj_points.loc[near_adj_points.index == l, 'INFUEL'] = "EL2_" + str(int(near_adj_points.SENDID[l]))
    #     near_adj_points.loc[near_adj_points.index == l , 'INTECH'] = "TRHV_" + str(int(near_adj_points.SENDID[l])) + "_" + str(int(near_adj_points.NEARID[l]))
    #     near_adj_points.loc[near_adj_points.index == l, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[l]))


    try:
    #Allow for connections over cells with no population ("nan")
        not_grid = near_adj_points[~near_adj_points.SENDID.isin(HV.id)]
        nan = not_grid.loc[not_grid.INFUEL.isnull()]
        not_grid_reciever = nan[~nan.NEARID.isin(HV.id)]
        for j in not_grid_reciever.index:
            near_adj_points.loc[near_adj_points.index == j, 'INFUEL'] = "EL2_" + str(int(near_adj_points.SENDID[j]))
            near_adj_points.loc[near_adj_points.index == j , 'INTECH'] = "TRHV_" + str(int(near_adj_points.SENDID[j])) + "_" + str(int(near_adj_points.NEARID[j]))
            near_adj_points.loc[near_adj_points.index == j, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[j]))
    except:
        print('no cells wth no population to overlap')
    #try:
    nan_matrix = near_adj_points.loc[near_adj_points.INTECH.notnull()]
    #concat the two dataframes with the transmissionlines to one
    final_matrix = nan_matrix.drop(['OBJECTID *','INPUT_FID','NEAR_FID','NEARID','SENDID'], axis=1)
    final_matrix = final_matrix.drop_duplicates()

    final_matrix.to_csv(os.path.join(topath,'%i_adjacencymatrix.csv' %(spatial)))
    return(final_matrix)

    #print("No neartable in the folder. Please check that there are no cells to connect.")
    #exit

def transformer_calculation(distributionlines_file, distributioncelllength_file, demandcells_csv, input, scenario, tofolder, cost_transf, connectioncost):
    # profile = pd.read_csv(specifieddemand,index_col='Timeslice', header=0)
    # demand = pd.read_csv(demand_csv, header=0)
    # HV = pd.read_csv(HV_csv, header=0)
    demand_cells =  pd.read_csv(demandcells_csv, header=0)
    # #demand['cell'] = demand['Fuel'].apply(lambda row: row.split("_")[1])
    # demand.index = demand['Fuel']
    # demand = demand.drop(['Fuel'], axis=1)
    # yearsplit = pd.read_csv(yearsplit_csv, index_col='Timeslice', header=0)

    # #The peakdemand is defined as:
    # # Peakdemand = specifiedannualdemand*specifieddemandprofile/(capacitytoactivityunit*yearsplit)

    # demand_capacitytoact = demand.apply(lambda row: row/capacitytoactivity,axis=1)
    # profile_yearsplit = profile.divide(yearsplit)
    # peak = profile_yearsplit.max()
    # peak_demand_all = demand_capacitytoact.apply(lambda row: row * peak, axis=1)
    distributionlines = pd.read_csv(distributionlines_file)
    distributioncelllength= pd.read_csv(distributioncelllength_file)

    #van ruijven transformer calculations
    input_data = pd.read_csv(input)
    demand_cells['Area'] = int( input_data['Area_cell_size'][0])
    demand_cells['Inhibited_area'] =input_data['Inhibited_area'][0] #https://lutw.org/wp-content/uploads/Energy-services-for-the-millennium-development-goals.pdf Box II.I The example for disaggregation factor is based on the spread of population in an area.
    demand_cells['Household_size'] = input_data['HH'][0]
    demand_cells['peak_W'] = input_data['peak(Watt)'][0]
    demand_cells['LVarea'] = input_data['LV_area'][0]
    demand_cells['capacity'] = input_data['MaxCapacityLV(W)'][0]

    network_list = []
    demand_cells['HH'] = demand_cells['pop']/demand_cells['Household_size']
    demand_cells['NRLVs'] = demand_cells['HH']*demand_cells['peak_W'] /demand_cells['capacity']
    demand_cells['minLV'] = demand_cells['Inhibited_area']/ demand_cells['LVarea']
    demand_cells['LV_km'] = np.nan
    for i,row in demand_cells.iterrows():
        row['LV_SWER'] = min(row['HH'],max(row['minLV'],row['NRLVs']))
        row['num_transf'] = row['LV_SWER'] + 1 #assumed 1 SWER per cell as the size is so small
        network_list.append(row)
        ind = row.index
    
    distributionlines = distributionlines.set_index(distributionlines.iloc[:, 0])
    distributionlines.index = distributionlines['id']
    distribution = distributionlines.drop(columns =['id.1', 'id'])

    distributioncelllength.index = distributioncelllength['id']
    distribtionlength = distributioncelllength.drop(['Unnamed: 0', 'id', 'elec'], axis = 1)

    distribution_total = distribution.multiply(distribtionlength.LV_km, axis = "rows")

    networkkm = pd.DataFrame(network_list, columns=ind)
    transformers =  networkkm[['elec', 'id', 'HH', 'num_transf']]
    average_transfomers = transformers[transformers['elec'] == 0]
    transformers_aggr = average_transfomers.groupby(["id"])
    transform_cost = transformers_aggr.sum().reset_index()
    for i,row in transform_cost.iterrows():
        transformcost = (transform_cost['num_transf']*cost_transf + transform_cost['HH']*connectioncost)

    transformcost.to_csv(os.path.join(os.getcwd(), tofolder,'%i_transformers.csv' %(scenario)))


def peakdemand_csv(demand_csv, specifieddemand,capacitytoactivity, yearsplit_csv, distr_losses, HV_csv, distributionlines_file, distribution_header, distributioncelllength_file, tofolder, spatail, demand_scneario, temporal_id):
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
    distributionlines.index = distributionlines['id']
    distribution = distributionlines.drop(columns =['id.1', 'id'])

    distributioncelllength.index = distributioncelllength['id']
    distribtionlength = distributioncelllength.drop(['Unnamed: 0', 'id', 'elec'], axis = 1)

    distribution_total = distribution.multiply(distribtionlength.LV_km, axis = "rows")

    peakdemand.index = peakdemand[('cell')]
    a = distribution_total.index
    peakdemand_divided_km = peakdemand.apply(lambda x: (x/distribution_total.loc[x['cell']]['sum'] if x['cell'] in a else print('not same')), axis=1)
    peakdemand_divided_km.fillna(0,inplace=True)
    peakdemand_divided_km['Fuel'] = peakdemand.index.to_series().apply(lambda row: 'TRLV_'+str(row)+'_0')
    peakdemand_divided_km.index = peakdemand_divided_km['Fuel']
    peakdemand_divided_km_cleaned = peakdemand_divided_km.drop(['cell', 'Fuel'], axis=1)

    peakdemandLVM_ = peakdemand.loc[peakdemand['cell'].isin(HV.id)]
    peakdemandLVM_divided_km = peakdemandLVM_.apply(lambda x: (x/distribution_total.loc[x['cell']][0] if x['cell'] in a else print('not same')), axis=1)
    peakdemandLVM_divided_km.fillna(0,inplace=True)
    peakdemandLVM_divided_km['Fuel'] = peakdemandLVM_divided_km.index.to_series().apply(lambda row: 'TRLVM_'+str(row)+'_0')
    peakdemandLVM_divided_km.index = peakdemandLVM_divided_km['Fuel']
    peakdemandLVM_divided_km_cleaned = peakdemandLVM_divided_km.drop(['cell', 'Fuel'], axis=1)

    TRLV_TRLVM = pd.concat([peakdemand_divided_km_cleaned, peakdemandLVM_divided_km_cleaned])

    TRLV_TRLVM.to_csv(os.path.join(tofolder,'%i_%i_%ipeakdemand.csv') %(spatail, demand_scneario, temporal_id))

def distribution_elec_startyear(demand, capacitytoactivity, distrlosses,basetopeak, years, savepath, PVshare_baseyear, PV_capacityfactor):
    demand_df =pd.read_csv(demand)
    elecdemand = demand_df.loc[demand_df.Fuel.str.endswith('_1', na=False)]
    elecdemand['Technology'] = elecdemand['Fuel'].str.replace('EL3_', 'EL00d_')
    elecdemand['Technology'] = elecdemand['Technology'] .str.slice(stop=-2)
    elecdemand.drop('Fuel', axis=1)
    elecdemand.index = elecdemand['Technology']

    demand_column = elecdemand[years[0]]
    capacity_distrib  = demand_column.apply(lambda row: (row*basetopeak/(capacitytoactivity*distrlosses))*(1-PVshare_baseyear))

    elecdemand_PV = demand_df.loc[demand_df.Fuel.str.endswith('_1', na=False)]
    elecdemand_PV['Technology'] = elecdemand_PV['Fuel'].str.replace('EL3_', 'SOPVBattery_')
    elecdemand_PV.drop('Fuel', axis=1)
    elecdemand_PV.index = elecdemand_PV['Technology']

    demand_column_PV = elecdemand_PV[years[0]]
    capacity_distrib_PV  = demand_column_PV.apply(lambda row: (row/(capacitytoactivity*PV_capacityfactor))*(PVshare_baseyear))

    capacity_concat = pd.concat([capacity_distrib_PV, capacity_distrib])

    #capacity_distrib = elecdemand['new_column']
    #df= capacity_distrib
    multiple = pd.DataFrame({f'col{i+1}': capacity_concat for i in range((len(years)))})
    multiple.columns = years
    multiple.index = capacity_concat.index
    multiple.to_csv(savepath)
