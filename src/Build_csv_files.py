"""
Module: Build_csv_files
=============================

A module for building the csv-files for GEOSeMOSYS https://github.com/KTH-dESA/GEOSeMOSYS to run that code
In this module the logic around electrified and un-electrified cells are implemented for the 378 cells

----------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""
from typing import Any, Union

import pandas as pd
import geopandas as gpd
import sys
import os
import fnmatch
from geopandas.tools import sjoin
from numpy import ndarray
from pandas import Series, DataFrame
from pandas.core.arrays import ExtensionArray
pd.options.mode.chained_assignment = None


def renewableninja(path, dest):
    """
    This function organize the data to the required format of a matrix with the
    location name on the x axis and hourly data on the y axis so that it can be fed into https://github.com/KTH-dESA/GEOSeMOSYS code
    the data is saved as capacityfactor_wind.csv and capacityfactor_solar.csv
    :param path:
    :return:
    """
    files = os.listdir(path)
    outwind = []
    outsolar = []
    for file in files:

        if fnmatch.fnmatch(file, 'timezoneoffsetout_wind*'):

            file = os.path.join(path,file)
            wind = pd.read_csv(file, index_col='adjtime')
            outwind.append(wind)
    for file in files:

        if fnmatch.fnmatch(file, 'timezoneoffsetout_solar*'):

            file = os.path.join(path,file)
            solar = pd.read_csv(file, index_col='adjtime')
            outsolar.append(solar)

    solarbase = pd.concat(outsolar, axis=1)
    windbase = pd.concat(outwind, axis=1)

    header = solarbase.columns
    new_header = [x.replace('X','') for x in header]
    solarbase.columns = new_header

    solarbase.drop('Unnamed: 0', axis='columns', inplace=True)
    solarbase.to_csv(os.path.join(dest, 'capacityfactor_solar.csv'))

    header = windbase.columns
    new_header = [x.replace('X','') for x in header]
    windbase.columns = new_header
    windbase.drop('Unnamed: 0', axis='columns', inplace=True)
    windbase.to_csv(os.path.join(dest, 'capacityfactor_wind.csv'))
    return()

def GIS_file(dest, fil):
    point = gpd.read_file(os.path.join('../Projected_files/',fil.loc['point','filename']))
    GIS_data = point['pointid']
    grid = pd.DataFrame(GIS_data, copy=True)
    grid.columns = ['Location']
    grid.to_csv(os.path.join(dest, 'GIS_data.csv'), index=False)
    return()

## Build files with elec/unelec aspects
def capital_cost_transmission_distrib(capital_cost_LV_strengthening, distribution_network, elec, noHV_file, HV_file, unelec, capital_cost_HV, substation, capital_cost_LV, capacitytoactivity, distrbution_cost, path, distribution_length_cell, adjacencymatrix):
    """Reads the transmission lines shape file, creates empty files for inputactivity, outputactivity, capitalcost for transmission lines, ditribution lines and distributed supply options

    :param distribution_network: a csv file with number of cells included in Pathfinder
    :param elec: are the 40*40m cells that have at least one cell of electrified 1x1km inside it
    :param unelec: are the 40*40m cells that have NO electrified cells 1x1km inside it
    :param noHV: are the cells that are electrified and not 5000 m from a minigrid and not 50,000 m from the exsiting HV-MV grid the cell are concidered electrified by transmission lines.
    :param transmission_near: Is the distance to closest HV line from the center of the 40*40 cell
    :param capital_cost_HV: kUSD/MW
    :param substation: kUSD/MW
    :param capital_cost_LV: kUSD/MW
    :return:
    """

    #gdf = gpd.read_file(transmission_near)
    #transm = pd.DataFrame(gdf)
    #transm.index = transm['pointid']

    capitalcost = pd.DataFrame(columns=['Technology', 'Capitalcost'], index=range(0,5000)) # dtype = {'Technology':'object', 'Capitalcost':'float64'}

    fixedcost = pd.DataFrame(columns=['Technology', 'Fixed Cost'], index=range(0,5000)) # dtype = {'Technology':'object', 'Capitalcost':'float64'}

    inputactivity = pd.DataFrame(columns=['Column','Fuel','Technology','Inputactivity','ModeofOperation'], index=range(0,10000))

    outputactivity = pd.DataFrame(columns=['Column','Fuel',	'Technology','Outputactivity','ModeofOperation'], index=range(0,10000))

    elec = pd.read_csv(elec)
    matrix = pd.read_csv(adjacencymatrix)
    elec.pointid = elec.pointid.astype(int)
    un_elec = pd.read_csv(unelec)
    un_elec.pointid = un_elec.pointid.astype(int)
    noHV = pd.read_csv(noHV_file)
    HV = pd.read_csv(HV_file)
    noHV.pointid = noHV.pointid.astype(int)
    distribution = pd.read_csv(distribution_network, index_col=0)
    xls = pd.ExcelFile(distribution_length_cell)
    dist_length = pd.read_excel(xls, 'distance_average', index_col='Row Labels')


    m = 0
    input_temp = []
    output_temp = []
    capital_temp = []

    ## Electrified cells
    for i in elec['pointid']:

        capitalcost.loc[m]['Capitalcost'] = distribution.loc[i, distrbution_cost] * capital_cost_LV * dist_length.loc[i, 'Average of Tier2_LV_length_(km)'] + substation
        capitalcost.loc[m]['Technology'] = "TRLV_%i_0" % (i)

        fixedcost.loc[m]['Fixed Cost'] = distribution.loc[i, distrbution_cost] * capital_cost_LV *  dist_length.loc[i, 'Average of Tier2_LV_length_(km)']* 0.025 + substation * 0.025
        fixedcost.loc[m]['Technology'] = "TRLV_%i_0" % (i)

        m = m+1
        capitalcost.loc[m]['Capitalcost'] = distribution.loc[i, distrbution_cost] * capital_cost_LV_strengthening * dist_length.loc[i, 'Average of Tier2_LV_length_(km)'] + substation
        capitalcost.loc[m]['Technology'] = "TRLV_%i_1" % (i)

        fixedcost.loc[m]['Fixed Cost'] = distribution.loc[i, distrbution_cost] * capital_cost_LV * 0.025*  dist_length.loc[i, 'Average of Tier2_LV_length_(km)']+ substation * 0.025
        fixedcost.loc[m]['Technology'] = "TRLV_%i_1" % (i)

        input_temp = [0,"EL2_%i" %(i),"TRLV_%i_1" %(i), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        input_temp = [0,"EL2_%i" %(i),"TRLV_%i_0" %(i), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()


        output_temp = [0, "EL3_%i_1" % (i), "TRLV_%i_1" % (i), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (i), "BACKSTOP", 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (i), "TRLV_%i_0" % (i), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (i), "BACKSTOP", 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (i),  "SOPV4r_%i_1" % (i), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (i),  "SOPV4r_%i_0" % (i), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (i),  "SOPV_%i_1" % (i), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (i),  "SOPV_%i_0" % (i), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        m = m+1


    for k in HV['pointid']:

        input_temp = [0, "EL2", "TRLV_%i_0" %(k), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        input_temp = [0, "EL2", "TRLV_%i_1" %(k), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        input_temp = [0, "EL2","KEEL00d_%i" %(k), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (k), "EL00d_%i" % (k), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

    for j in un_elec['pointid']:
        capitalcost.loc[m]['Capitalcost'] = distribution.loc[j,distrbution_cost]*capital_cost_LV* dist_length.loc[j, 'Average of Tier2_LV_length_(km)']+ substation
        capitalcost.loc[m]['Technology'] = "TRLV_%i_0" %(j)

        fixedcost.loc[m]['Fixed Cost'] = distribution.loc[j,distrbution_cost]*capital_cost_LV*0.025* dist_length.loc[j, 'Average of Tier2_LV_length_(km)'] + substation*0.025
        fixedcost.loc[m]['Technology'] = "TRLV_%i_0" %(j)

        m = m+1

        input_temp = [0, "EL2_%i" %(j),"TRLV_%i_0" %(j), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()


        output_temp = [0, "EL3_%i_0" % (j), "TRLV_%i_0" % (j), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,  "EL3_%i_0" % (j), "BACKSTOP", 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,"EL3_%i_0" % (j),"SOPV4r_%i_0" % (j), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,"EL3_%i_0" % (j),"SOPV_%i_0" % (j), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

    #For all cells
    for k in range(1,379):
        output_temp = [0,  "EL2_%i" % (k),"SOMG_%i" %(k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,  "EL2_%i" % (k),"SOMG4c_%i" %(k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,  "EL2_%i" % (k),"WI_%i" %(k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,  "EL2_%i" % (k),"WI4c_%i" %(k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,  "EL2_%i" % (k),"DSGEN_%i" %(k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        input_temp = [0,  "DS", "DSGEN_%i" %(k), 4, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

    output_matrix = matrix.drop(['INFUEL','SendTech','ReceiveTech','Unnamed: 0'], axis=1)
    matrix_out = output_matrix.drop_duplicates()

    for l in matrix_out.index:
        output_temp = [0,  matrix.loc[l]['OUTFUEL'], matrix.loc[l]['INTECH'], 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

    input_matrix = matrix.drop(['OUTFUEL','SendTech','ReceiveTech','Unnamed: 0'], axis=1)
    matrix_in = input_matrix.drop_duplicates()

    for l in matrix_in.index:
        input_temp = [0,  matrix.loc[l]['INFUEL'], matrix.loc[l]['INTECH'], 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

    tech_matrix = matrix.drop(['SendTech','INFUEL','OUTFUEL','ReceiveTech','Unnamed: 0'], axis=1)
    tech_matr = tech_matrix.drop_duplicates()
    for h in tech_matr.index:
        capitalcost.loc[m]['Capitalcost'] = tech_matr['DISTANCE'] /1000*capital_cost_HV + substation  #kUSD/MW divided by 1000 as it is in meters
        capitalcost.loc[m]['Technology'] =  matrix.loc[h]['INTECH']

        fixedcost.loc[m]['Fixed Cost'] = tech_matr['DISTANCE']/1000*capital_cost_HV*0.025 + substation*0.025  #kUSD/MW divided by 1000 as it is in meters
        fixedcost.loc[m]['Technology'] =  matrix.loc[h]['INTECH']
        m = m+1

    df1 = outputactivity['Fuel']
    df2 = inputactivity['Fuel']

    fuels = pd.concat([df1, df2]).drop_duplicates().reset_index(drop=True)
    tech1 = outputactivity['Technology']
    tech2 = inputactivity['Technology']
    technolgies = pd.concat([tech1, tech2]).drop_duplicates().reset_index(drop=True)

    df3 = outputactivity['Technology']
    df4 = inputactivity['Technology']

    technologies = pd.concat([df3, df4]).drop_duplicates().reset_index(drop=True)
    techno = pd.Series(technologies[~technologies.str.startswith('SO_', na=False)])
    techno = pd.DataFrame(techno, columns = ['Technology'])

    capacitytoa = pd.DataFrame(columns=['Capacitytoactivity'], index= range(0,len(techno)))
    capacitytoact = pd.concat([capacitytoa, techno], axis=1, ignore_index=True)
    capacitytoactiv = capacitytoact.assign(Capacitytoactivity = capacitytoactivity)

    fixedcost.to_csv(os.path.join(path, 'fixed_cost_tnd.csv'))
    capitalcost.to_csv(os.path.join(path, 'capitalcost.csv'))
    inputactivity.to_csv(os.path.join(path, 'inputactivity.csv'))
    outputactivity.to_csv(os.path.join(path, 'outputactivity.csv'))
    capacitytoactiv.to_csv(os.path.join(path, 'capacitytoactivity.csv'))
    fuels.to_csv(os.path.join(path, 'fuels.csv'))
    technolgies.to_csv(os.path.join(path, 'technologies.csv'))


def adjacency_matrix(path, noHV_file, HV_file, topath):
    noHV = pd.read_csv(noHV_file)
    HV = pd.read_csv(HV_file)
    neartable = pd.read_csv(path)
    # The table includes the raw data from ArcMap function
    near_adj_points: Union[Union[Series, ExtensionArray, ndarray, DataFrame, None], Any] = neartable[neartable["DISTANCE"] > 0]

    near_adj_points.loc[(near_adj_points.SENDID.isin(HV.pointid)), 'SendTech'] = 'KEEL00t00'

    #add input fuel and inputtech to central exisitng grid
    central = near_adj_points.loc[(near_adj_points.SENDID.isin(HV.pointid))]
    central_nogrid = central.loc[central.NEARID.isin(noHV.pointid)]
    for m in central_nogrid.index:
        near_adj_points.loc[near_adj_points.index == m, 'INFUEL'] = 'KEEL2'
        near_adj_points.loc[(near_adj_points.index == m , 'INTECH')] = "TRHV_" + str(int(near_adj_points.NEARID[m]))
        near_adj_points.loc[near_adj_points.index == m, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[m]))
    #add fuels to the adjacent cells
    nan_intech = near_adj_points.loc[near_adj_points.INFUEL.isnull()]
    nan_intech_nogrid = nan_intech.loc[nan_intech.NEARID.isin(noHV.pointid)]
    for l in nan_intech_nogrid.index:
        near_adj_points.loc[near_adj_points.index == l, 'INFUEL'] = "EL2_" + str(int(near_adj_points.SENDID[l]))
        near_adj_points.loc[(near_adj_points.index == l , 'INTECH')] = "TRHV_" + str(int(near_adj_points.NEARID[l]))
        near_adj_points.loc[near_adj_points.index == l, 'OUTFUEL'] = "EL2_" + str(int(near_adj_points.NEARID[l]))

    for i in noHV.pointid:
        near_adj_points.loc[near_adj_points.SENDID == i, 'SendTech'] = "TRHV_"+ str(int(i))
        near_adj_points.loc[near_adj_points.NEARID == i, 'ReceiveTech'] = "TRHV_" + str(int(i))

    #Allow for connections over cells with no population ("nan")
    nan = near_adj_points.loc[near_adj_points.SendTech.isnull()]
    for j in nan.SENDID:
        near_adj_points.loc[near_adj_points.SENDID == j, 'SendTech'] = "TRHV_" + str(int(j))

    #Allow for connections over cells with no population ("nan")
    nannear = near_adj_points.loc[near_adj_points.NEARID.isin(noHV.pointid)]
    nannear_recieve = nannear.loc[nannear.ReceiveTech.isnull()]

    for k in nannear_recieve.NEARID:
        near_adj_points.loc[near_adj_points.NEARID == k, 'ReceiveTech'] = "TRHV_" + str(int(k))

    nan_matrix = near_adj_points.loc[near_adj_points.ReceiveTech.notnull()]

    final_matrix = nan_matrix.drop(['OBJECTID *','INPUT_FID','NEAR_FID','DISTANCE','NEARID','SENDID'], axis=1)
    final_matrix = final_matrix.drop_duplicates()

    final_matrix.to_csv(os.path.join(topath,'adjacencymatrix.csv'))
    return(os.path.join(topath,'adjacencymatrix.csv'))

def near_dist(pop_shp, un_elec_cells, path):

    unelec = pd.read_csv(un_elec_cells, usecols= ["pointid"])
    point = gpd.read_file(os.path.join(path, pop_shp))
    point.index = point['pointid']
    unelec_shp = gpd.GeoDataFrame(crs=32737)
    for i in unelec['pointid']:
        unelec_point = point.loc[i]
        unelec_shp = unelec_shp.append(unelec_point)

    lines = gpd.read_file(os.path.join(path, 'Concat_Transmission_lines_UMT37S.shp'))

    unelec_shp['HV_dist'] = unelec_shp.geometry.apply(lambda x: lines.distance(x).min())
    unelec_shp.set_crs(32737)
    outpath = "run/Demand/transmission.shp"
    unelec_shp.to_file(outpath)

    return(outpath)

def noHV_polygons(polygons, noHV, outpath):
    unelec = pd.read_csv(noHV, usecols= ["pointid"])
    point = gpd.read_file(polygons)
    point.index = point['pointid']
    unelec_shp = gpd.GeoDataFrame(crs=32737)
    for i in unelec['pointid']:
        unelec_point = point.loc[i]
        unelec_shp = unelec_shp.append(unelec_point)

    #unelec_shp.set_crs(32737)
    #outpath = "run/Demand/un_elec_polygons.shp"
    unelec_shp.to_file(outpath)

if __name__ == "__main__":
    #path = sys.argv[1]
    renewable_path = 'temp'
    pop_shp = 'new_40x40points_WGSUMT37S.shp'
    unelec = 'run/un_elec.csv'
    noHV = 'run/noHV_cells.csv'
    HV = 'run/HV_cells.csv'
    elec = 'run/elec.csv'
    Projected_files_path = '../Projected_files/'
    distribution_network = 'run/Demand/distributionlines.csv'
    distribution_length_cell = 'run/Demand/Distribution_network.xlsx'

    capital_cost_HV = 2.5  # kUSD MW-km
    substation = 1.5  # kUSD/MW
    capital_cost_LV = 4  # kUSD/MW
    capital_cost_LV_strengthening = 1  # kUSD/MW Assumed 25% of the cost
    capacitytoactivity = 31.536  # coversion MW to TJ
    distribution_cost = '0'
    path = 'run/ref/'
    neartable = 'run/Demand/Near_table.csv'

    # Solar and wind csv files
    #renewableninja(renewable_path, path)
    # Location file which sets the spatial resolution
    GIS_file(path)

    matrix = adjacency_matrix(neartable, noHV, HV, path)
    capital_cost_transmission_distrib(capital_cost_LV_strengthening, distribution_network, elec, noHV, HV, unelec,
                                      capital_cost_HV, substation, capital_cost_LV,
                                      capacitytoactivity, distribution_cost, path, distribution_length_cell,matrix)
