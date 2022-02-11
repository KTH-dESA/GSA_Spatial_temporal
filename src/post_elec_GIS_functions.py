"""
Module: post_elec_GIS_functions
===========================================================================

A module for joining the larger polygons with the electrification algorithm to be able to calculate the demand in Excel

----------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""
import geopandas as gpd
from geopandas.tools import sjoin
import pandas as pd
import numpy as np
import rasterio
from rasterio.merge import merge
from osgeo import gdal, ogr, gdalconst
import os

def join(elec, tif, cells):
    """

    :param elec:
    :param tif:
    :param cells:
    :return:
    """
    settlements = gpd.read_file(elec)
    print(settlements.crs)
    settlements.index = range(len(settlements))
    coords = [(x, y) for x, y in zip(settlements.geometry.x, settlements.geometry.y)]

    _, filename = os.path.split(tif)
    name, ending = os.path.splitext(filename)
    gdp = rasterio.open(tif)
    print(gdp.crs)
    settlements['GDP_PPP'] = [x[0] for x in gdp.sample(coords)]
    print(name)

    cell =  gpd.read_file(cells)
    demand_cells = sjoin(settlements, cell, how="left")
    if not os.path.exists('run/Demand'):
        os.makedirs('run/Demand')
    demand_cells.to_file(os.path.join(os.getcwd(), 'run\Demand\demand.shp'))
    demand_cell = pd.DataFrame(demand_cells, copy=True)
    demand_cell.to_csv('run/Demand/demand_cells.csv')
    path = 'run/Demand/demand_cells.csv'
    return(path)

def elec(demandcells):
    demand_cell = pd.read_csv(demandcells)

    allcells = demand_cell.groupby(["pointid"])
    HV_all = allcells.filter(lambda x: (x['elec'].mean() > 0) and ((x['Minigrid'].min() > 5000)) or ((x['MV'].min() < 1)) or ((x['LV'].min() < 1)) or ((x['Grid'].min() < 1)))
    HV = HV_all.groupby(["pointid"])
    HV_df = HV.sum().reset_index()[['pointid']]
    HV_df.to_csv(os.path.join(os.getcwd(),'run/HV_cells.csv'))

    elec_all = allcells.filter(lambda x: (x['elec'].mean() > 0))
    elec = elec_all.groupby(["pointid"])
    elec.sum().reset_index()[['pointid']].to_csv(os.path.join(os.getcwd(),'run/elec.csv'))
    elec.sum().reset_index()[['pointid']].to_csv(os.path.join(os.getcwd(),'run/ref/elec.csv'))
    elec.sum().reset_index()[['pointid']].to_csv(os.path.join(os.getcwd(), 'run/vision/elec.csv'))


    #noHV_all = allcells.filter()
    #noHV_all = allcells.filter(lambda x: (x['p'].mean() == 0 ) or (x['Minigrid'].min() < 5000) and (x['MV'].min() > 1) or (x['LV'].min() > 1))
    all_pointid = demand_cell['pointid'].drop_duplicates().dropna()
    noHV = (pd.merge(all_pointid,HV_df, indicator=True, how='outer').query('_merge=="left_only"').drop('_merge', axis=1))
    noHV.to_csv(os.path.join(os.getcwd(),'run/noHV_cells.csv'))

    minigrid = allcells.filter(lambda x: (x['elec'].mean() > 0 ) and ((x['Minigrid'].min() < 5000) ))
    minigrid_all = minigrid.groupby(["pointid"])
    minigrid_all.sum().reset_index()[['pointid']].to_csv(os.path.join(os.getcwd(),'run/minigridcells.csv'))

    unelec_all = allcells.filter(lambda x: (x['elec'].mean() == 0 ))
    unelec = unelec_all.groupby(["pointid"])
    unelec.sum().reset_index()[['pointid']].to_csv(os.path.join(os.getcwd(),'run/un_elec.csv'))
    unelec.sum().reset_index()[['pointid']].to_csv(os.path.join(os.getcwd(),'run/ref/un_elec.csv'))
    unelec.sum().reset_index()[['pointid']].to_csv(os.path.join(os.getcwd(), 'run/vision/un_elec.csv'))

#This function is not used in the current version of the code
# def calculate_demand(settlements):
#     demand_cell = pd.read_csv(settlements, index_col=[0])
#     demand_cell["pointid"] = demand_cell["pointid"].astype("category")
#
#     totals_elec = pd.pivot_table(demand_cell, index=["elec"],
#                           aggfunc={'GDP_PPP': np.average, "Grid": np.min, "pop": np.sum, "pointid": np.sum}, margins=True,
#                           values=['GDP_PPP', "Grid", "pop", "pointid"])
#     total_gdp_elec = totals_elec['GDP_PPP'][1]
#     total_gdp_unelec = totals_elec['GDP_PPP'][0]
#     total_pop_elec = totals_elec['pop'][1]
#     total_pop_unelec = totals_elec['pop'][0]
#
#     cell = pd.pivot_table(demand_cell, index=["pointid", "elec"], aggfunc= {'GDP_PPP' : np.average, "Grid": np.min, "pop": np.sum}, margins = True, values= ['GDP_PPP', "Grid","pop"])
#
#     pd.concat(
#         [cell, cell.sum(level=[0, 1]).assign(iten_name='popsum').set_index('popsum', append=True)]).sort_index(
#         level=[0, 1, 2])
#
#
#     demand_emop = pd.read_csv('run/Demand/ref_demand.csv', index_col=0, header= 0)
#     demand_emop.columns = (demand_emop.columns).astype(int)
#     column_names = demand_emop.columns
#     index = range(len(cell.index))
#     demand = pd.DataFrame(columns=column_names, index= index)
#     demand.insert(0, 'Fuel', 0)
#
#     locations = cell.index.get_level_values(0).unique()
#     j = 0
#
#     for location in locations:
#         if location == 'All':
#             break
#         cell_id = int(location)
#         split = cell.xs(location)
#
#         for elec in split.index:
#             if elec ==1 and split['pop']!=0:
#                 demand['Fuel'][j] ='EL3_%i_1' % (cell_id)
#                 for i in demand_emop.columns:
#
#                     #This needs to be revised and also to sum over the electrified only. Also adding gdp is difficult here. Cannot be added like this at least.
#                     demand[i][j] = (0.5*split['pop'][elec] / total_pop_elec[0] + 0.5*split['GDP_PPP'][elec]/total_gdp_elec[0]) * demand_emop[i][0]
#                 j +=1
#             elif elec==0 and split['pop']!=0:
#                 demand['Fuel'][j] = 'EL3_%i_0' % (cell_id)
#                 for i in demand_emop.columns:
#
#                     #This needs to be revised to sum over un-electrified.
#                     demand[i][j] = (split['pop'][elec]/total_pop_unelec[0])*demand_emop[i][1]
#                 j +=1
#             else:
#                 break
#     demand.index = demand['Fuel']
#     demand = demand.drop("Fuel", axis=1)
#     demand = demand.drop(0, axis=0)
#     demand.to_csv('run/demand.csv')
#
#     return ()

if __name__ == "__main__":

    shape = '../Projected_files/Final_Polygons_Kenya.shp'
    gdp = '../Projected_files/masked_UMT37S_GDP_PPP_2015_WGS_84.tif'
    elec_shp = '../Projected_files/elec.shp'
    demandcells = os.path.join(os.getcwd(), 'run/Demand/demand_cells.csv')

    #join(elec_shp, gdp, shape)
    elec(demandcells)