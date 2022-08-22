"""
Module: Pathfinder_processing_steps
===============================================

A module that runs the GIS functions for Pathfinder, removes overlapping grid from the results and then mosaic the results to a tif file
----------------------------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

from Pathfinder import *
from Pathfinder_GIS_steps import *
import numpy as np
import pandas as pd
import os
import gdal
import ogr
import csv


def mosaic(dict_raster, proj_path, cr):
    """
    This function mosaic the tiles (dict_raster) from Pathfinder to one tif file and places it in Projected_files folder
    :param dict_raster:
    :param proj_path:
    :return:
    """
    pathfinder = []
    for key, value in dict_raster.items():
        src = rasterio.open(value)
        pathfinder.append(src)

    mosaic, out_trans = merge(pathfinder)
    out_meta = src.meta.copy()
    out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "transform": out_trans,
                     "crs": ({'init': cr})})
    with rasterio.open('%s/pathfinder.tif' % proj_path, "w", **out_meta) as dest:
        dest.write(mosaic)
    print('Pathfinder is now mosaicked to pathfinder.tif')
    return ()

def remove_grid_from_results_multiply_with_lenght(dict_pathfinder, dict_weight,tofolder):
    """
    This function sets the results (shortest path network) from Pathfinder that are overlapping weights less than 0.5
    so that where the grid route is utilized this is not double counted in the final results. It then runs zonal statistics 
    using the polygon for each scenario.
    :param dict_pathfinder:
    :param dict_weight:
    :param tofolder
    :return:dict_pathfinder
    """
    sum_distribution = {}
    for key in dict_pathfinder:
        elec_path = dict_pathfinder[key]
        path_weight = dict_weight[key]
        assert elec_path.size == path_weight.size
        col_length = len(elec_path.columns)
        row_length = len(elec_path)
        i = 0
        k = 0
        j = 0
        while j < row_length:
            m = 0
            while m < col_length:
                if path_weight.iloc[(i + j), (k + m)] < 0.5:
                    elec_path.iloc[(i + j), (k + m)] = 0
                m += 1
            j += 1

        #sum_distribution[key] = elec_path.values.sum()
    
    elec_path_np = elec_path.to_numpy()
    #rasterize the cleaned numpyarray
    make_raster(elec_path_np, "cleaned", "1")

    #df = pd.DataFrame.from_dict(sum_distribution, orient='index')
    #Modified to not sum at this point
    #elec_path.to_csv(os.path.join(tofolder,'distributionlines.csv'))

def zonalstat_pathfinder(raster, polygon, scenario):
    """
    This function returns the zonalstatistic "count" from the raster to the polygon and is saved to run/scenario/{scenario}_distributionlines.csv
    :param raster: The Pathfinder raster
    :param polygon: The polygon of choise
    :param scenario: the scenario represents the number of cells in the polygon
    """
    from rasterstats import zonal_stats
    zonal_polygon = zonal_stats(polygon, raster,
            stats="sum")
    
    df_zonal = pd.DataFrame(zonal_polygon)
    df_zonal.to_csv("run/scenarios/%i_distributionslines.csv" %(scenario))


def pathfinder_main(path,proj_path, elec_shp, tofolder, tiffile, crs):
    """
    This is the function which runs all GIS functions and Pathfinder
    :param path:
    :param proj_path:
    :param elec_shp:
    :param tofolder:
    :return:
    """
    elec_shape = convert_zero_to_one(elec_shp)
    #The elec_raster will serve as the points to connect and the roads will create the weights
    #Returns the path to elec_raster
    elec_raster = rasterize_elec(elec_shape, path, tiffile)

    #Concatinate the highway with high- medium and low voltage lines
    grid_weight = merge_grid(path)

    #returns the path to highway_weights
    highway_shp, grid_shp = highway_weights(grid_weight, path, crs)
    highway_raster = rasterize_road(highway_shp, path)
    transmission_raster = rasterize_transmission(grid_shp, path)
    weights_raster = merge_raster(transmission_raster, highway_raster, crs)


    files = os.listdir(proj_path)
    shapefiles = []
    for file in files:
        if file.endswith('.shp'):
            f = os.path.join(proj_path, file)
            shapefiles += [f]

    print("Calculating Pathfinder for each cell, used for the OSeMOSYS-file")
    #This is the final version and the other is as reference for uncertainty analysis
    dict_pathfinder = {}
    dict_raster = {}
    dict_weight = {}
    for f in shapefiles:
        name, end = os.path.splitext(os.path.basename(f))
        weight_raster_cell = masking(f, weights_raster, '%s_weight.tif' %(name))
        elec_raster_cell = masking(f, elec_raster, '%s_elec.tif' % (name))

        # make csv files for Dijkstra
        weight_csv = make_weight_numpyarray(weight_raster_cell, name)
        target_csv = make_target_numpyarray(elec_raster_cell, name)
        if not os.path.exists(target_csv):
          e = "No targets in square"
        try:
            if os.path.exists(target_csv):
                targets = np.genfromtxt(os.path.join('temp/dijkstra', "%s_target.csv" % (name)), delimiter=',')
                weights = np.genfromtxt(os.path.join('temp/dijkstra', "%s_weight.csv" % (name)), delimiter=',')
                origin_csv = make_origin_numpyarray(target_csv, name)
                origin = np.genfromtxt(os.path.join('temp/dijkstra', "%s_origin.csv" % (name)), delimiter=',')
                # Run the Pathfinder alogrithm seek(origins, target, weights, path_handling='link', debug=False, film=False)
                pathfinder = seek(origin, targets, weights, path_handling='link', debug=False, film=False)
                elec_path = pathfinder['paths']
                elec_path_trimmed = elec_path[1:-1,1:-1]
                weights_trimmed= weights[1:-1,1:-1]
                electrifiedpath = pd.DataFrame(elec_path_trimmed)
                weight_pandas = pd.DataFrame(weights_trimmed)
                electrifiedpath.to_csv("temp/dijkstra/elec_path_%s.csv" % (name))
                dict_pathfinder[name] = electrifiedpath
                dict_weight[name] = weight_pandas
                raster_pathfinder = make_raster(elec_path_trimmed, name, name)
                dict_raster[name] = raster_pathfinder

        except Exception as e:
            print(e)
            continue

    print("Make raster of pathfinder")
    mosaic(dict_raster, path, crs)
    print("Remove pathfinder where grid is passed to not double count")
    remove_grid_from_results_multiply_with_lenght(dict_pathfinder, dict_weight, tofolder)

