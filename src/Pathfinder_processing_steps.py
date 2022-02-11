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
import rasterio

def mosaic(dict_raster, proj_path):
    pathfinder = []
    for key, value in dict_raster.items():
        src = rasterio.open(value)
        pathfinder.append(src)

    mosaic, out_trans = merge(pathfinder)
    out_meta = src.meta.copy()
    out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "transform": out_trans,
                     "crs": ({'init': 'EPSG:32737'})})
    with rasterio.open('%s/pathfinder.tif' % proj_path, "w", **out_meta) as dest:
        dest.write(mosaic)
    print('Pathfinder is now mosaicked to pathfinder.tif')
    return ()

def remove_grid_from_results_multiply_with_lenght(dict_pathfinder, dict_weight):
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

        sum_distribution[key] = elec_path.values.sum()

    df = pd.DataFrame.from_dict(sum_distribution, orient='index')
    df.to_csv('run/Demand/distributionlines.csv')

    return dict_pathfinder

def pathfinder_main(path,proj_path, elec_shp):
    #Only settlements with population over pop_cutoff are concidered to be part of the distribution network
    elec_shape = convert_zero_to_one(elec_shp)
    #The elec_raster will serve as the points to connect and the roads will create the weights
    #Returns the path to elec_raster
    elec_raster = rasterize_elec(elec_shape, path)

    #Concatinate the highway with high- medium and low voltage lines
    grid_weight = merge_grid(path)

    #returns the path to highway_weights

    highway_shp, grid_shp = highway_weights(grid_weight, path)
    #highway_shp =  "../Projected_files/road_weights.shp"
    highway_raster = rasterize_road(highway_shp, path)
    #grid_shp =  "../Projected_files/grid_weights.shp"
    transmission_raster = rasterize_transmission(grid_shp, path)
    #transmission_raster = "../Projected_files/transmission.tif"
    #highway_raster = "../Projected_files/road.tif"
    weights_raster = merge_raster(transmission_raster, highway_raster)
    #weights_raster = "../Projected_files/weights.tif"
    #elec_raster = "../Projected_files/zero_to_one_elec.tif"

    #print("Calculating Pathfinder for all of Kenya, used to benchmark the decentralized results")
    #name = 'Kenya'

    #weight_csv = make_weight_numpyarray(weights_raster, name)
    #target_csv = make_target_numpyarray(elec_raster, name)
    #targets = np.genfromtxt(os.path.join('temp/dijkstra', "%s_target.csv" %(name)), delimiter=',')
    #weights = np.genfromtxt(os.path.join('temp/dijkstra', "%s_weight.csv" %(name)), delimiter=',')
    #origin_csv = make_origin_numpyarray(target_csv, name)
    #origin = np.genfromtxt(os.path.join('temp/dijkstra', "%s_origin.csv" %(name)), delimiter=',')

    # Run the Pathfinder alogrithm seek(origins, target, weights, path_handling='link', debug=False, film=False)

    #print("Calculating Pathfinder")
    #pathfinder = seek(origin, targets, weights, path_handling='link', debug=False, film=False)
    #elec_path = pathfinder['paths']
    #elec_path_trimmed = elec_path[1:-1,1:-1]
    #pd.DataFrame(elec_path_trimmed).to_csv("temp/dijkstra/elec_path_%s.csv" %(name))
    #print("Saving results to csv from Pathfinder")
    # print the algortihm to raster
    #raster_pathfinder = make_raster(elec_path_trimmed, name)

    files = os.listdir(proj_path)
    shapefiles = []
    for file in files:
        if file.endswith('.shp'):
            f = os.path.join('temp/', file)
            shapefiles += [f]
    #for f in shapefiles:
    #    name, end = os.path.splitext(os.path.basename(f))
    #    pathmask = masking(f, raster_pathfinder, '%s_Kenya_pathfinder.tif' %(name))

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
          e = "Not targets in square"
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
                raster_pathfinder = make_raster(elec_path_trimmed, name)
                dict_raster[name] = raster_pathfinder

        except Exception as e:
            print(e)
            continue

    print("Make raster of pathfinder")
    mosaic(dict_raster, path)
    print("Remove pathfinder where grid is passed to not double count")
    remove_grid_from_results_multiply_with_lenght(dict_pathfinder, dict_weight)


path = '../Projected_files/'
proj_path = 'temp'
elec_shp = '../Projected_files/elec.shp'

pathfinder_main(path,proj_path, elec_shp)