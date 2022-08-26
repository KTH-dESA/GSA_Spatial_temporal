"""
Module: Build_csv_files
=============================

A module for building the csv-files for GEOSeMOSYS https://github.com/KTH-dESA/GEOSeMOSYS to run that code
In this module the logic around electrified and un-electrified cells are implemented for the 378 cells

---------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>

"""
import pandas as pd
import geopandas as gpd
import os
import fnmatch

pd.options.mode.chained_assignment = None


def renewableninja(path, dest, spatial):
    """
    This function organize the data to the required format of a matrix with the
    location name on the x axis and hourly data on the y axis so that it can be fed into https://github.com/KTH-dESA/GEOSeMOSYS code
    the data is saved as capacityfactor_wind.csv and capacityfactor_solar.csv
    :param path:
    :param dest:
    :return:
    """
    files = os.listdir(path)
    outwind = []
    outsolar = []
    for file in files:

        if fnmatch.fnmatch(file, 'timezoneoffsetout_w*'):

            file = os.path.join(path,file)
            wind = pd.read_csv(file, index_col='adjtime')
            outwind.append(wind)
    for file in files:

        if fnmatch.fnmatch(file, 'timezoneoffsetout_s*'):

            file = os.path.join(path,file)
            solar = pd.read_csv(file, index_col='adjtime')
            outsolar.append(solar)
    try:
        solarbase = pd.concat(outsolar, axis=1)
        windbase = pd.concat(outwind, axis=1)
    except:
        print('Only one solar/wind file')
        solarbase = outsolar[0]
        windbase = outwind[0]
    header = solarbase.columns
    new_header = [x.replace('X','') for x in header]
    solarbase.columns = new_header

    #solarbase.drop('Unnamed: 0', axis='columns', inplace=True)
    solarbase.to_csv(os.path.join(dest, '%i_capacityfactor_solar.csv' %(spatial)))

    header = windbase.columns
    new_header = [x.replace('X','') for x in header]
    windbase.columns = new_header
    #windbase.drop('Unnamed: 0', axis='columns', inplace=True)
    windbase.to_csv(os.path.join(dest, '%i_capacityfactor_wind.csv' %(spatial)))
    return()

def GIS_file(dest, point, spatial):
    """
    Creates the GIS location file which determins the spatial resolution
    :param dest:
    :return:
    """
    point = gpd.read_file(point)
    GIS_data = point.index
    grid = pd.DataFrame(GIS_data, copy=True)
    grid.columns = ['Location']
    grid.to_csv(os.path.join(dest, '%i_GIS_data.csv' %(spatial)), index=False)
    return(os.path.join(dest, '%i_GIS_data.csv' %(spatial)))

## Build files with elec/unelec aspects
def capital_cost_transmission_distrib(elec, noHV_file, HV_file, elec_noHV_cells_file, unelec, capital_cost_HV, substation, capacitytoactivity, path, adjacencymatrix,gis_file, scenario, diesel):
    """Reads the transmission lines shape file, creates empty files for inputactivity, outputactivity, capitalcost for transmission lines.

    :param distribution_network: a csv file with number of cells included in Pathfinder
    :param elec: are the 40*40m cells that have at least one cell of electrified 1x1km inside it
    :param unelec: are the 40*40m cells that have NO electrified cells 1x1km inside it
    :param noHV: are the cells that are electrified and not 5000 m from a minigrid and not 50,000 m from the exsiting HV-MV grid the cell are concidered electrified by transmission lines.
    :param transmission_near: Is the distance to closest HV line from the center of the 40*40 cell
    :param capital_cost_HV: kUSD/MW
    :param substation: kUSD/MW
    :param capital_cost_LV: kUSD/MW
    :param adajencymatrix is the model where the adjacent cell is located
    :return:
    """

    #gdf = gpd.read_file(transmission_near)
    #transm = pd.DataFrame(gdf)
    #transm.index = transm['index_right']

    capitalcost = pd.DataFrame(columns=['Technology', 'Capitalcost'], index=range(0,5000)) # dtype = {'Technology':'object', 'Capitalcost':'float64'}

    fixedcost = pd.DataFrame(columns=['Technology', 'Fixed Cost'], index=range(0,5000)) # dtype = {'Technology':'object', 'Capitalcost':'float64'}

    inputactivity = pd.DataFrame(columns=['Column','Fuel','Technology','Inputactivity','ModeofOperation'], index=range(0,10000))

    outputactivity = pd.DataFrame(columns=['Column','Fuel',	'Technology','Outputactivity','ModeofOperation'], index=range(0,10000))

    elec = pd.read_csv(elec)
    gis = pd.read_csv(gis_file)
    try:
        matrix = pd.read_csv(adjacencymatrix)
    except:
        print("No adjacencymatrix for this scenario")
        pass
    elec.index_right = elec.index_right.astype(int)
    un_elec = pd.read_csv(unelec)
    un_elec.index_right = un_elec.index_right.astype(int)
    noHV = pd.read_csv(noHV_file)
    HV = pd.read_csv(HV_file)
    elec_noHV_cells = pd.read_csv(elec_noHV_cells_file)
    noHV.index_right = noHV.index_right.astype(int)

    m = 0
    input_temp = []
    output_temp = []
    capital_temp = []

    ## Electrified by HV in baseyear
    for k in HV['index_right']:

        input_temp = [0, "KEEL2", "TRLV_%i_0" %(k), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        #The TRLVM is introduced as one technology cannot input two fuels in the same timeslice
        #This is for minigrid supply
        input_temp = [0, "EL2_%i" %(k), "TRLVM_%i_0" %(k), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        input_temp = [0, "KEEL2","KEEL00d_%i" %(k), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (k), "KEEL00d_%i" % (k), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (k), "TRLV_%i_0" % (k), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        #The TRLVM is introduced as one technology cannot input two fuels in the same timeslice
        #This is for minigrid supply
        output_temp = [0, "EL3_%i_0" % (k), "TRLVM_%i_0" % (k), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (k), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (k), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (k),  "SOPV8r_%i_1" % (k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (k),  "SOPV8r_%i_0" % (k), 1,1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (k),  "SOPV_%i_1" % (k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (k),  "SOPV_%i_0" % (k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

    # Electrified by minigrid in base year
    for m in elec_noHV_cells['index_right']:
        input_temp = [0, "EL2_%i" %(m), "TRLV_%i_1" %(m), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (m), "TRLV_%i_1" % (m), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        input_temp = [0,"EL2_%i" %(m),"TRLV_%i_0" %(m), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (m), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (m), "TRLV_%i_0" % (m), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (m), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (m),  "SOPV8r_%i_1" % (m), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (m),  "SOPV8r_%i_0" % (m), 1,1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_1" % (m),  "SOPV_%i_1" % (m), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (m),  "SOPV_%i_0" % (m), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

    # No electrified cells in the base year
    for j in noHV['index_right']:

        input_temp = [0, "EL2_%i" %(j),"TRLV_%i_0" %(j), 1, 1]
        inputactivity.loc[-1] = input_temp  # adding a row
        inputactivity.index = inputactivity.index + 1  # shifting index
        inputactivity = inputactivity.sort_index()

        output_temp = [0, "EL3_%i_0" % (j), "TRLV_%i_0" % (j), 0.83, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,  "EL3_%i_0" % (j), "BACKSTOP", 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,"EL3_%i_0" % (j),"SOPV8r_%i_0" % (j), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,"EL3_%i_0" % (j),"SOPV_%i_0" % (j), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

    #For all cells
    for k in range(0,len(gis)):

        output_temp = [0,  "EL2_%i" % (k+1),"SOMG8c_%i" %(k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        output_temp = [0,  "EL2_%i" % (k+1),"WI_%i" %(k), 1, 1]
        outputactivity.loc[-1] = output_temp  # adding a row
        outputactivity.index = outputactivity.index + 1  # shifting index
        outputactivity = outputactivity.sort_index()

        if diesel == True:
            output_temp = [0,  "EL2_%i" % (k+1),"DSGEN_%i" %(k), 1, 1]
            outputactivity.loc[-1] = output_temp  # adding a row
            outputactivity.index = outputactivity.index + 1  # shifting index
            outputactivity = outputactivity.sort_index()

            input_temp = [0,  "KEDS", "DSGEN_%i" %(k), 4, 1]
            inputactivity.loc[-1] = input_temp  # adding a row
            inputactivity.index = inputactivity.index + 1  # shifting index
            inputactivity = inputactivity.sort_index()
    try:
        output_matrix = matrix.drop(['INFUEL','SendTech','Unnamed: 0'], axis=1)

        matrix_out = output_matrix.drop_duplicates()

        for l in matrix_out.index:
            output_temp = [0,  matrix.loc[l]['OUTFUEL'], matrix.loc[l]['INTECH'], 1, 1]
            outputactivity.loc[-1] = output_temp  # adding a row
            outputactivity.index = outputactivity.index + 1  # shifting index
            outputactivity = outputactivity.sort_index()

        input_matrix = matrix.drop(['OUTFUEL','SendTech','Unnamed: 0'], axis=1)
        matrix_in = input_matrix.drop_duplicates()

        for l in matrix_in.index:
            input_temp = [0,  matrix.loc[l]['INFUEL'], matrix.loc[l]['INTECH'], 1, 1]
            inputactivity.loc[-1] = input_temp  # adding a row
            inputactivity.index = inputactivity.index + 1  # shifting index
            inputactivity = inputactivity.sort_index()

        tech_matrix = matrix.drop(['SendTech','INFUEL','OUTFUEL','Unnamed: 0'], axis=1)
        tech_matr = tech_matrix.drop_duplicates()
        for h in tech_matr.index:
            capitalcost.loc[m]['Capitalcost'] = tech_matr.loc[h]['DISTANCE'] /1000*capital_cost_HV + substation  #kUSD/MW divided by 1000 as it is in meters
            capitalcost.loc[m]['Technology'] =  matrix.loc[h]['INTECH']

            fixedcost.loc[m]['Fixed Cost'] = tech_matr.loc[h]['DISTANCE']/1000*capital_cost_HV*0.025 + substation*0.025  #kUSD/MW divided by 1000 as it is in meters
            fixedcost.loc[m]['Technology'] =  matrix.loc[h]['INTECH']
            m = m+1
    except:
        pass

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

    inputactivity.dropna(subset=['Technology'], inplace=True)
    capacitytoactiv.dropna(subset=[1], inplace=True)
    outputactivity.dropna(subset=['Technology'], inplace=True)
    capitalcost.dropna(subset=['Technology'], inplace=True)

    fixedcost.to_csv(os.path.join(path, '%i_fixed_cost_tnd.csv' %(scenario)))
    capitalcost.to_csv(os.path.join(path, '%i_capitalcost.csv' %(scenario)))
    inputactivity.to_csv(os.path.join(path, '%i_inputactivity.csv' %(scenario)))
    outputactivity.to_csv(os.path.join(path, '%i_outputactivity.csv'%(scenario)))
    capacitytoactiv.to_csv(os.path.join(path, '%i_capacitytoactivity.csv'%(scenario)))
    fuels.to_csv(os.path.join(path, '%i_fuels.csv'%(scenario)))
    technolgies.to_csv(os.path.join(path, '%i_technologies.csv'%(scenario)))

def near_dist(pop_shp, un_elec_cells, path, CR):

    unelec = pd.read_csv(un_elec_cells, usecols= ["index_right"])
    point = gpd.read_file(os.path.join(path, pop_shp))
    point.index = point['index_right']
    unelec_shp = gpd.GeoDataFrame(crs=32737)
    for i in unelec['index_right']:
        unelec_point = point.loc[i]
        unelec_shp = unelec_shp.append(unelec_point)

    lines = gpd.read_file(os.path.join(path, 'Concat_Transmission_lines_UMT37S.shp'))

    unelec_shp['HV_dist'] = unelec_shp.geometry.apply(lambda x: lines.distance(x).min())
    unelec_shp.set_crs(CR)
    outpath = "run/Demand/transmission.shp"
    unelec_shp.to_file(outpath)

    return(outpath)

def noHV_polygons(polygons, noHV, outpath, CR):
    unelec = pd.read_csv(noHV, usecols= ["index_right"])
    point = gpd.read_file(polygons)
    #point.index = point['FID']
    unelec_shp = gpd.GeoDataFrame(geometry=[], crs=CR)
    for i in unelec['index_right']:
        unelec_point = point.loc[i]
        unelec_shp = unelec_shp.append(unelec_point)
    unelec_shp.to_file(outpath)
