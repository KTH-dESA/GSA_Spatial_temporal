#https://github.com/renewables-ninja/ninja_automator/tree/master/R From GitHub renewables-ninja modified for this project by Nandi Moksnes 2021-04
###########################
####  ###
####  ###
##   ##      RENEWABLES.NINJA
#####       WEBSITE AUTOMATOR
##
#
#  simple instructions:
#    change any file paths in this script from './path/to/' to the directory where you saved the R and CSV files
#    change the token string to match that from your user account
#    run through the five simple examples below
#

args = commandArgs(trailingOnly=TRUE)

print(args)

if (length(args)<2){
  stop("At least one argument must be supplied (input file).n", call.=FALSE)

} else if (length(args)==5){

#####
## ##  MODEL SETUP
#####
  

  setwd(args[1])
  getwd()

	# pre-requisites
	library(curl)
	source('ninja_automator.r')

	# insert your API authorisation token here
	token = args[2]

	# establish your authorisation
	h = new_handle()
	handle_setheaders(h, 'Authorization'=paste('Token ', token))
	
	
#####
## ##  DOWNLOAD RENEWABLE TIME SERIES DATA FOR MULTIPLE LOCATIONS
## ##  USING CSV FILES FOR DATA INPUT AND OUTPUT
#####	

	# EXAMPLE 4 :::: read a set of wind farms from CSV - save their outputs to CSV
	#                this is the same as example 3 - the UK capital cities
	#    your csv must have a strict structure: one row per farm, colums = lat, lon, from, to, dataset, capacity, height, turbine - and optionally name (all lowercase!)

	if (args[3] == "wind") {



	farms = read.csv(file = args[4], stringsAsFactors=FALSE)

	z = ninja_aggregate_wind(farms$lat, farms$lon, farms$from[1], farms$to[1], farms$dataset, farms$capacity, farms$height, farms$turbine, farms$name)

	write.csv(z, args[5], row.names=TRUE)
}


	# EXAMPLE 5 :::: read a set of solar farms from CSV - save their outputs to CSV
	#                this is the ten largest US cities - and uses the 'name' column to identify our farms
	#    your csv must have a strict structure: one row per farm, colums = lat, lon, from, to, dataset, capacity, system_loss, tracking, tilt, azim - and optionally name (all lowercase!)

	if (args[3] == 'solar') {
	  
	farms = read.csv(args[4], stringsAsFactors=FALSE)

	z = ninja_aggregate_solar(farms$lat, farms$lon, farms$from[1], farms$to[1], farms$dataset, farms$capacity, farms$system_loss, farms$tracking, farms$tilt, farms$azim, name=farms$name)

	write.csv(z, args[5], row.names=TRUE)

	# how productive are these places
	colMeans(z[ , -1]) / farms$capacity
}

	# now you know the way of the ninja
	# use your power wisely
	# fight bravely
}
	