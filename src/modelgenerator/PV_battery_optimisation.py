"""
Module: PV_battery_optimization
=============================

A module for optimising the PV and battery for each location. Returns the size of PV and hours of battery

---------------------------------------------------------------------------------------------------------------------------------------------

Module author: Nandi Moksnes <nandi@kth.se>
"""
import pulp as pl
from pulp import *
import pandas as pd
from datetime import timedelta
from matplotlib import pyplot as plt

def optimize_battery_pv(pv_power, location, load_profile, efficiency_discharge,  efficiency_charge, pv_cost, battery_cost, scenario):
    """
    This function optmize the PV+battery system based on the location specific capacity factor and load profile.
    The function returns the PV adjustment needed plus the required battery hours to meet the whole demand.
    The original script is from the answer of AirSquid on March 16, 2022:
    https://stackoverflow.com/questions/71494297/python-pulp-linear-optimisation-for-off-grid-pv-and-battery-system 
    Released under license CC BY-SA 4.0 https://creativecommons.org/licenses/by-sa/4.0/
    The charging variable did not work properly (charging when no excess energy exsited) therefore a binary variable controling 
    that only charging or discharging can be true at one time slice was introduced.
    In addition losses are accounted for discharging and charging which was not part of the code.
    This code does use CPLEX CMD as this is a MILP problem and solving time with PuLP was too long.
    """

    load = load_profile['Load']

    T = len(load)
    # Decision variables
    Bmax = LpVariable('Bmax', 0, None) # battery max energy (kWh)
    PV_size = LpVariable('PV_size', 0, None) # PV size

    # Optimisation problem
    prb = LpProblem('Battery_Operation', LpMinimize)

    # Auxilliary variables
    PV_gen = [LpVariable('PVgen_{}'.format(i), 0, None) for i in range(T)]

    # Load difference
    Pflow = [LpVariable('Pflow_{}'.format(i), None, None) for i in range(T)]
    # Excess PV
    Pcharge = [LpVariable('Pcharge_{}'.format(i), lowBound=0, upBound=None) for i in range(T)]
    # Discharge required
    Pdischarge = [LpVariable('Pdischarge_{}'.format(i), lowBound=None, upBound=0) for i in range(T)]
    # Charge delivered
    Pcharge_a = [LpVariable('Pcharge_a{}'.format(i), 0, None) for i in range(T)]
    bin_dich = LpVariable.dicts('BinaryDischarge', indexs=range(T), cat='Binary')
    M =100

    ###  Moved this down as it needs to include Pdischarge
    # Objective function
    # cost + some small penalty for cumulative discharge, just to shape behavior 
    prb += (PV_size*pv_cost) + (Bmax*battery_cost) #- 0.01 * lpSum(Pdischarge[t] for t in range(T))

    # Battery
    Bstate = [LpVariable('E_{}'.format(i), 0, None) for i in range(T)]

    # Battery Constraints
    for t in range(1, T):
        prb += Bstate[t] == Bstate[t-1] + Pdischarge[t]*efficiency_discharge + Pcharge_a[t]*efficiency_charge 

    # Power flow Constraints
    for t in range(0, T):
        
        # PV generation
        prb += PV_gen[t] == PV_size*pv_power[location][t]
        
        # Pflow is the energy flow reuired *from the battery* to meet the load
        # Negative if load greater than PV, positive if PV greater than load
        prb += Pflow[t] == PV_gen[t] - load[t]
        
        # charging should be more than zero if Pflow is larger than zero
        prb += Pcharge[t]  >= 0
        prb += Pcharge[t] >= Pflow[t]

        prb += Pcharge[t] - M*(1-bin_dich[t])<=0

        # If Pflow is negative (discharge), then it will at least Pflow discharge required load
        # If Pflow is positive (charge), then Pdischarge (discharge rePflowuired will ePflowual 0)
        prb += Pdischarge[t] <= 0
        prb += Pdischarge[t] <= Pflow[t]

        prb += Pdischarge[t] + M*bin_dich[t]>=0
        prb += Pflow[t]<= M*(1-bin_dich[t])

        # Discharge cannot exceed available charge in battery
        # Discharge is negative
        prb += Pdischarge[t] >= (-1)*Bstate[t-1]
        
        # Ensures that energy flow rePflowuired is satisifed by charge and discharge flows
        prb += Pflow[t] >= Pcharge[t] + Pdischarge[t]
        
        # Limit amount charge delivered by the available space in the battery
        prb += Pcharge_a[t]  >= 0
        prb += Pcharge_a[t]  <= Pcharge[t]
        prb += Pcharge_a[t]  <= Bmax - Bstate[t-1]
        
        prb += Bstate[t] >= 0
        prb += Bstate[t] <= Bmax

    # Solve problem
    solver = pl.CPLEX_CMD(msg=0)
    prb.setSolver(solver)
    prb.solve()

    # write to a csv file
    output_filename = 'input_data/results_PuLP_%s_%s.csv'%(location, scenario)

    # use a context manager to open/close the file...
    with open(output_filename, 'w') as fout:
        for v in prb.variables():
            line = ','.join([v.name, str(v.varValue)])   # make a string out of the elements, separated by commas
            #line2 = ','.join([model.name, str(model.objective)])
            fout.write(line)    # write the line to the file
            #fout.write(line2) 
            fout.write('\n')    # add a newline character

    return PV_size.varValue , Bmax.varValue
