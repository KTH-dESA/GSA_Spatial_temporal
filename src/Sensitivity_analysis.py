import sys
from SALib.analyze import morris
from SALib.sample.morris import sample
from SALib.util import read_param_file
from SALib.plotting.morris import horizontal_bar_plot, covariance_plot, \
    sample_histograms
import matplotlib.pyplot as plt
import numpy as np 
import os

sys.path.append('../..')

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Read the parameter range file and generate samples
#problem = read_param_file('../../src/SALib/test_functions/params/Sobol_G.txt')
# or define manually without a parameter file:
problem = {
 'num_vars': 3,
 'names': ['space', 'demand', 'discountrate'],
 'groups': None,
  'bounds': [[1, 13],
             [1, 2],
             [1, 2]]
 }
# Files with a 4th column for "group name" will be detected automatically, e.g.
# param_file = '../../src/SALib/test_functions/params/Ishigami_groups.txt'

# Generate samples
param_values = sample(problem, N=100,num_levels=4,
                      optimal_trajectories=10, seed = 1)

modified_values = param_values.copy()

# We then `floor` the values for the categorical inputs
# e.g., everything < 1 will be mapped to 0, between 1.0 and 2.0 as 1, etc
# modified_values[:, 0:3] = np.floor(modified_values[:, 0:3])
# np.savetxt("sensitivity/sample_morris.csv", modified_values, delimiter=",")

# To use optimized trajectories (brute force method),
# give an integer value for optimal_trajectories

# Run the "model" -- this will happen offline for external models
# Modified values that are floored
cap_results = np.loadtxt("sensitivity/morris_results_capitalinv.csv", dtype=float)
param_values_ = np.loadtxt("sensitivity/sample_morris.csv", delimiter=",")
#param_values_float = param_values_.astype(np.float)
Y = cap_results

# Perform the sensitivity analysis using the model output
# Specify which column of the output file to analyze (zero-indexed)
Si = morris.analyze(problem, param_values_, Y, conf_level=0.95, print_to_console=True, num_levels=4, num_resamples=100)
# Returns a dictionary with keys 'mu', 'mu_star', 'sigma', and 'mu_star_conf'
# e.g. Si['mu_star'] contains the mu* value for each parameter, in the
# same order as the parameter file

fig, (ax1, ax2) = plt.subplots(1, 2)
horizontal_bar_plot(ax1, Si, {}, sortby='mu_star', unit=r"kUSD")
covariance_plot(ax2, Si, {}, unit=r"kUSD")

fig2 = plt.figure()
sample_histograms(fig2, param_values_, problem, {'color': 'y'})
plt.show()
