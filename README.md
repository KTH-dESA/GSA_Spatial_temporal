## Global Sensitivity Analysis - Spatial and temporal analysis for energy access

This repository is for the paper

**Global sensitivity analysis spatial and temporal resolution in energy systems optimization model**

Nandi Moksnes (1) *, William Usher (1)
1)	KTH Royal Institute of Technology

To be able to run the model you need to have approx 256 GB RAM. 
The workflow is only tested on a Windows computer, therefore, there might be small adjustements needed for other OS.

### Modelled input parameters

![image](https://github.com/KTH-dESA/GSA_Spatial_temporal/assets/30128518/ecde38dd-310b-4c27-9a21-9ea791614a31)


# Python dependencies
The workflow has a number packages that needs to be installed.

The easiest way to install the Python packages is to use miniconda.

Obtain the miniconda package (https://docs.conda.io/en/latest/miniconda.html):
1) Add the conda-forge channel: **conda config --add channels conda-forge**
2) Create a new Python environment: **conda env create -f environment.yml**
3) Activate the new environment: **conda activate GSA**

# R
To download the capacityfactors for solar and wind you need to have R on your computer.
You can download R for free https://www.r-project.org/
You also need to install the package "curl" which you install through the R commander
<pre><code>install.packages("curl")</code></pre>

# Required accounts (free to register)
To run the code you need to create accounts in the following places:
- https://www.renewables.ninja/ and get the token to download several files per hour
- https://payneinstitute.mines.edu/eog/nighttime-lights/ and the password is entered in the first cell in the notebook

# Run the model
Run first the src/build_initial_countrydata.py and make sure that the base files look as expected.
Then run the src/scenario_builder.py to build all the scenarios.
