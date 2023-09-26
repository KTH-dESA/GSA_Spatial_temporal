## Global Sensitivity Analysis - Spatial and temporal analysis for energy access

This repository is for the paper

**Global sensitivity analysis spatial and temporal resolution in energy systems optimization model**

Nandi Moksnes (1) *, William Usher (1)
1)	KTH Royal Institute of Technology

To be able to run the model you need to have approx 256 GB RAM. This model has been run on a High performance cluster at KTH.
The shell file is therefor applicable for the HPC at PDC Dardel from NAISS (National Academic Infrastructure for Supercomputing in Sweden).

The workflow is only tested on a Windows computer and Linux for the modelsruns (Snakemake runs file), therefore there might be small adjustements needed for other OS.

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
TBD
