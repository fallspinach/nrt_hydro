import netCDF4 as nc
import numpy as np
import sys

f=nc.Dataset(sys.argv[1], 'a')
f.variables['sfc'][:]=np.linspace(0.5, 99.5, 100)
f.sync()
f.close()
