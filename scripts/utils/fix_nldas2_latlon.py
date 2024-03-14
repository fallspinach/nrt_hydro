import sys
import numpy as np
import netCDF4 as nc

def fix_latlon(argv):

    f = nc.Dataset(argv[0], 'a')
    f.variables['lon'][:] = np.linspace(-124.9375, -67.0625, 464)
    f.variables['lat'][:] = np.linspace(25.0625, 52.9375, 224)
    f.sync()
    f.close()

    return 0

if __name__ == '__main__':
    fix_latlon(sys.argv[1:])

