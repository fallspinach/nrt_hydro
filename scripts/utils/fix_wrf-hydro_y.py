import sys
import numpy as np
import netCDF4 as nc

def fix_y(argv):

    f = nc.Dataset(argv[0], 'a')
    f.variables['y'][:] = -np.flip(f.variables['y'][:])
    f.sync()
    f.close()

    return 0

if __name__ == '__main__':
    fix_y(sys.argv[1:])

