''' Merge WRF-Hydro per-day deterministic forecast output files into per-month and make the time dimension compliant to standard format

Usage:
    mpirun -np [# of procs] python merge_fix_time_wwrf.py [domain] [yyyymm1] [yyyymm2] [ens1] [ens2] [fcst_type]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time
import netCDF4 as nc
from glob import glob
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

from mpi4py import MPI
#import add_pctl_rank_daily, add_pctl_rank_monthly

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]
    t1 = datetime.strptime(argv[1], '%Y%m%d')
    t2 = datetime.strptime(argv[2], '%Y%m%d')
    ens1 = int(argv[3])
    ens2 = int(argv[4])
    if len(argv)>=6:
        fcst_type = argv[5]
    else:
        fcst_type = 'wwrf'
    workdir   = f'{config["base_dir"]}/wrf_hydro/{domain}/fcst/{fcst_type}/output'

    for ens in range(ens1, ens2+1)[rank::size]:

        os.chdir(f'{workdir}/{ens:02d}')

        fout = f'{t1:%Y%m%d}-{t2:%Y%m%d}.LDASOUT_DOMAIN1'
        fin  = []
        dd = t1+timedelta(days=1)
        while dd<=t2+timedelta(days=1):
            dfile = f'{dd:%Y%m%d%H}.LDASOUT_DOMAIN1'
            if os.path.isfile(dfile):
                fin.append(dfile)
            dd += timedelta(days=1)

        cmd = f'cdo -O -f nc4 -z zip shifttime,-12hour -mergetime {" ".join(fin)} {fout}'
        print(cmd); os.system(cmd)
        os.system(f'rm -f {" ".join(fin)}')
        
        for rout in ['CHRTOUT_DOMAIN1', 'LAKEOUT_DOMAIN1']:

            tofix = ['streamflow', 'q_lateral', 'velocity', 'qSfcLatRunoff', 'qBucket', 'qBtmVertRunoff',
                     'reservoir_assimilated_value', 'water_sfc_elev', 'inflow', 'outflow']

            fndst = f'{t1:%Y%m%d}-{t2:%Y%m%d}.{rout}'
            dst = nc.Dataset(fndst, 'w')
            fnsrc = (t1+timedelta(days=1)).strftime('%Y%m%d%H.')+rout
            src = nc.Dataset(fnsrc, 'r')

            # copy global attributes all at once via dictionary
            dst.setncatts(src.__dict__)
            # copy dimensions
            for name, dimension in src.dimensions.items():
                dst.createDimension(
                    name, (len(dimension) if not dimension.isunlimited() else None))
            # copy all file data except for the excluded
            for name, variable in src.variables.items():
                if name not in tofix:
                    x = dst.createVariable(name, variable.datatype, variable.dimensions, zlib=True)
                    dst[name].setncatts(src[name].__dict__)
                    dst[name][:] = src[name][:]
                else:
                    x = dst.createVariable(name, variable.datatype, ('time', 'feature_id',), zlib=True)
                    dst[name].setncatts(src[name].__dict__)
                    dst[name][0,:] = src[name][:]
            dst['time'][0] = src['time'][0] - 720
            src.close()
            os.system(f'rm -f {fnsrc}')

            dd = t1 + timedelta(days=2)
            d = 1
            while dd<=t2+timedelta(days=1):
                fnsrc = f'{dd:%Y%m%d%H}.{rout}'
                src = nc.Dataset(fnsrc, 'r')
                dst['time'][d] = src['time'][0] - 720
                for name, variable in src.variables.items():
                    if name in tofix:
                        dst[name][d,:] = src[name][:]
                src.close()
                os.system(f'rm -f {fnsrc}')
                dd += timedelta(days=1)
                d += 1

            dst.close()

    comm.Barrier()
    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        
