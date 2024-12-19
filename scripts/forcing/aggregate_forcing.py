''' Aggregate hourly forcing to daily and monthly

Usage:
    mpirun -np [# of procs] python aggregate_forcing.py [domain] [yyyymm1] [yyyymm2] [retro|nrt]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import sys, os, pytz, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from mpi4py import MPI
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]

    t1 = datetime.strptime(argv[1], '%Y%m')
    t2 = datetime.strptime(argv[2], '%Y%m')
    ptype = argv[3]
    
    workdir = f'{config["base_dir"]}/wrf_hydro/{domain}/{ptype}/forcing'
    os.chdir(workdir)

    step  = relativedelta(months=1)
    alltimes = []
    t = t1
    while t <= t2:
        alltimes.append(t)
        t += step
    
    for t in alltimes[rank::size]:
        md = monthrange(t.year, t.month)[1]
        tn = t + step
        cmd = f'cdo -O -f nc4 -z zip delete,timestep=1,{md+2} -daymean -shifttime,-1hour [ -mergetime 1km_hourly/{t:%Y/%Y%m}??.LDASIN_DOMAIN1 1km_hourly/{tn:%Y/%Y%m}01.LDASIN_DOMAIN1 ] 1km_daily/{t:%Y%m}.LDASIN_DOMAIN1.daily'
        print(cmd); os.system(cmd)
        cmd = f'cdo -O -f nc4 -z zip monmean 1km_daily/{t:%Y%m}.LDASIN_DOMAIN1.daily 1km_monthly/{t:%Y%m}.LDASIN_DOMAIN1.monthly'
        print(cmd); os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
