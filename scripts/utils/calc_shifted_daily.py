''' Calculate daily sums of Stage 4 filled with NLDAS-2, time shifted by -1 hour

Usage:
    python update_conus_forcing_retro.py [yyyy1] [yyyy2]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Deprecated'

import sys, os, pytz, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from glob import glob
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time
from mpi4py import MPI

## some setups
workdir   = f'{config["base_dir"]}/forcing/stage4/'

## main function
def main(argv):

    '''main loop'''

    os.chdir(workdir)
    
    # MPI setup
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    step = relativedelta(years=1)
    time1 = datetime.strptime(argv[0], '%Y')
    time2 = datetime.strptime(argv[1], '%Y')
        
    alltimes = []
    t = time1
    while t <= time2:
        alltimes.append(t)
        t += step

    for t in alltimes[rank::size]:
        y = t.year
        tny = datetime(y+1, 1, 1)
        ydays = (tny-t).days
        fins = f'filled_with_nldas2/{y:d}/st4nl2_{y:d}????.nc'
        fout = f'filled_with_nldas2/daily/st4nl2_daily_{y:d}.nc'
        ndays = len(glob(fins))
        if ndays==ydays:
            fins += f' filled_with_nldas2/{y+1:d}/st4nl2_{y+1:d}0101.nc'
            t2 = ydays+1
        else:
            t2 = ndays-1
        t1 = 1 if y==1979 else 2
        cmd = f'cdo -f nc4 -z zip -shifttime,-690minute -seltimestep,{t1:d}/{t2:d} -daysum -shifttime,+11hour -mergetime {fins} {fout}'
        print(cmd); os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
