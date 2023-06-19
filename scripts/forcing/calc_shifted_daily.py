###############################################################################
# Calculate daily sums of Stage 4 filled with NLDAS-2, time shifted by -1 hour
# Ming Pan <m3pan@ucsd.edu>
###############################################################################

import sys, os, pytz, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from glob import glob
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time
from mpi4py import MPI

## some setups
workdir   = config['base_dir'] + '/forcing/stage4/'

## main function
def main(argv):

    '''main loop'''

    # MPI setup
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    step = relativedelta(years=1)
    time1 = datetime.strptime(argv[0], '%Y')
    time2 = datetime.strptime(argv[1], '%Y')
        
    #basecmd = argv[3]
    #cmdargs = argv[4]
    cmd_tpls = argv[3:]

    alltimes = []
    t = time1
    while t <= time2:
        alltimes.append(t)
        t += step

    for t in alltimes[rank::size]:
        y = t.year
        tny = datetime(y+1, 1, 1)
        ydays = (tny-t).days
        fins = 'filled_with_nldas2/%d/st4nl2_%d????.nc' % (y, y)
        fout = 'filled_with_nldas2/daily/st4nl2_daily_%d.nc' % (y)
        ndays = len(glob(fins))
        if ndays==ydays:
            fins += ' filled_with_nldas2/%d/st4nl2_%d0101.nc' % (y+1, y+1) 
            t2 = ydays+1
        t1 = 1 if y==1979 else 2
        cmd = 'cdo -f nc4 -z zip -shifttime,-690minute -seltimestep,%d/%d -daysum -shifttime,-1hour -mergetime %s %s' % (t1, t2, fins, fout)
        print(cmd); os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])
