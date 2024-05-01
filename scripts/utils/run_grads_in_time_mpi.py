''' Run a templated GrADS script for many time steps with MPI parallel processing

Usage:
    mpirun -np [num_of_procs] python run_grads_in_time_mpi.py [hourly|daily|monthly|yearly] [time_start] [time_end] ["grads_script"]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, math, pytz
from glob import glob
from datetime import datetime, timedelta
from mpi4py import MPI


# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):
    
    '''main loop'''
    
    if argv[0] == 'hourly':
        step = timedelta(hours=1)
        time1 = datetime.strptime(argv[1], '%Y%m%d%H')
        time2 = datetime.strptime(argv[2], '%Y%m%d%H')
    elif argv[0] == 'daily':
        step = timedelta(days=1)
        time1 = datetime.strptime(argv[1], '%Y%m%d')
        time2 = datetime.strptime(argv[2], '%Y%m%d')
    elif argv[0] == 'monthly':
        step = relativedelta(months=1)
        time1 = datetime.strptime(argv[1], '%Y%m')
        time2 = datetime.strptime(argv[2], '%Y%m')
    elif argv[0] == 'yearly' or argv[0] == 'annual' or argv[0] == 'annually':
        step = relativedelta(years=1)
        time1 = datetime.strptime(argv[1], '%Y')
        time2 = datetime.strptime(argv[2], '%Y')
    else:
        print(f'Usage: mpirun -np [num_of_procs] python {os.path.basename(sys.argv[0])} [hourly|daily|monthly|yearly] [time_start] [time_end] [grads_script]')
        sys.exit(1)

    gs = argv[3]

    alltimes = []
    t = time1
    while t <= time2:
        alltimes.append(t)
        t += step

    ntimes = len(alltimes)
    chunk  = math.ceil(ntimes/size)
    #print('Chunk size = %d' % chunk)

    #for rank in range(size):
    t1 = chunk*rank
    t2 = chunk*(rank+1)-1
    t2 = ntimes-1 if t2>ntimes-1 else t2

    if t1<ntimes:
        tg1 = alltimes[t1].strftime('%Hz%d%b%Y')
        tg2 = alltimes[t2].strftime('%Hz%d%b%Y')

        cmd = f'opengrads -lbc "{gs} {tg1} {tg2}"'
        print(cmd); os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

