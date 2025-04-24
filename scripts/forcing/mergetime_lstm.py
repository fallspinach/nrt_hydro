''' Merge per-day LSTM forcing data into per-month

Usage:
    python mergetime_lstm.py [yyyymm1] [yyyymm2] [retro|nrt]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, math, pytz, yaml
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from mpi4py import MPI
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## some setups
workdir   = f'{config["base_dir"]}/forcing/lstm'

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## main function
def main(argv):
    
    '''main loop'''

    for key,value in os.environ.items():
        if 'SLURM' in key:
            print(key, '=', value)
    
    os.chdir(workdir)

    time1 = datetime.strptime(argv[0], '%Y%m')
    time2 = datetime.strptime(argv[1], '%Y%m')
    time1 = time1.replace(tzinfo=pytz.utc)
    time2 = time2.replace(tzinfo=pytz.utc)
    prodtype = argv[2]
    
    step  = relativedelta(months=1)

    alltimes = []
    t = time1
    while t <= time2:
        alltimes.append(t)
        t += step

    for t in alltimes[rank::size]:

        fsrc = f'{prodtype}/{t:%Y/lstm_forcing_%Y%m}??.nc'
        fout = f'{prodtype}/{t:%Y/lstm_forcing_%Y%m}.nc'
        if prodtype == 'nrt':
            cmd = f'cdo -O -f nc4 -z zip mergetime {fsrc} {fout}'
        else:
            cmd = f'cdo -O -f nc4 -z zip mergetime {fsrc} {fout}; /bin/rm -f {fsrc}'
        print(cmd); os.system(cmd)

    comm.Barrier()

    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

