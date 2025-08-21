''' Merge WRF-Hydro per-dayoutput files into per-month and aggregate to daily/monthly

Usage:
    mpirun -np [# of procs] python merge_aggregate.py [domain] [yyyymm1] [yyyymm2] [retro/nrt]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, calendar
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

modelid = 'nwm_v3'

## main function
def main(argv):

    '''main loop'''

    domain = argv[0]
    t1 = datetime.strptime(argv[1], '%Y%m')
    t2 = datetime.strptime(argv[2], '%Y%m')
    period = argv[3]
    step  = timedelta(days=1)

    if period=='retro':
        flag_deldaily  = True
        flag_delhourly = True
    else:
        flag_deldaily  = False
        flag_delhourly = False

    workdir   = f'{config["base_dir"]}/{modelid}/{domain}/{period}/output/1km_daily'
    os.chdir(workdir)

    alltimes = []
    t = t1
    while t <= t2:
        alltimes.append(t)
        t += relativedelta(months=1)

    for m in alltimes[rank::size]:

        fout = f'{m:%Y/%Y%m}.LDASOUT_DOMAIN1.daily'
        fin  = []
        for d in range(calendar.monthrange(m.year, m.month)[1]):
            dd = m+timedelta(days=d)
            fn = f'{dd:%Y/%Y%m%d}.LDASOUT_DOMAIN1.daily'
            if os.path.isfile(fn):
                fin.append(fn)

        cmd = f'cdo -O -f nc4 -z zip -mergetime {" ".join(fin)} {fout}'
        print(cmd); os.system(cmd)
        cmd = f'ncks -4 -L 5 {fout} {fout}.nc4; /bin/mv {fout}.nc4 {fout}'
        print(cmd); os.system(cmd)
        if flag_deldaily:
            os.system(f'rm -f {" ".join(fin)}')
        #add_pctl_rank_daily.main([fout])
        
        fmout = f'../1km_monthly/{m:%Y/%Y%m}.LDASOUT_DOMAIN1.monthly'
        cmd = f'cdo -O -f nc4 -z zip monmean {fout} {fmout}'
        print(cmd); os.system(cmd)
        cmd = f'ncks -4 -L 5 {fmout} {fmout}.nc4; /bin/mv {fmout}.nc4 {fmout}'
        print(cmd); os.system(cmd)
        #add_pctl_rank_monthly.main([fmout])

        outtypes = ['CHRT']
        if config[modelid][domain]['lake']:
            outtypes = ['CHRT', 'LAKE']

        for rout in outtypes:

            fout = f'../1km_hourly/{m:%Y/%Y%m}.{rout}OUT_DOMAIN1'
            fin  = []
            for d in range(calendar.monthrange(m.year, m.month)[1]):
                dd = m+timedelta(days=d)
                fn = f'../1km_hourly/{dd:%Y/%Y%m%d}.{rout}OUT_DOMAIN1'
                if os.path.isfile(fn):
                    fin.append(fn)

            cmd = f'cdo -O -f nc4 -z zip -mergetime {" ".join(fin)} {fout}'
            print(cmd); os.system(cmd)
            cmd = f'ncks -4 -L 5 {fout} {fout}.nc4; /bin/mv {fout}.nc4 {fout}'
            print(cmd); os.system(cmd)
            if flag_delhourly:
                os.system(f'rm -f {" ".join(fin)}')
            #add_pctl_rank_daily.main([fout])
        
            fdout = f'{m:%Y/%Y%m}.{rout}OUT_DOMAIN1.daily'
            cmd = f'cdo -O -f nc4 -z zip daymean {fout} {fdout}'
            print(cmd); os.system(cmd)
            cmd = f'ncks -4 -L 5 {fdout} {fdout}.nc4; /bin/mv {fdout}.nc4 {fdout}'
            print(cmd); os.system(cmd)
            
            fmout = f'../1km_monthly/{m:%Y/%Y%m}.{rout}OUT_DOMAIN1.monthly'
            cmd = f'cdo -O -f nc4 -z zip monmean {fdout} {fmout}'
            print(cmd); os.system(cmd)
            cmd = f'ncks -4 -L 5 {fmout} {fmout}.nc4; /bin/mv {fmout}.nc4 {fmout}'
            print(cmd); os.system(cmd)
            #add_pctl_rank_monthly.main([fmout])

    return 0

if __name__ == '__main__':
    main(sys.argv[1:])

        
