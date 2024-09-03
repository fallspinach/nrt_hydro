''' Download and process PRISM "recent" and "provisional" versions

Usage:
    python process_prism.py
Default values:
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time, subprocess
from glob import glob
import netCDF4 as nc
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## some setups
workdir  = f'{config["base_dir"]}/forcing/prism'
lockfile = 'prism.lock'

bildir = f'/scratch/{os.getenv("USER")}/{config["node_scratch"]}{os.getenv("SLURM_JOBID")}/bil'
ncdir  = bildir.replace('bil', 'nc')

httpspath = 'https://prism.oregonstate.edu/fetchData.php'

override_flag = True       # override the old output files or not

vs = ['ppt', 'tmean']

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(workdir)
    
    # simple file to avoid running multiple instances of this code
    if os.path.isfile(lockfile):
        print(f'{os.path.basename(__file__)} is exiting: another copy of the program is running.')
        #return 1
    else:
        pass
        #os.system(f'touch {lockfile}')
    
    # keep the time
    time_start = time.time()
        
    for d in [bildir, ncdir]:
        if not os.path.isdir(d):
            os.system(f'mkdir -p {d}')

    update_prisms()
    #update_historical()
    
    time_finish = time.time()
    print(f'Total download/process time {(time_finish-time_start):.1f} seconds')
    
    os.system(f'/bin/rm -f {lockfile}')

    return 0


def update_prisms():

    # get current UTC time
    curr_time  = datetime.utcnow() - timedelta(days=1) # add 1 days buffer for PRISM
    curr_month = datetime(curr_time.year, curr_time.month, 1)

    ptypes  = ['recent', 'provisional']
    prods   = ['stable', 'provisional']
    tsfmts  = ['%Y', '%Y%m']
    latests = [curr_month-relativedelta(months=7), curr_month-relativedelta(months=1)]
    steps   = [relativedelta(years=1), relativedelta(months=1)]
    
    for ptype,prod,tsfmt,latest,step in zip(ptypes, prods, tsfmts, latests, steps):        
        
        os.chdir(ptype)

        # find the last nc file
        ncfiles = glob(f'nc/PRISM_tmean_{prod}_4kmD2_*.nc')
        ncfiles.sort()
        f = nc.Dataset(ncfiles[-1], 'r')
        last_day = datetime.strptime(str(nc.num2date(f['time'][-1], f['time'].units)), '%Y-%m-%d %H:%M:%S')
        f.close()
        print(f'Last day of PRISM-{prod}: {last_day:%Y-%m-%d}, latest (assumed) available is {latest:%Y-%m-%d}')
        
        next_day = last_day + timedelta(days=1)
        while next_day<=latest:
            
            y  = next_day.strftime('%Y')
            m  = next_day.strftime('%m')

            ts = next_day.strftime(tsfmt)
    
            for v in vs:
                
                if ptype=='recent':
                    cmd = f'wget "{httpspath}?type=all_year&range=daily&kind=recent&elem={v}&temporal={y}0101" -O zip/{v}_{y}.zip'
                elif ptype=='provisional':
                    cmd = f'wget "{httpspath}?type=all_bil&range=daily&kind=6months&elem={v}&temporal={m}01&year={y}" -O zip/{v}_{ts}.zip'
                print(cmd); os.system(cmd)
    
            for v in vs:
                cmd = f'unzip -o -q -d {bildir} zip/{v}_{ts}.zip'
                print(cmd); os.system(cmd)
                fname = f'PRISM_{v}_{prod}_4kmD2_{ts}'
                bilfiles = glob(f'{bildir}/{fname}*.bil')
                bilfiles.sort()
                for f in bilfiles:
                    cmd = f'gdal_translate -of netcdf {f} {f.replace("bil", "nc")}'
                    print(cmd); os.system(cmd)
                cmd = f'ncecat -O -h -4 -L 5 -u time -v Band1 {ncdir}/{fname}* nc/{fname}.nc'
                print(cmd); os.system(cmd)
                cmd = f'ncrename -v Band1,{v} nc/{fname}.nc'
                print(cmd); os.system(cmd)
                times = ','.join(['%s' % i for i in range(len(bilfiles))])
                cmd = f'ncap2 -O -s "time[time]={{{times}}}" nc/{fname}.nc nc/{fname}.nc'
                print(cmd); os.system(cmd)
                if ptype=='recent':
                    cmd = f'ncatted -a units,time,o,c,"days since {y}-01-01" nc/{fname}.nc'
                elif ptype=='provisional':
                    cmd = f'ncatted -a units,time,o,c,"days since {y}-{m}-01" nc/{fname}.nc'
                print(cmd); os.system(cmd)
                cmd = f'rm -f {ncdir}/{fname}* {bildir}/{fname}*'
                print(cmd); os.system(cmd)

            next_day += step

        os.chdir('..')

def update_historical():

    ptypes  = ['historical']
    prods   = ['stable']
    tsfmts  = ['%Y']
    latests = [datetime(1980, 1, 1)]
    steps   = [relativedelta(years=1)]
    
    for ptype,prod,tsfmt,latest,step in zip(ptypes, prods, tsfmts, latests, steps):        
        
        os.chdir(ptype)
        
        next_day = datetime(1895, 1, 1)
        while next_day<=latest:
            
            y  = next_day.strftime('%Y')
            ts = next_day.strftime(tsfmt)
            
            for v in vs:
                if ptype=='historical':
                    cmd = f'wget "{httpspath}?type=all_bil&range=monthly&kind=historical&elem={v}&year={y}" -O zip/{v}_{y}.zip'
                print(cmd); os.system(cmd)
    
            for v in vs:
                cmd = f'unzip -o -q -d {bildir} zip/{v}_{ts}.zip'
                print(cmd); os.system(cmd)
                if v=='ppt':
                    fname = f'PRISM_{v}_{prod}_4kmM2_{ts}'
                else:
                    fname = f'PRISM_{v}_{prod}_4kmM3_{ts}'
                bilfiles = glob(f'{bildir}/{fname}??_bil.bil')
                bilfiles.sort()
                for f in bilfiles:
                    cmd = f'gdal_translate -of netcdf {f} {f.replace("bil", "nc")}'
                    print(cmd); os.system(cmd)
                cmd = f'ncecat -O -h -4 -L 5 -u time -v Band1 {ncdir}/{fname}* nc/{fname}.nc'
                print(cmd); os.system(cmd)
                cmd = f'ncrename -v Band1,{v} nc/{fname}.nc'
                print(cmd); os.system(cmd)
                if (next_day.year % 4) == 0 and next_day.year != 1900:
                    times = '0, 31,  60,  91, 121, 152, 182, 213, 244, 274, 305, 335'
                else:
                    times = '0, 31,  59,  90, 120, 151, 181, 212, 243, 273, 304, 334'
                cmd = f'ncap2 -O -s "time[time]={{{times}}}" nc/{fname}.nc nc/{fname}.nc'
                print(cmd); os.system(cmd)
                if ptype=='historical':
                    cmd = f'ncatted -a units,time,o,c,"days since {y}-01-01" nc/{fname}.nc'
                print(cmd); os.system(cmd)
                cmd = f'rm -f {ncdir}/{fname}* {bildir}/{fname}*'
                print(cmd); os.system(cmd)

            next_day += step

        os.chdir('..')
    
if __name__ == '__main__':
    main(sys.argv[1:])
