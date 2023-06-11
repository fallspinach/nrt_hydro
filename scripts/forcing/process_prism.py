###############################################################################
# Download and process PRISM data
# Ming Pan <m3pan@ucsd.edu>
###############################################################################

import sys, os, pytz, time, subprocess
from glob import glob
import netCDF4 as nc
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

## some setups
workdir  = config['base_dir'] + '/forcing/prism'
lockfile = 'prism.lock'

httpspath = 'https://prism.oregonstate.edu/fetchData.php'

bildir = '/scratch/%s/%s/bil' % (os.getenv('USER'), os.getenv('SLURM_JOBID'))
ncdir  = bildir.replace('bil', 'nc')
for d in [bildir, ncdir]:
    if not os.path.isdir(d):
        os.system('mkdir -p %s' % d)

override_flag = True       # override the old output files or not

vs = ['ppt', 'tmean']

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(workdir)
    
    # simple file to avoid running multiple instances of this code
    if os.path.isfile(lockfile):
        print('%s is exiting: another copy of the program is running.' % os.path.basename(__file__))
        #return 1
    else:
        pass
        #os.system('touch '+lockfile)
    
    # keep the time
    time_start = time.time()
        
    #update_prism('recent')
    #update_prism('provisional')
    update_prisms()
    
    time_finish = time.time()
    print('Total download/process time %.1f seconds' % (time_finish-time_start))
    
    os.system('/bin/rm -f '+lockfile)

    return 0


def update_prism(ptype):

    os.chdir(ptype)
    if ptype=='recent':
        prod = 'stable'
    elif ptype=='provisional':
        prod = 'provisional'

    # find the last nc file
    ncfiles = glob('nc/PRISM_tmean_%s_4kmD2_*.nc' % prod)
    ncfiles.sort()
    f = nc.Dataset(ncfiles[-1], 'r')
    last_day = datetime.strptime(str(nc.num2date(f['time'][-1], f['time'].units)), '%Y-%m-%d %H:%M:%S')
    next_day = last_day + timedelta(days=1)
    f.close()
    y  = next_day.strftime('%Y')
    m  = next_day.strftime('%m')
    ym = next_day.strftime('%Y%m')

    if ptype=='recent':
        print('Last day of PRISM-recent: %s, year to update: %s' % (last_day.isoformat(), next_day.strftime('%Y')))
        ts = y
    elif ptype=='provisional':
        print('Last day of PRISM-provisional: %s, year-month to update: %s' % (last_day.isoformat(), next_day.strftime('%Y-%m')))
        ts = ym
    
    for v in vs:
        if ptype=='recent':
            cmd = 'wget "%s?type=all_year&range=daily&kind=recent&elem=%s&temporal=%s0101" -O zip/%s_%s.zip' % (httpspath, v, y, v, y)
        elif ptype=='provisional':
            cmd = 'wget "%s?type=all_bil&range=daily&kind=6months&elem=%s&temporal=%s01&year=%s" -O zip/%s_%s.zip' % (httpspath, v, m, y, v, ts)
        print(cmd); os.system(cmd)
    
    for v in vs:
        cmd = 'unzip -o -q -d %s zip/%s_%s.zip' % (bildir, v, ts)
        print(cmd); os.system(cmd)
        fname = 'PRISM_%s_%s_4kmD2_%s' % (v, prod, ts)
        bilfiles = glob('%s/%s*.bil' % (bildir, fname))
        bilfiles.sort()
        for f in bilfiles:
            cmd = 'gdal_translate -of netcdf %s %s' % (f, f.replace('bil', 'nc'))
            print(cmd); os.system(cmd)
        cmd = 'ncecat -O -h -4 -L 5 -u time -v Band1 %s/%s* nc/%s.nc' % (ncdir, fname, fname)
        print(cmd); os.system(cmd)
        cmd = 'ncrename -v Band1,%s nc/%s.nc' % (v, fname)
        print(cmd); os.system(cmd)
        times = ','.join(['%s' % i for i in range(len(bilfiles))])
        cmd = 'ncap2 -O -s "time[time]={%s}" nc/%s.nc nc/%s.nc' % (times, fname, fname)
        print(cmd); os.system(cmd)
        if ptype=='recent':
            cmd = 'ncatted -a units,time,o,c,"days since %s-01-01" nc/%s.nc' % (y, fname)
        elif ptype=='provisional':
            cmd = 'ncatted -a units,time,o,c,"days since %s-%s-01" nc/%s.nc' % (y, m, fname)
        print(cmd); os.system(cmd)
        cmd = 'rm -f %s/%s* %s/%s*' %(ncdir, fname, bildir, fname)
        print(cmd); os.system(cmd)

    os.chdir('..')

def update_prisms():

    # get current UTC time
    curr_time  = datetime.utcnow()    
    curr_month = datetime(curr_time.year, curr_time.month, 1)

    ptypes  = ['recent', 'provisional']
    prods   = ['stable', 'provisional']
    tsfmts  = ['%Y', '%Y%m']
    latests = [curr_month-relativedelta(months=7), curr_month-relativedelta(months=1)]
    steps   = [relativedelta(years=1), relativedelta(months=1)]
    
    for ptype,prod,tsfmt,latest,step in zip(ptypes, prods, tsfmts, latests, steps):        
        
        os.chdir(ptype)

        # find the last nc file
        ncfiles = glob('nc/PRISM_tmean_%s_4kmD2_*.nc' % prod)
        ncfiles.sort()
        f = nc.Dataset(ncfiles[-1], 'r')
        last_day = datetime.strptime(str(nc.num2date(f['time'][-1], f['time'].units)), '%Y-%m-%d %H:%M:%S')
        f.close()
        print('Last day of PRISM-%s: %s, latest (assumed) available is %s' % (prod, last_day.isoformat(), latest.isoformat()))
        
        next_day = last_day + timedelta(days=1)
        while next_day<=latest:
            
            y  = next_day.strftime('%Y')
            m  = next_day.strftime('%m')

            ts = next_day.strftime(tsfmt)
    
            for v in vs:
                
                if ptype=='recent':
                    cmd = 'wget "%s?type=all_year&range=daily&kind=recent&elem=%s&temporal=%s0101" -O zip/%s_%s.zip' % (httpspath, v, y, v, y)
                elif ptype=='provisional':
                    cmd = 'wget "%s?type=all_bil&range=daily&kind=6months&elem=%s&temporal=%s01&year=%s" -O zip/%s_%s.zip' % (httpspath, v, m, y, v, ts)
                print(cmd); os.system(cmd)
    
            for v in vs:
                cmd = 'unzip -o -q -d %s zip/%s_%s.zip' % (bildir, v, ts)
                print(cmd); os.system(cmd)
                fname = 'PRISM_%s_%s_4kmD2_%s' % (v, prod, ts)
                bilfiles = glob('%s/%s*.bil' % (bildir, fname))
                bilfiles.sort()
                for f in bilfiles:
                    cmd = 'gdal_translate -of netcdf %s %s' % (f, f.replace('bil', 'nc'))
                    print(cmd); os.system(cmd)
                cmd = 'ncecat -O -h -4 -L 5 -u time -v Band1 %s/%s* nc/%s.nc' % (ncdir, fname, fname)
                print(cmd); os.system(cmd)
                cmd = 'ncrename -v Band1,%s nc/%s.nc' % (v, fname)
                print(cmd); os.system(cmd)
                times = ','.join(['%s' % i for i in range(len(bilfiles))])
                cmd = 'ncap2 -O -s "time[time]={%s}" nc/%s.nc nc/%s.nc' % (times, fname, fname)
                print(cmd); os.system(cmd)
                if ptype=='recent':
                    cmd = 'ncatted -a units,time,o,c,"days since %s-01-01" nc/%s.nc' % (y, fname)
                elif ptype=='provisional':
                    cmd = 'ncatted -a units,time,o,c,"days since %s-%s-01" nc/%s.nc' % (y, m, fname)
                print(cmd); os.system(cmd)
                cmd = 'rm -f %s/%s* %s/%s*' %(ncdir, fname, bildir, fname)
                print(cmd); os.system(cmd)

            next_day += step

        os.chdir('..')

    
if __name__ == '__main__':
    main(sys.argv[1:])
