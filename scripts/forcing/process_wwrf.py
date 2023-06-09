###############################################################################
# Download and process West-WRF NRT data, 
# Ming Pan <m3pan@ucsd.edu>
###############################################################################

import sys, os, pytz, time, yaml
from glob import glob
import numpy as np
import numpy.ma as ma
from datetime import datetime, timedelta
from utilities import config, base_dir, find_last_time
from mpi4py import MPI

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## some setups
wwrfdir  = base_dir + '/forcing/wwrf'
lockfile = 'wwrf.lock'

fcst_init   = 'ecmwf'
fcst_domain = '01'
fcst_length = 10

tmpdir = '/scratch/%s/%s' % (os.getenv('USER'), os.getenv('SLURM_JOBID'))

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(wwrfdir)
    
    # simple file to avoid running multiple instances of this code
    #if os.path.isfile(lockfile):
        #print('%s is exiting: another copy of the program is running.' % os.path.basename(__file__))
        #return 1
    #else:
        #os.system('touch '+lockfile)
    
    # keep the time
    #time_start = time.time()
    
    # get current UTC time
    curr_time = datetime.utcnow()
    curr_time = curr_time.replace(tzinfo=pytz.utc)
    
    curr_day  = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
    
    # figure out the water year
    wy      = curr_day.year if curr_day.month>=10 else curr_day.year-1
    fcst_dir = 'links/NRT/%d-%d/NRT_%s' % (wy, wy+1, fcst_init)
    
    # find the latest West-WRF forecast
    latest_day = find_last_time(fcst_dir+'/??????????', '%Y%m%d%H') #- timedelta(hours=24)
    
    fcst_length = 10

    if len(argv)>0:
        fcst_length = int(argv[0])
    if len(argv)>1:
        latest_day = datetime.strptime(argv[1], '%Y%m%d%H')
        latest_day = latest_day.replace(tzinfo=pytz.utc)
        
    print('Latest forecast to process: %s.' % (latest_day.isoformat()))
    
    #sys.exit("here")
    
    ncocmd1 = 'ncap2 -O -s "PSFC=PSFC*100; RAINRATE=RAINRATE/3600" '
    ncocmd2 = 'ncatted -a units,PSFC,o,c,"Pa" -a units,RAINRATE,o,c,"kg m-2 s-1" '
    
    for region in config['regions']:
        
        out_dir  = 'NRT/%d-%d/NRT_%s/%s' % (wy, wy+1, fcst_init, region)
        if not os.path.isdir(out_dir):
            cmd = 'mkdir -p %s' % out_dir
            print(cmd); os.system(cmd)
        
        cdocmd = 'cdo -O -f nc4 -z zip remap,../nwm/domain/scrip_%s_bilinear.nc,NRT/%d-%d/NRT_%s/cdo_weights_d01_cf_%s.nc -chname,p_sfc,PSFC,T_2m,T2D,q_2m,Q2D,LW_d,LWDOWN,SW_d,SWDOWN,precip_bkt,RAINRATE,u_10m_gr,U2D,v_10m_gr,V2D -selname,p_sfc,T_2m,q_2m,LW_d,SW_d,precip_bkt,u_10m_gr,v_10m_gr' % (region, wy, wy+1, fcst_init, region)
    
        t = latest_day + timedelta(hours=1)
        last_day = latest_day + timedelta(days=fcst_length)
        allsteps = []
        alldays  = []
        while t <= last_day:
            fww = '%s/%s/cf/wrfcf_%s_d%s_%s_00_00.nc' % (fcst_dir, latest_day.strftime('%Y%m%d%H'), fcst_init, fcst_domain, t.strftime('%Y-%m-%d_%H'))
            fnwm = '%s/%s.LDASIN_DOMAIN1' % (out_dir, t.strftime('%Y%m%d%H'))
            if os.path.isfile(fww): # and not os.path.isfile(fnwm):
                allsteps.append(t)
            if t.hour == 23:
                alldays.append(t-timedelta(hours=23))
            t = t + timedelta(hours=1)
        # add last day for WRF-Hydro
        alldays.append(alldays[-1]+timedelta(days=1))
        #print(alldays)

        for t in allsteps[rank::size]:
        
            print(t.strftime('Processing %Y-%m-%d %H:00'))

            fww = '%s/%s/cf/wrfcf_%s_d%s_%s_00_00.nc' % (fcst_dir, latest_day.strftime('%Y%m%d%H'), fcst_init, fcst_domain, t.strftime('%Y-%m-%d_%H'))
            ftmp = '%s/%s.LDASIN_DOMAIN1' % (tmpdir,  t.strftime('%Y%m%d%H'))
            ftmp2 = '%s/%s.LDASIN_DOMAIN1.nc' % (tmpdir,  t.strftime('%Y%m%d%H'))
            fnwm = '%s/%s.LDASIN_DOMAIN1' % (out_dir, t.strftime('%Y%m%d%H'))
            if os.path.isfile(fww):
                cmd = '%s %s %s' % (cdocmd, fww, ftmp)
                #print(cmd)
                os.system(cmd)
                cmd = '%s %s %s' % (ncocmd1, ftmp, ftmp2)
                os.system(cmd)
                cmd = 'cdo -f nc4 -z zip add %s ../nwm/domain/xmask0_%s.nc %s' % (ftmp2, region, fnwm)
                os.system(cmd)
                cmd = '%s %s' % (ncocmd2, fnwm)
                os.system(cmd)
                cmd = '/bin/rm -f %s' % (ftmp)
                os.system(cmd)
            
        comm.Barrier()
    
        for t in alldays[rank::size]:
            fh = '%s/%s??.LDASIN_DOMAIN1' % (out_dir, t.strftime('%Y%m%d'))
            fd = '%s/%s.LDASIN_DOMAIN1' % (out_dir, t.strftime('%Y%m%d'))
            cmd = 'cdo -O -f nc4 -z zip mergetime %s %s' % (fh, fd)
            os.system(cmd)
    
        # delete hourly files older than 2 days
        old_day = latest_day - timedelta(days=2)
        cmd = '/bin/rm -f %s/%s??.LDASIN_DOMAIN1' % (out_dir, old_day.strftime('%Y%m%d'))
        os.system(cmd)
    
    time_finish = time.time()
    #print('Total processing time %.1f seconds' % (time_finish-time_start))
    
    os.system('/bin/rm -f '+lockfile)

    comm.Barrier()
    return 0
    

if __name__ == '__main__':
    main(sys.argv[1:])
