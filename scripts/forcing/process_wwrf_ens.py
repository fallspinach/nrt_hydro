###############################################################################
# Pprocess West-WRF NRT ensemble forecast data, 
# Ming Pan <m3pan@ucsd.edu>
###############################################################################

import sys, os, pytz, time
from glob import glob
import numpy as np
import numpy.ma as ma
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time
from mpi4py import MPI

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

## some setups
wwrfdir  = config['base_dir'] + '/forcing/wwrf'

fcst_init   = 'ens'
fcst_domain = '01'
fcst_length = 7

enss = [ 'ecm%03d' % i for i in [4, 5, 6, 7, 24, 25, 26, 27, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 
                                 64, 65, 66, 67, 84, 85, 86, 87, 104, 105, 106, 107]]

tmpdir = '/scratch/%s/%s' % (os.getenv('USER'), os.getenv('SLURM_JOBID'))

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(wwrfdir)
        
    # keep the time
    #time_start = time.time()
    
    # get current UTC time
    curr_time = datetime.utcnow()
    curr_time = curr_time.replace(tzinfo=pytz.utc)
    
    curr_day  = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
    
    # figure out the water year
    wy      = curr_day.year if curr_day.month>=10 else curr_day.year-1
    fcst_dir = 'links/NRT/%d-%d/NRT_%s' % (wy, wy+1, fcst_init)
    out_dir  =       'NRT/%d-%d/NRT_%s' % (wy, wy+1, fcst_init)
    
    # find the latest West-WRF forecast
    latest_day = find_last_time(fcst_dir+'/??????????', '%Y%m%d%H')
    #latest_day = datetime(2023, 1, 1); latest_day = latest_day.replace(tzinfo=pytz.utc)
    
    fcst_length = 7

    if len(argv)>0:
        fcst_length = int(argv[0])
    if len(argv)>1:
        latest_day = datetime.strptime(argv[1], '%Y%m%d%H')
        latest_day = latest_day.replace(tzinfo=pytz.utc)

    print('Latest forecast to process: %s.' % (latest_day.isoformat()))
    
    #sys.exit("here")

    for region in config['forcing']['regions']:
        
        cdocmd = 'cdo -O -f nc4 -z zip remap,../nwm/domain/scrip_%s_bilinear.nc,%s/cdo_weights_d01_cf_%s.nc -chname,p_sfc,PSFC,T_2m,T2D,q_2m,Q2D,LW_d,LWDOWN,SW_d,SWDOWN,precip_bkt,RAINRATE,u_10m_gr,U2D,v_10m_gr,V2D -selname,p_sfc,T_2m,q_2m,LW_d,SW_d,precip_bkt,u_10m_gr,v_10m_gr' % (region, out_dir, region)
        ncocmd1 = 'ncap2 -O -s "PSFC=PSFC*100; RAINRATE=RAINRATE/3600" '
        ncocmd2 = 'ncatted -a units,PSFC,o,c,"Pa" -a units,RAINRATE,o,c,"kg m-2 s-1" '
    
        t = latest_day + timedelta(hours=1)
        last_day = latest_day + timedelta(days=fcst_length)
        allsteps = []
        alldays  = []
        while t <= last_day:
            fww = '%s/%s/cf/ecm004/wrfcf_d%s_%s_00_00.nc' % (fcst_dir, latest_day.strftime('%Y%m%d%H'), fcst_domain, t.strftime('%Y-%m-%d_%H'))
            fnwm = '%s/%s/%s.LDASIN_DOMAIN1' % (out_dir, region, t.strftime('%Y%m%d%H'))
            if os.path.isfile(fww): # and not os.path.isfile(fnwm):
                allsteps.append(t)
            if t.hour == 23:
                alldays.append(t-timedelta(hours=23))
            t = t + timedelta(hours=1)
        # add last day for WRF-Hydro
        alldays.append(alldays[-1]+timedelta(days=1))
        #print(allsteps); print(alldays)


        for ens in enss[rank::size]:
        
            for t in allsteps:
        
                #print(t.strftime('Processing %Y-%m-%d %H:00'))

                fww = '%s/%s/cf/%s/wrfcf_d%s_%s_00_00.nc' % (fcst_dir, latest_day.strftime('%Y%m%d%H'), ens, fcst_domain, t.strftime('%Y-%m-%d_%H'))
                ftmp = '%s/%s/%s.LDASIN_DOMAIN1' % (tmpdir, ens, t.strftime('%Y%m%d%H'))
                ftmp2 = '%s/%s/%s.LDASIN_DOMAIN1.nc' % (tmpdir, ens, t.strftime('%Y%m%d%H'))
                dtmp = os.path.dirname(ftmp)
                if not os.path.isdir(dtmp):
                    os.system('mkdir -p %s' % dtmp)
                fnwm = '%s/%s/%s/%s.LDASIN_DOMAIN1' % (out_dir, region, ens, t.strftime('%Y%m%d%H'))
                dnwm = os.path.dirname(fnwm)
                if not os.path.isdir(dnwm):
                    os.system('mkdir -p %s' % dnwm)
                if os.path.isfile(fww):
                    cmd = '%s %s %s' % (cdocmd, fww, ftmp)
                    #print(cmd)
                    os.system(cmd)
                    cmd = '%s %s %s' % (ncocmd1, ftmp, ftmp2)
                    os.system(cmd)
                    cmd = '%s %s' % (ncocmd2, ftmp2)
                    os.system(cmd)
                    cmd = 'cdo -f nc4 -z zip add %s ../nwm/domain/xmask0_%s.nc %s' % (ftmp2, region, fnwm)
                    os.system(cmd)
                    cmd = '/bin/rm -f %s %s' % (ftmp, ftmp2)
                    os.system(cmd)
    
            for t in alldays:
            
                fh = '%s/%s/%s/%s??.LDASIN_DOMAIN1' % (out_dir, region, ens, t.strftime('%Y%m%d'))
                # repeat each hour 3 times to "fake" hourly data
                fhs = glob(fh)
                fhs.sort()
                #print(fhs)
                fh3 = ' '.join([ f+' '+f+' '+f for f in fhs])
                fd = '%s/%s/%s/%s.LDASIN_DOMAIN1' % (out_dir, region, ens, t.strftime('%Y%m%d'))
                cmd = 'cdo -O -f nc4 -z zip mergetime %s %s' % (fh3, fd)
                os.system(cmd)
                if True:
                    fh4 = '%s/%s/%s/%s[12]?.LDASIN_DOMAIN1' % (out_dir, region, ens, t.strftime('%Y%m%d'))
                    cmd = 'rm -f %s' % fh4
                    os.system(cmd)
                    fh4 = '%s/%s/%s/%s0[369].LDASIN_DOMAIN1' % (out_dir, region, ens, t.strftime('%Y%m%d'))
                    cmd = 'rm -f %s' % fh4
                    os.system(cmd)
    
        # delete hourly files older than 2 days
        if rank==0:
            old_day = latest_day - timedelta(days=2)
            for ens in enss:
                cmd = '/bin/rm -f %s/%s/%s/%s??.LDASIN_DOMAIN1' % (out_dir, region, ens, old_day.strftime('%Y%m%d'))
                os.system(cmd)
    
    #time_finish = time.time()
    #print('Total download/process time %.1f seconds' % (time_finish-time_start))
    
    comm.Barrier()
    return 0
    

if __name__ == '__main__':
    main(sys.argv[1:])
