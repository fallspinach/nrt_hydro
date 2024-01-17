###############################################################################
# Process West-WRF NRT deterministic forecast data, 
# Ming Pan <m3pan@ucsd.edu>
###############################################################################

import sys, os, pytz, time, yaml
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
wwrfdir  = f'{config["base_dir"]}/forcing/wwrf'

fcst_init   = 'ecmwf'
fcst_domain = '01'
fcst_length = 10

tmpdir = f'/scratch/{os.getenv("USER")}/{os.getenv("SLURM_JOBID")}'

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
    fcst_dir = f'links/NRT/{wy:d}-{wy+1:d}/NRT_{fcst_init}'
    out_dir  =       f'NRT/{wy:d}-{wy+1:d}/NRT_{fcst_init}'
    
    # find the latest West-WRF forecast
    latest_day = find_last_time(fcst_dir+'/??????????', '%Y%m%d%H') #- timedelta(hours=24)
    
    fcst_length = 10

    if len(argv)>0:
        fcst_length = int(argv[0])
    if len(argv)>1:
        latest_day = datetime.strptime(argv[1], '%Y%m%d%H')
        latest_day = latest_day.replace(tzinfo=pytz.utc)
        
    print(f'Latest forecast to process: {latest_day:%Y-%m-%dT%H}.')
    
    #sys.exit("here")
    
    ncocmd1 = 'ncap2 -O -s "PSFC=PSFC*100; RAINRATE=RAINRATE/3600" '
    ncocmd2 = 'ncatted -a units,PSFC,o,c,"Pa" -a units,RAINRATE,o,c,"kg m-2 s-1" '
    
    for region in config['forcing']['regions']:
        
        cdocmd = f'cdo -O -f nc4 -z zip remap,../nwm/domain/scrip_{region}_bilinear.nc,{out_dir}/cdo_weights_d01_cf_{region}.nc -chname,p_sfc,PSFC,T_2m,T2D,q_2m,Q2D,LW_d,LWDOWN,SW_d,SWDOWN,precip_bkt,RAINRATE,u_10m_gr,U2D,v_10m_gr,V2D -selname,p_sfc,T_2m,q_2m,LW_d,SW_d,precip_bkt,u_10m_gr,v_10m_gr'
    
        t = latest_day + timedelta(hours=1)
        last_day = latest_day + timedelta(days=fcst_length)
        allsteps = []
        alldays  = []
        while t <= last_day:
            fww = f'{fcst_dir}/{latest_day:%Y%m%d%H}/cf/wrfcf_{fcst_init}_d{fcst_domain}_{t:%Y-%m-%d_%H}_00_00.nc'
            fnwm = f'{out_dir}/{region}/{t:%Y%m%d%H}.LDASIN_DOMAIN1'
            if os.path.isfile(fww): # and not os.path.isfile(fnwm):
                allsteps.append(t)
            if t.hour == 23:
                alldays.append(t-timedelta(hours=23))
            t = t + timedelta(hours=1)
        # add last day for WRF-Hydro
        alldays.append(alldays[-1]+timedelta(days=1))
        #print(alldays)

        for t in allsteps[rank::size]:
        
            print(f'Processing {t:%Y-%m-%d %H:00}')

            fww   = f'{fcst_dir}/{latest_day:%Y%m%d%H}/cf/wrfcf_{fcst_init}_d{fcst_domain}_{t:%Y-%m-%d_%H}_00_00.nc'
            ftmp  = f'{tmpdir}/{t:%Y%m%d%H}.LDASIN_DOMAIN1'
            ftmp2 = f'{ftmp}.nc'
            fnwm  = f'{out_dir}/{region}/{t:%Y%m%d%H}.LDASIN_DOMAIN1'
            if os.path.isfile(fww):
                cmd = f'{cdocmd} {fww} {ftmp}'
                #print(cmd)
                os.system(cmd)
                cmd = f'{ncocmd1} {ftmp} {ftmp2}'
                os.system(cmd)
                cmd = f'cdo -f nc4 -z zip add {ftmp2} ../nwm/domain/xmask0_{region}.nc {fnwm}'
                os.system(cmd)
                cmd = f'{ncocmd2} {fnwm}'
                os.system(cmd)
                cmd = f'/bin/rm -f {ftmp}'
                os.system(cmd)
            
        comm.Barrier()
    
        for t in alldays[rank::size]:
            fh = f'{out_dir}/{region}/{t:%Y%m%d}??.LDASIN_DOMAIN1'
            fd = f'{out_dir}/{region}/{t:%Y%m%d}.LDASIN_DOMAIN1'
            cmd = f'cdo -O -f nc4 -z zip mergetime {fh} {fd}'
            os.system(cmd)
    
        # delete hourly files older than 2 days
        old_day = latest_day - timedelta(days=2)
        cmd = f'/bin/rm -f {out_dir}/{region}/{old_day:%Y%m%d}.LDASIN_DOMAIN1'
        os.system(cmd)
    
    time_finish = time.time()
    #print('Total processing time %.1f seconds' % (time_finish-time_start))
    
    comm.Barrier()
    return 0
    

if __name__ == '__main__':
    main(sys.argv[1:])
