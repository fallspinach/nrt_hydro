''' Download and process River Flow and Stage from MADIS/HADS

Usage:
    python process_cdec_madis_hydro.py [yyyymmdd1] [yyyymmdd2]
Default values:
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Prototype'

import os, sys

from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from glob import glob
import xarray as xr
import csv
import time
import pytz
import pandas as pd
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


## main function
def main(argv):
    
    '''main loop'''

    madis_dir = f'{config["base_dir"]}/obs/madis/hydro'

    if len(argv)==1:
        t1 = datetime.strptime(argv[0], '%Y%m%d')
        t2 = t1
    elif len(argv)==2:
        t1 = datetime.strptime(argv[0], '%Y%m%d')
        t2 = datetime.strptime(argv[1], '%Y%m%d')
    else:
        curr_time = datetime.utcnow()
        curr_day  = curr_time - timedelta(hours=curr_time.hour, minutes=curr_time.minute, seconds=curr_time.second, microseconds=curr_time.microsecond)
        t1 = curr_day - timedelta(days=4)
        t2 = curr_day

    t1 = t1.replace(tzinfo=pytz.utc)
    t2 = t2.replace(tzinfo=pytz.utc)

    t = t1
    while t<=t2:

        dout = f'{madis_dir}/{t:%Y}'
        if not os.path.isdir(dout):
            os.system(f'mkdir -p {dout}')

        cmd = f'wget -q -r -np -nH --cut-dirs=9 --reject="*index*" --reject="*netCDF*" -P {madis_dir}/{t:%Y} https://madis-data.ncep.noaa.gov/madisPublic1/data/archive/{t:%Y/%m/%d}/LDAD/hydro/netCDF'
        print(cmd); os.system(cmd)

        cnt = 0
        for fgz in sorted(glob(f'{madis_dir}/{t:%Y/%Y%m%d}_????.gz')):
            cmd = f'gunzip {fgz}'
            f = fgz.replace('.gz', '')
            print(cmd);
            if os.system(cmd)!=0:
                continue
            cmd = f'ncks -O -4 -L 5 -v stationId,observationTime,riverFlow,riverStage {f} {f}.nc'
            print(cmd); os.system(cmd)
            cmd = f'rm -f {f}'
            os.system(cmd)
            
            ds = xr.open_dataset(f'{f}.nc', decode_cf=True)
            station_s = ds["stationId"].astype(str).to_series()
            obs_s = ds["observationTime"].to_series()
            flow_s  = ds['riverFlow'].to_series()
            stage_s = ds['riverStage'].to_series()
            df = pd.concat(
                {
                    "stationId": station_s,
                    "observationTime": obs_s,
                    "riverFlow": flow_s,
                    "riverStage": stage_s,
                },
                axis=1,
            )
            # Preserve column order
            df = df[["stationId", "observationTime", "riverFlow", "riverStage"]]
            if cnt==0:
                df_all = df.copy()
            else:
                df_all = pd.concat([df_all, df], ignore_index=True)
            cnt += 1

        df_all.dropna(subset=['riverFlow', 'riverStage'], how='all', inplace=True)

        df_all.to_csv(f'{madis_dir}/{t:%Y/%Y%m%d}.csv.gz', compression='gzip', index=False, quoting=csv.QUOTE_NONE, escapechar='\\')

        cmd = f'rm -f {madis_dir}/{t:%Y/%Y%m%d}*.nc'
        os.system(cmd)
        
        t += timedelta(days=1)


if __name__ == '__main__':
    main(sys.argv[1:])

