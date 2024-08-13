''' Fill gaps in Stage IV archive version with NLDAS-2

Usage:
    python fill_stage4_with_nldas2.py [yyyymmdd1] [yyyymmdd2]
Default values:
    must specify all
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz, time
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time
import run_cmd_in_time_mpi


## some setups
workdir  = f'{config["base_dir"]}/forcing/stage4'

#cdocmd1 = 'cdo -f nc4 -z zip -setrtomiss,-1000,-1 -expr,"apcpsfc=((apcpsfc>-1))?apcpsfc:nldas2" -merge -setmisstoc,-100 archive/%Y/st4n2_%Y%m%d.nc -chname,apcpsfc,nldas2 -remap,latlon_conus_0.04deg.txt,nldas2_to_0.04deg_weight.nc -selname,apcpsfc ../nldas2/NLDAS_FORA0125_H.002/%Y/NLDAS_FORA0125_H.A%Y%m%d.002.nc filled_with_nldas2/%Y/st4nl2_%Y%m%d.nc'

cdocmd1 = 'cdo -f nc4 -z zip -setrtomiss,-1000,-1 -expr,"apcpsfc=((apcpsfc>-1))?apcpsfc:nldas2" -merge -setmisstoc,-100 archive/%Y/st4n2_%Y%m%d.nc -chname,Rainf,nldas2 -remap,latlon_conus_0.04deg.txt,nldas2_to_0.04deg_weight.nc -selname,Rainf ../nldas2/NLDAS_FORA0125_H.2.0/%Y/NLDAS_FORA0125_H.A%Y%m%d.020.nc filled_with_nldas2/%Y/st4nl2_%Y%m%d.nc'

#cdocmd2 = 'if [ -f archive/%Y/st4_conus.%Y%m%d.01h.nc ]; then cdo -f nc4 -z zip -setrtomiss,-1000,-1 -expr,"apcpsfc=((apcpsfc>-1))?apcpsfc:nldas2" -merge -setmisstoc,-100 archive/%Y/st4_conus.%Y%m%d.01h.nc -chname,apcpsfc,nldas2 -remap,latlon_conus_0.04deg.txt,nldas2_to_0.04deg_weight.nc -selname,apcpsfc ../nldas2/NLDAS_FORA0125_H.002/%Y/NLDAS_FORA0125_H.A%Y%m%d.002.nc filled_with_nldas2/%Y/st4nl2_%Y%m%d.nc; else cdo -f nc4 -z zip remap,latlon_conus_0.04deg.txt,nldas2_to_0.04deg_weight.nc -selname,apcpsfc ../nldas2/NLDAS_FORA0125_H.002/%Y/NLDAS_FORA0125_H.A%Y%m%d.002.nc filled_with_nldas2/%Y/st4nl2_%Y%m%d.nc; fi'

cdocmd2 = 'if [ -f archive/%Y/st4_conus.%Y%m%d.01h.nc ]; then cdo -f nc4 -z zip -setrtomiss,-1000,-1 -expr,"apcpsfc=((apcpsfc>-1))?apcpsfc:nldas2" -merge -setmisstoc,-100 archive/%Y/st4_conus.%Y%m%d.01h.nc -chname,Rainf,nldas2 -remap,latlon_conus_0.04deg.txt,nldas2_to_0.04deg_weight.nc -selname,Rainf ../nldas2/NLDAS_FORA0125_H.2.0/%Y/NLDAS_FORA0125_H.A%Y%m%d.020.nc filled_with_nldas2/%Y/st4nl2_%Y%m%d.nc; else cdo -f nc4 -z zip chname,Rainf,apcpsfc -remap,latlon_conus_0.04deg.txt,nldas2_to_0.04deg_weight.nc -selname,Rainf ../nldas2/NLDAS_FORA0125_H.2.0/%Y/NLDAS_FORA0125_H.A%Y%m%d.020.nc filled_with_nldas2/%Y/st4nl2_%Y%m%d.nc; fi'

## main function
def main(argv):
    
    '''main loop'''
    
    os.chdir(workdir)

    if datetime.strptime(argv[1], '%Y%m%d') < datetime(2020, 1, 1) and datetime.strptime(argv[0], '%Y%m%d') >= datetime(2002, 1, 1):
        cmd = cdocmd1
    else:
        cmd = cdocmd2

    run_cmd_in_time_mpi.main(['daily', argv[0], argv[1], cmd])
    
    return 0
    

if __name__ == '__main__':
    main(sys.argv[1:])
