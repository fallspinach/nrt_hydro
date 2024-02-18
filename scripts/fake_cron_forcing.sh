#!/bin/bash

hinterval=4
hoffset=0

cd /expanse/nfs/cw3e/cwp101/nrt_hydro/

sleep 60m

while true; do
    currhour=$(date -u +%k)
    if [ $(( $currhour % $hinterval )) == $hoffset ]; then
        date -u
        #echo Hour $currhour has a $hoffset-hour offset to $hinterval-hour intervals.
        if [ $currhour == 8 ]; then
            # update NRT forcing
            sbatch -t  2:00:00 --nodes=1 --ntasks-per-node=1 -p cw3e-shared -A cwp101 -J forcnrt -o forcing/log/update_nrt_`date -u +\%Y\%m\%d_\%Hz`.txt --wrap="python scripts/forcing/update_conus_forcing_nrt.py"
            # update WWRF deterministic (ECMWF) forecast forcing
            #sbatch -t 00:40:00 --nodes=1 --ntasks-per-node=12 -p cw3e-shared -A cwp101 -J wwrfdet -o forcing/log/update_wwrf_`date -u +\%Y\%m\%d_\%Hz`.txt --wrap="mpirun -np 12 python scripts/forcing/process_wwrf.py"
            # update WWRF ensemble forecast forcing between September and April
            #if [ `date -u +%m` -le "04" ] || [ `date -u +%m` -ge "09" ]; then 
            #    sbatch -t 04:00:00 --nodes=1 --ntasks-per-node=12 -p cw3e-shared -A cwp101 -J wwrfens -o forcing/log/update_wwrfens_`date -u +\%Y\%m\%d_\%Hz`.txt --wrap="mpirun -np 12 python scripts/forcing/process_wwrf_ens.py"
            #fi
        fi
        sleep 60m
    else
        #echo Hour $currhour does not have a $hoffset-hour offset to $hinterval-hour intervals.
        sleep 10m
    fi
done



