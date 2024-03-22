#!/bin/bash

hinterval=4
hoffset=0

cd /expanse/nfs/cw3e/cwp101/nrt_hydro/

sleep 60m

while true; do
    currhour=$(date -u +%k)
    
    if [ $currhour == 8 ]; then
        # update NRT forcing
        flog=forcing/log/update_nrt_`date -u +\%Y\%m\%d_\%H`z.txt
        sbatch -t  2:00:00 --nodes=1 --ntasks-per-node=1 -p cw3e-shared -A cwp101 -J forcnrt -o $flog --wrap="python scripts/forcing/update_conus_forcing_nrt.py"
        sleep 60m
    
    elif  [ $currhour == 10 ]; then
        # run WRF-Hydro
        for domain in cnrfc; do
            flog=wrf_hydro/$domain/nrt/run/log/log_nrt_$(date -u +%Y%m%d_%H)z.txt
            python scripts/wrf_hydro/run_nrt.py $domain > $flog 2>&1
        done
        sleep 60m
    
    else
        sleep 10m
    fi
done



            # update WWRF deterministic (ECMWF) forecast forcing
            #sbatch -t 00:40:00 --nodes=1 --ntasks-per-node=12 -p cw3e-shared -A cwp101 -J wwrfdet -o forcing/log/update_wwrf_`date -u +\%Y\%m\%d_\%Hz`.txt --wrap="mpirun -np 12 python scripts/forcing/process_wwrf.py"
            # update WWRF ensemble forecast forcing between September and April
            #if [ `date -u +%m` -le "04" ] || [ `date -u +%m` -ge "09" ]; then 
            #    sbatch -t 04:00:00 --nodes=1 --ntasks-per-node=12 -p cw3e-shared -A cwp101 -J wwrfens -o forcing/log/update_wwrfens_`date -u +\%Y\%m\%d_\%Hz`.txt --wrap="mpirun -np 12 python scripts/forcing/process_wwrf_ens.py"
            #fi

            #flog=log/log_wwrf_$(date -u +%Y%m%d_%H)z.txt
            #python run_wwrf.py > $flog 2>&1
            #sbatch -A cwp101 -t 00:20:00 -p shared -n 1 --mem=20G -J "modis" --wrap="python process_modis_sca.py"
            #sleep 40m
            #python check_status.py update_gcloud >> $flog 2>&1
