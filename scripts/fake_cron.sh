#!/bin/bash
# Shell script to mimic the function of "cron" to initiate scheduled tasks as the cron tool is unavailable on a cluster
# Usage: ./fake_cron.sh
#
# author: Ming Pan
# email: m3pan@ucsd.edu
# status: Development

hinterval=4
hoffset=0

#cd /expanse/nfs/cw3e/cwp101/nrt_hydro/
cd $(realpath $(dirname $0))/../

sleep 60m

while true; do
    currhour=$(date -u +%k)
    
    if [ $currhour == 8 ]; then
    
        # update NRT forcing
        flog=forcing/log/update_nrt_`date -u +\%Y\%m\%d_\%H`z.txt
        sbatch -t 2:00:00 --nodes=1 --ntasks-per-node=1 -p cw3e-shared -A cwp101 -J forcnrt -o $flog --wrap="python scripts/forcing/update_conus_forcing_nrt.py"
        
        # collect MODIS SCA
        flog=wrf_hydro/cnrfc/nrt/run/log/log_modis_$(date -u +%Y%m%d_%H)z.txt
        sbatch -t 2:00:00 --nodes=1 --ntasks-per-node=1 --mem=20G -p cw3e-shared -A cwp101 -J modissca -o $flog --wrap="python scripts/obs/process_modis_sca.py"
        # collect FNF data from CDEC
        python scripts/obs/process_cdec_fnf.py
        
        sleep 60m
    
    elif  [ $currhour == 11 ]; then
    
        # run WRF-Hydro NRT
        for domain in cnrfc basins24; do
            flog=wrf_hydro/$domain/nrt/run/log/log_nrt_$(date -u +%Y%m%d_%H)z.txt
            python scripts/wrf_hydro/run_nrt.py $domain > $flog 2>&1
        done
        sleep 30m

        # run WRF-Hydro + WWRF deterministic forecast
        for domain in cnrfc; do
            flog=wrf_hydro/$domain/fcst/wwrf/run/log/log_wwrf_$(date -u +%Y%m%d_%H)z.txt
            python scripts/wrf_hydro/run_wwrf.py $domain > $flog 2>&1
        done
        sleep 30m

        # update system status
        flog=wrf_hydro/$domain/fcst/wwrf/run/log/log_status_$(date -u +%Y%m%d_%H)z.txt
        python scripts/wrf_hydro/check_status.py update_gcloud > $flog 2>&1
        
    elif  [ $currhour == 18 ]; then
    
        # update WWRF deterministic (ECMWF) forecast forcing
        flog=forcing/log/update_wwrf_`date -u +\%Y\%m\%d_\%H`z.txt
        sbatch -t 00:30:00 --nodes=1 --ntasks-per-node=10 --mem=30G -p cw3e-shared -A cwp101 -J wwrfdet -o $flog --wrap="unset SLURM_MEM_PER_NODE; mpirun -np 10 python scripts/forcing/process_wwrf.py"
        
        # update WWRF ensemble forecast forcing between September and April
        if [ `date -u +%m` -le "04" ] || [ `date -u +%m` -ge "09" ]; then
            flog=forcing/log/update_wwrfens_`date -u +\%Y\%m\%d_\%Hz`.txt
            #sbatch -t 04:00:00 --nodes=1 --ntasks-per-node=12 -p cw3e-shared -A cwp101 -J wwrfens -o $flog --wrap="mpirun -np 12 python scripts/forcing/process_wwrf_ens.py"
        fi

        # rsync data to skyriver
        

        sleep 60m
        
    else
        sleep 10m
    fi
done



