#!/bin/bash

cd nldas2/NLDAS_FORA0125_H.002

for y in $1 $2; do
    mkdir -p $y
    for f in ../../../../../cwp101/wrf_hydro/forcing/nldas2/NLDAS_FORA0125_H.002/$y/*.grb; do
      fn=`basename $f`
      ftmp=/scratch/$USER/$SLURM_JOBID/${fn/grb/nc}
      fnn=$y/${fn/grb/nc}
      echo $f $fnn
      cdo -O -f nc4 -z zip chname,var61,apcpsfc,var157,cape180_0mb,var205,dlwrfsfc,var204,dswrfsfc,var228,pevapsfc,var1,pressfc,var51,spfh2m,var11,tmp2m,var33,ugrd10m,var34,vgrd10m,var153,var153sfc $f $ftmp
      ncatted -a long_name,apcpsfc,a,c,"surface Total precipitation" -a long_name,cape180_0mb,a,c,"180-0 mb above gnd Convective available potential energy" -a long_name,dlwrfsfc,a,c,"surface Downward longwave radiation flux" -a long_name,dswrfsfc,a,c,"surface Downward shortwave radiation flux" -a long_name,pevapsfc,a,c,"surface Potential evaporation" -a long_name,pressfc,a,c,"surface Pressure" -a long_name,spfh2m,a,c,"2 m above ground Specific humidity" -a long_name,tmp2m,a,c,"2 m above ground Temperature" -a long_name,ugrd10m,a,c,"10 m above ground u wind" -a long_name,vgrd10m,a,c,"10 m above ground v wind" -a long_name,var153sfc,a,c,"surface undefined" -a units,apcpsfc,a,c,"kg/m^2" -a units,cape180_0mb,a,c,"J/Kg" -a units,dlwrfsfc,a,c,"W/m^2" -a units,dswrfsfc,a,c,"W/m^2" -a units,pevapsfc,a,c,"Kg/m^2" -a units,pressfc,a,c,"Pa" -a units,spfh2m,a,c,"kg/kg" -a units,tmp2m,a,c,"K" -a units,ugrd10m,a,c,"m/s" -a units,vgrd10m,a,c,"m/s" -a units,var153sfc,a,c,"" $ftmp
      ncwa -O -a height,height_2,lev $ftmp $fnn
      /bin/rm -f $ftmp
  done
done
