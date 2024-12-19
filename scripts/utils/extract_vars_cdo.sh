#!/bin/bash

tmpdir=/scratch/$USER/job_$SLURM_JOBID
cdo="cdo -s -O -f nc4 -z zip"

for y in `seq $1 $2`; do
  for m in `seq -w 1 12`; do
    if [ -f ../$y/$y$m.LDASOUT_DOMAIN1 ]; then
      $cdo -chname,SMTOT,SOIL_M -delname,SOIL_M -vertsum -aexpr,"SMTOT=sellevel(SOIL_M,1)*0.05+sellevel(SOIL_M,2)*0.15+sellevel(SOIL_M,3)*0.3+sellevel(SOIL_M,4)*0.5" -selname,SOIL_M,SNEQV ../$y/$y$m.LDASOUT_DOMAIN1 $tmpdir/tmp$y$m.SMTOT_SWE
    fi
  done
  $cdo add -mergetime [ $tmpdir/tmp$y??.SMTOT_SWE ] ../../../../domain/xmask0_conus.nc extract/$y.SMTOT_SWE
  $cdo selname,streamflow -mergetime ../$y/$y??.CHRTOUT_DOMAIN1 extract/$y.STREAMFLOW
  rm -f $tmpdir/tmp$y*
done

