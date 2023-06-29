from PIL import Image
import numpy as np
import netCDF4 as nc
import os

os.chdir('/cw3e/mead/projects/cnt107/nrt_hydro/forcing/nwm/domain/pfconus2')

if False:
    f = nc.Dataset('mask_comb.nc', 'r')
    f.set_auto_mask(False)

    im = Image.open('CONUS2.0.Final1km.NWM.Mask.tif')
    pf = np.array(im)

    mask = f.variables['mask'][0][:]
    print(mask[mask==1].sum(), pf.sum())

    mask[pf==1]=1
    print(mask[mask==1].sum(), pf.sum())

    f.variables['mask'][0][:] = pf[:]

    f.sync()

    mask1 = f.variables['mask'][0][:]
    print(mask1[mask1==1].sum(), pf.sum())

    f.close()

os.system('cdo -f nc4 setctomiss,0 -setrtoc,1,100,1 -add -setmisstoc,0 mask_orig.nc CONUS2.0.Final1km.NWM.Mask.nc mask_comb.nc')