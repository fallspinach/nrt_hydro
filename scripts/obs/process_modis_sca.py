import os, sys
from glob import glob

from modis_tools.auth import ModisSession
from modis_tools.resources import CollectionApi, GranuleApi
from modis_tools.granule_handler import GranuleHandler

from datetime import datetime, timedelta, timezone
import pytz

import netCDF4 as nc
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time


def cmap_listed(cmname, vmin=0, vmax=100):
    '''listed colormaps'''

    vmin = float(vmin)
    vmax = float(vmax)

    if cmname=='modis_sca':
        bounds = np.array([-1, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
        colors = np.array([plt.cm.spring_r(1/(bounds.size-1)*i)[:3] for i in range(bounds.size)], float)
        colors[0, :] = colors[0, :]*0+0.7
        
    ticks = bounds
    bounds = (vmax - vmin) / (bounds.max()-bounds.min()) * bounds + vmin
    norm = mpl.colors.BoundaryNorm(bounds-vmin, colors.shape[0])
    cmap = mpl.colors.ListedColormap(colors, cmname)
    cmap.set_bad('white')

    return cmap, norm, ticks


## main function
def main(argv):
    
    '''main loop'''
    
    if len(argv)>=1:
        domain = argv[0]
    else:
        domain = 'cnrfc'
    
    modis_dir = f'{config["base_dir"]}/wrf_hydro/{domain}/obs/modis'
    web_dir   = f'{config["base_dir"]}/wrf_hydro/{domain}/web'
    
    os.chdir(modis_dir)

    # Authenticate a session
    username = 'fallspinach'
    password = 'TsingHua1911'
    session = ModisSession(username=username, password=password)

    # Query the MODIS catalog for collections
    collection_client = CollectionApi(session=session)
    collections = collection_client.query(short_name='MOD10A1', version='61')
    #print(collections[0])

    # Query the selected collection for granules
    granule_client = GranuleApi.from_collection(collections[0], session=session)

    # Filter the selected granules via spatial and temporal parameters
    backday = datetime.utcnow()-timedelta(days=4); backday = backday.replace(tzinfo=pytz.utc)
    #backday = find_last_time('nc/MOD10A1.A???????.nc', 'MOD10A1.A%Y%j.nc')
    utcnow = datetime.now(timezone.utc)
    ca_bbox = [-121, 38, -115, 41]
    ca_granules = granule_client.query(start_date=backday, bounding_box=ca_bbox)

    # Download the granules
    GranuleHandler.download_from_granules(ca_granules, session, path='hdf')

    t = backday
    while t<=utcnow:
    
        # fix file names and convert to nc
        fhs = glob(f'hdf/MOD10A1.A{t:%Y%j}.h0?v0?.061.?????????????.hdf')
        for fh1 in fhs:
            fh = '.'.join(fh1.split('.')[:-2])+'.hdf'
            os.system(f'/bin/mv {fh1} {fh}')
            ft = fh.replace('hdf', 'tif')
            if not os.path.isfile(ft):
                os.system(f'modis_convert.py -o {ft} -s "(1)" -f netCDF -e 4326 -g 0.005 {fh}')
                os.system(f'/bin/mv {ft}.tif {ft}')
    
        # mosaic files
        ft = f'tif/MOD10A1.A{t:%Y%j}.h08v05.061.tif'
        fn = f'nc/MOD10A1.A{t:%Y%j}.nc'
        if os.path.isfile(ft):
            cmd = f'gdal_merge.py -o nc/MOD10A1.A{t:%Y%j}.nc -of netCDF tif/MOD10A1.A{t:%Y%j}.h0?v0?.061.tif'
            os.system(cmd)
            os.system(f'cdo -f nc4 -z zip copy {fn} {fn}4; /bin/mv {fn}4 {fn}')
    
        t += timedelta(days=1)
    
    print('Start to plot ...')

    cnrfc = np.array([-125, -113, 32, 44], dtype=float)/180*np.pi
    lonlim = cnrfc[:2]
    latlim = np.log(np.tan(cnrfc[2:]/2+np.pi/4))

    plt.ioff()

    ncvar = 'Band1'
    imvar = 'modis_sca'

    t = backday
    while t<=utcnow:
    
        fn = f'nc/MOD10A1.A{t:%Y%j}.nc'
        fout = f'{web_dir}/imgs/obs/modis/{t:%Y}/{imvar}_{t:%Y%m%d}.png'
    
        if os.path.isfile(fn): # and not os.path.isfile(fout):
        
            print(f'{t:%Y-%m-%d}')
            f = nc.Dataset(fn, 'r')
        
            lat = f.variables['lat'][:]/180*np.pi
            lon = f.variables['lon'][:]/180*np.pi
        
            xx,yy = np.meshgrid(lon, np.log(np.tan(lat/2+np.pi/4)))
        
            data = f.variables[ncvar][:].astype(float)
            data[(data>130)&(data<254)&(data!=239)] = -0.6
            data = np.ma.masked_where((data==239)|(data==129), data)
            #data = data1
        
            fig1 = plt.figure(figsize=(16,16))
            ax1 = fig1.add_axes([0,0,1,1])
        
            cmap, norm, ticks = cmap_listed(imvar, -1, 100)
            ax1.pcolormesh(xx, yy, data, shading='auto', cmap=cmap, norm=norm)
            ax1.set_xlim(lonlim)
            ax1.set_ylim(latlim)
            ax1.axis('off')
            #fout = '%s/imgs/obs/modis/%s/%s_%s.png' % (web_dir, t.strftime('%Y'), imvar, t.strftime('%Y%m%d'))
            dout = os.path.dirname(fout)
            if not os.path.isdir(dout):
                os.system(f'mkdir -p {dout}')
            fig1.savefig(fout, dpi=70, transparent=True)
            os.system(f'convert -transparent white {fout} {fout}.png; /bin/mv {fout}.png {fout}')
            plt.clf()
            plt.close(fig1)
        
            if False: #t==backday and rank==0:
            
                data = np.ma.masked_where(data<100000000000000, data)
    
                fig1 = plt.figure(figsize=(16,16))
                ax1 = fig1.add_axes([0.1,0.1,0.8,0.8])
        
                #cmap, norm, ticks = cmap_listed(imvar, -1, 100)
                cmap.set_bad('white')
                print(ticks)
                print(cmap.colors)
                cs = ax1.pcolormesh(xx, yy, data, shading='auto', cmap=cmap, norm=norm)
                ax1.set_xlim(lonlim)
                ax1.set_ylim(latlim)
                ax1.axis('off')
                cbar = plt.colorbar(cs, orientation='vertical', fraction=0.03, pad=0.02, ticks=ticks)
                cbar.set_label('MODIS Snow Cover (%)', fontsize=24)
                cbar.ax.tick_params(labelsize=20)
                fout = f'imgs/{imvar}_cbar.png'
                dout = os.path.dirname(fout)
                if not os.path.isdir(dout):
                    os.system(f'mkdir -p {dout}')
                fig1.savefig(fout, dpi=50, transparent=True)
                os.system(f'convert -transparent white -trim {fout} {fout}.png; /bin/mv {fout}.png {fout}')
                plt.close(fig1)
        
            f.close()
            lat = None; lon = None; xx = None; yy = None; data = None

        t += timedelta(days=1)

    #os.chdir(web_dir)
    #os.system('rsync -a -e "ssh -x -i /home/mpan/.ssh/id_rsa_cw3e" imgs/obs/modis cw3e@cw3e.ucsd.edu:htdocs/wrf_hydro/cnrfc/imgs/obs/')

if __name__ == '__main__':
    main(sys.argv[1:])

