''' Plot NRT WRF-Hydro results and forcing for web map overlays
    (pure data pixels only, no other map information)

Usage:
    mpirun -np [# of procs] python plot_forcing_output.py [domain] [yyyymm1] [yyyymm2] [nrt|retro|fcst/xxx] [monthly_flag]
Default values:
    [monthly_flag] is optional (left empty for daily plots or "monthly" for monthly plots)
    others must be specified
'''

__author__ = 'Ming Pan'
__email__  = 'm3pan@ucsd.edu'
__status__ = 'Development'

import sys, os, pytz
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from mpi4py import MPI
import netCDF4 as nc
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

import add_pctl_rank_monthly

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))+'/utils')
from utilities import config, find_last_time

modelid = 'nwm_v3'

mpl.use('pdf')

# MPI setup
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

def cmap_listed(cmname, vmin=0, vmax=100):
    '''listed colormaps'''

    vmin = float(vmin)
    vmax = float(vmax)

    if cmname=='smtot_r':
        bounds = np.array([0, 2, 5, 10, 20, 30, 70, 80, 90, 95, 98, 100], np.single)
        red = np.array([115, 230, 230, 254, 254, 255, 170, 76, 56, 20, 0], np.single)
        green = np.array([0, 0, 152, 211, 254, 255, 245, 230, 168, 90, 38], np.single)
        blue = np.array([0, 0, 0, 127, 0, 255, 150, 0, 0, 0, 115], np.single)
        colors = np.transpose(np.array([red/255., green/255., blue/255.]))
    elif cmname=='swe_r':
        bounds = np.array([0, 1, 5, 10, 20, 35, 65, 80, 90, 95, 99, 100], np.single)
        red = np.array([180, 255, 255, 255, 255, 255, 215, 185, 139, 93, 46], np.single)
        green = np.array([0, 46, 93, 139, 185, 232, 215, 185, 139, 93, 46], np.single)
        blue = np.array([0, 46, 93, 139, 185, 93, 255, 255, 255, 255, 180], np.single)
        colors = np.transpose(np.array([red/255., green/255., blue/255.]))
    elif cmname=='precip':
        bounds = np.array([0, 1, 2.5, 5, 7.5, 10, 15, 20, 30, 40,
                  50, 70, 100, 150, 200, 250, 300, 400, 500, 600, 750, 900], float)
        colors = np.array([(1.0, 1.0, 1.0),
             (0.3137255012989044, 0.8156862854957581, 0.8156862854957581),
             (0.0, 1.0, 1.0),
             (0.0, 0.8784313797950745, 0.501960813999176),
             (0.0, 0.7529411911964417, 0.0),
             (0.501960813999176, 0.8784313797950745, 0.0),
             (1.0, 1.0, 0.0),
             (1.0, 0.6274510025978088, 0.0),
             (1.0, 0.0, 0.0),
             (1.0, 0.125490203499794, 0.501960813999176),
             (0.9411764740943909, 0.250980406999588, 1.0),
             (0.501960813999176, 0.125490203499794, 1.0),
             (0.250980406999588, 0.250980406999588, 1.0),
             (0.125490203499794, 0.125490203499794, 0.501960813999176),
             (0.125490203499794, 0.125490203499794, 0.125490203499794),
             (0.501960813999176, 0.501960813999176, 0.501960813999176),
             (0.8784313797950745, 0.8784313797950745, 0.8784313797950745),
             (0.9333333373069763, 0.8313725590705872, 0.7372549176216125),
             (0.8549019694328308, 0.6509804129600525, 0.47058823704719543),
             (0.6274510025978088, 0.42352941632270813, 0.23529411852359772),
             (0.4000000059604645, 0.20000000298023224, 0.0)], float)
    elif cmname=='tair2m':
        bounds = np.linspace(-12, 39, 18)
        colors = np.array([plt.cm.rainbow(1/17.0*i)[:3] for i in range(18)], float)
    # monthly variables
    elif cmname=='precip_r':
        bounds = np.array([0, 1, 5, 10, 20, 35, 65, 80, 90, 95, 99, 100], np.single)
        colors = np.array([plt.cm.PuOr(1/(bounds.size-1)*i)[:3] for i in range(bounds.size)], float)
        colors[5, :] = colors[5, :]*0+1
    elif cmname=='tair2m_r':
        bounds = np.array([0, 1, 5, 10, 20, 35, 65, 80, 90, 95, 99, 100], np.single)
        colors = np.array([plt.cm.coolwarm(1/(bounds.size-1)*i)[:3] for i in range(bounds.size)], float)
        colors[5, :] = colors[5, :]*0+1

    ticks = bounds
    bounds = (vmax - vmin) / (bounds.max()-bounds.min()) * bounds + vmin
    norm = mpl.colors.BoundaryNorm(bounds-vmin, colors.shape[0])
    cmap = mpl.colors.ListedColormap(colors, cmname)
    cmap.set_bad('white')

    return cmap, norm, ticks


## main function
def main(argv):
    
    '''main loop'''

    domain = argv[0]
    
    time1 = datetime.strptime(argv[1], '%Y%m')
    time2 = datetime.strptime(argv[2], '%Y%m')

    ptype = argv[3]
    
    time1 = time1.replace(tzinfo=pytz.utc)
    time2 = time2.replace(tzinfo=pytz.utc)
    step  = relativedelta(months=1)

    webdir = f'{config["base_dir"]}/web/imgs/{domain}/{ptype}'
    nrtdir = f'{config["base_dir"]}/{modelid}/{domain}/{ptype}'
    
    os.chdir(nrtdir)
   
    if len(argv)>4: 
        monthly_flag = True
    else:
        monthly_flag = False
    
    alltimes = []
    t = time1
    while t <= time2:
        alltimes.append(t)
        t += step

    map_bounds = np.array(config[modelid][domain]['lonlatbox'], dtype=float)/180*np.pi
    lonlim = map_bounds[:2]
    latlim = np.log(np.tan(map_bounds[2:]/2+np.pi/4))
    figsize = (16, 16)
    dpi = config[modelid][domain]['mapdpi']

    plt.ioff()
    
    # read lat/lon
    fndom = f'{config["base_dir"]}/{modelid}/{domain}/domain/wrfinput_{domain}.nc'
    fdom = nc.Dataset(fndom, 'r')
    lons = np.squeeze(fdom.variables['XLONG'][:])/180*np.pi
    lats = np.squeeze(fdom.variables['XLAT'][:])/180*np.pi
    xland = np.squeeze(fdom.variables['XLAND'][:])
    xx = lons
    yy = np.log(np.tan(lats/2+np.pi/4))
    fdom.close()

    if domain=='conus':
        fnmask = f'{config["base_dir"]}/{modelid}/{domain}/domain/xmask0_us.nc'
        fmask = nc.Dataset(fnmask, 'r')
        xmask = np.squeeze(fmask.variables['XLAND'][:])
        print('CONUS pixel count before masking: %d' % (xland<1.5).sum())
        xland[xmask.mask] = 2
        print('CONUS pixel count after  masking: %d' % (xland<1.5).sum())
        fmask.close()
        figsize = (16, 12)

    for t in alltimes[rank::size]:
        
        # SMTOT and SWE
        fn = f'{nrtdir}/output/1km_daily/{t:%Y/%Y%m}.LDASOUT_DOMAIN1.daily'
        f = nc.Dataset(fn, 'r')
        
        ncvars = ['SOIL_M_r', 'SNEQV_r']
        imvars = ['smtot_r', 'swe_r']
        
        for i,d in enumerate(f.variables['time'][:]):
            if monthly_flag:
                break
            
            dd = nc.num2date(d, f.variables['time'].units)
            print(dd.strftime('%Y-%m-%d'))
            
            for j,v in enumerate(ncvars):

                data = np.squeeze(f.variables[v][i, :, :])
                data = np.ma.masked_where(xland>1.5, data)
                
                if v=='SNEQV_r':
                    sdata = np.squeeze(f.variables['SNEQV'][i, :, :])
                    data = np.ma.masked_where(sdata<10, data)
    
                fig1 = plt.figure(figsize=figsize)
                ax1 = fig1.add_axes([0,0,1,1])
        
                cmap, norm, ticks = cmap_listed(imvars[j], 0, 100)
                ax1.pcolormesh(xx, yy, data, shading='auto', cmap=cmap, norm=norm)
                ax1.set_xlim(lonlim)
                ax1.set_ylim(latlim)
                ax1.axis('off')
                fout = f'{webdir}/output/{dd:%Y}/{imvars[j]}_{dd:%Y%m%d}.png'
                dout = os.path.dirname(fout)
                if not os.path.isdir(dout):
                    os.system(f'mkdir -p {dout}')
                fig1.savefig(fout, dpi=dpi, transparent=True)
                os.system(f'magick {fout} -transparent white {fout}.png; /bin/mv {fout}.png {fout}')
                fig1.clf()
                plt.close(fig1)
                
        f.close()
                
        # precipitation and temperature        
        fn = f'{nrtdir}/forcing/1km_daily/{t:%Y/%Y%m}.LDASIN_DOMAIN1.daily'
        f = nc.Dataset(fn, 'r')
        
        ncvars = ['RAINRATE', 'T2D']
        imvars = ['precip', 'tair2m']
        nmvars = ['Precipitation (mm/day)', 'Air Temperature (C)']
        
        for i,d in enumerate(f.variables['time'][:]):
            if monthly_flag:
                break
            
            dd = nc.num2date(d, f.variables['time'].units)
            print(f'{dd:%Y-%m-%d}')
            
            for j,v in enumerate(ncvars):

                data = np.squeeze(f.variables[v][i, :, :])
                data = np.ma.masked_where(xland>1.5, data)
                
                if v=='RAINRATE':
                    data *= 86400
                    cmap, norm, ticks = cmap_listed(imvars[j], 0, 900)
                elif v=='T2D':
                    data -= 273.15
                    cmap, norm, ticks = cmap_listed(imvars[j], -12, 39)
    
                fig1 = plt.figure(figsize=figsize)
                ax1 = fig1.add_axes([0,0,1,1])
        
                ax1.pcolormesh(xx, yy, data, shading='auto', cmap=cmap, norm=norm)
                ax1.set_xlim(lonlim)
                ax1.set_ylim(latlim)
                ax1.axis('off')
                fout = f'{webdir}/forcing/{dd:%Y}/{imvars[j]}_{dd:%Y%m%d}.png'
                dout = os.path.dirname(fout)
                if not os.path.isdir(dout):
                    os.system(f'mkdir -p {dout}')
                fig1.savefig(fout, dpi=dpi, transparent=True)
                os.system(f'magick {fout} -transparent white {fout}.png; /bin/mv {fout}.png {fout}')
                fig1.clf()
                plt.close(fig1)
                
                if i==-1 and t==time1:
                    fig1 = plt.figure(figsize=figsize)
                    ax1 = fig1.add_axes([0.1,0.1,0.8,0.8])
        
                    data = np.ma.masked_where(xland<100000000000, data)
                    cs = ax1.pcolormesh(xx, yy, data, shading='auto', cmap=cmap, norm=norm)
                    ax1.set_xlim(lonlim)
                    ax1.set_ylim(latlim)
                    ax1.axis('off')
                    cbar = plt.colorbar(cs, orientation='vertical', fraction=0.03, pad=0.02, ticks=ticks)
                    cbar.set_label(nmvars[j], fontsize=24)
                    cbar.ax.tick_params(labelsize=20)
                    if v=='RAINRATE':
                        cbar.ax.set_yticklabels(['0', '1', '2.5', '5', '7.5', '10', '15', '20', '30', '40', '50', '70', '100', '150', '200', '250', '300', '400', '500', '600', '750', '900'])
                    fout = f'{webdir}/forcing/{imvars[j]}_cbar.png'
                    dout = os.path.dirname(fout)
                    if not os.path.isdir(dout):
                        os.system(f'mkdir -p {dout}')
                    fig1.savefig(fout, dpi=50, transparent=True)
                    os.system(f'magick {fout} -transparent white {fout}.png; /bin/mv {fout}.png {fout}')
                    fig1.clf()
                    plt.close(fig1)

        f.close()
    
        if monthly_flag:
            fn = f'{nrtdir}/forcing/1km_monthly/{t:%Y/%Y%m}.LDASIN_DOMAIN1.monthly'
            f = nc.Dataset(fn, 'r')
        
            ncvars = ['RAINRATE_r', 'T2D_r']
            imvars = ['precip_r', 'tair2m_r']
            nmvars = ['Precipitation Percentile', 'Air Temperature Percentile']
            
            dd = nc.num2date(f.variables['time'][0], f.variables['time'].units)
            print(dd.strftime('%Y-%m'))
            
            i = 0
            for j,v in enumerate(ncvars):

                data = np.squeeze(f.variables[v][i, :, :])
                data = np.ma.masked_where(xland>1.5, data)
                
                cmap, norm, ticks = cmap_listed(imvars[j], 0, 100)
    
                fig1 = plt.figure(figsize=figsize)
                ax1 = fig1.add_axes([0,0,1,1])
        
                ax1.pcolormesh(xx, yy, data, shading='auto', cmap=cmap, norm=norm)
                ax1.set_xlim(lonlim)
                ax1.set_ylim(latlim)
                ax1.axis('off')
                fout = f'{webdir}/forcing/{dd:%Y}/{imvars[j]}_{dd:%Y%m}.png'
                dout = os.path.dirname(fout)
                if not os.path.isdir(dout):
                    os.system(f'mkdir -p {dout}')
                fig1.savefig(fout, dpi=dpi, transparent=True)
                os.system(f'magick {fout} -transparent white {fout}.png; /bin/mv {fout}.png {fout}')
                fig1.clf()
                plt.close(fig1)
                
                if i==0 and t==time1:
                    fig1 = plt.figure(figsize=figsize)
                    ax1 = fig1.add_axes([0.1,0.1,0.8,0.8])
        
                    data = np.ma.masked_where(xland<100000000000, data)
                    cs = ax1.pcolormesh(xx, yy, data, shading='auto', cmap=cmap, norm=norm)
                    ax1.set_xlim(lonlim)
                    ax1.set_ylim(latlim)
                    ax1.axis('off')
                    cbar = plt.colorbar(cs, orientation='vertical', fraction=0.03, pad=0.02, ticks=ticks)
                    cbar.set_label(nmvars[j], fontsize=24)
                    cbar.ax.tick_params(labelsize=20)
                    fout = f'{webdir}/forcing/{imvars[j]}_cbar.png'
                    dout = os.path.dirname(fout)
                    if not os.path.isdir(dout):
                        os.system(f'mkdir -p {dout}')
                    fig1.savefig(fout, dpi=50, transparent=True)
                    os.system(f'magick {fout} -transparent white -trim {fout}.png; /bin/mv {fout}.png {fout}')
                    fig1.clf()
                    plt.close(fig1)

    comm.Barrier()
    return 0


if __name__ == '__main__':
    main(sys.argv[1:])

