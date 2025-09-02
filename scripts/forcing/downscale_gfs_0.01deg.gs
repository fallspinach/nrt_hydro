* GrADS script to downscale forcing data to 0.01 deg based on elevation
* Usage: opengrads -lbc "downscale_gfs_0.01deg.gs [time1] [time2] [fctl] [lon1] [lon2] [lat1] [lat2] [outpath]"
*
* author: Ming Pan
* email: m3pan@ucsd.edu
* status: Development

function downscale(args)

time1=subwrd(args, 1)
time2=subwrd(args, 2)
fctl=subwrd(args, 3)
lon1=subwrd(args, 4)
lon2=subwrd(args, 5)
lat1=subwrd(args, 6)
lat2=subwrd(args, 7)
outpath=subwrd(args, 8)

basedir=".."

* time step size,  hours
step=1

* flag to shift time stamp by 1 hour:
* All input products have the file name time-stamped at the end of the hour,
* i.e. the 01z file has the mean flux/accumulation from 00z to 01z
* Here we re-stamp it with the starting time of the accumulation period, i.e. 1-hour shift backward
flag_shift=0

* flag to enable/disable interpolation of downward longwave from 3-hourly to hourly
flag_dlitp=0

* some constants
lapse=-6.5/1000
g=9.81
ra=286.9
rv=455
sb=5.67e-8
pi=3.1415926535897384626

* rounding to nearest GFS grid boundaries
gfslon1=math_nint(lon1*4)/4-0.125
gfslon2=math_nint(lon2*4-0.0000001)/4+0.125
gfslat1=math_nint(lat1*4)/4-0.125
gfslat2=math_nint(lat2*4-0.0000001)/4+0.125

nwmlon1=math_nint(lon1*100-0.5)/100
nwmlon2=math_nint(lon2*100+0.49999999)/100
nwmlat1=math_nint(lat1*100-0.5)/100
nwmlat2=math_nint(lat2*100+0.49999999)/100

say "GFS domain rounded to: "gfslon1", "gfslon2", "gfslat1", "gfslat2
say "NWM domain rounded to: "nwmlon1", "nwmlon2", "nwmlat1", "nwmlat2

resdlo=0.25
resdhi=0.0125
resdnw=0.01
buff=4

rehi=mkre(gfslat1%" "%gfslat2%" "%gfslon1%" "%gfslon2%" "%resdhi)
relo=mkre(gfslat1%" "%gfslat2%" "%gfslon1%" "%gfslon2%" "%resdlo)
renw=mkre(nwmlat1%" "%nwmlat2%" "%nwmlon1%" "%nwmlon2%" "%resdnw)

say "Hires configuration:    "rehi
say "Lores configuration:    "relo
say "NWM mask configuration: "renw

'xdfopen domain/wrfinput.d01.lat-lon.modis.ctl'
'xdfopen domain/wrfinput.d01.lat-lon.usgs.ctl'
'sdfopen domain/pfconus2/CONUS2.0.Final1km.NWM.Mask.0.01deg.nc'

'set lat 'nwmlat1+resdnw/2' 'nwmlat2-resdnw/2
'set lon 'nwmlon1+resdnw/2' 'nwmlon2-resdnw/2

'define msks1=const(maskout(1, 1.5-xland.1), 0, -u)'
'define msks2=const(maskout(1, 1.5-xland.2), 0, -u)'
'define msks3=const(band1.3(t=1), 0, -u)'
'define msks=maskout(1, msks1+msks2+msks3-0.5)'
'define msks=nfill(msks, lat, 2)'

'close 3'
'close 2'
'close 1'

'open domain/gtopo30_nldas.ctl'
'set lat 'gfslat1+resdhi/2' 'gfslat2-resdhi/2
'set lon 'gfslon1+resdhi/2' 'gfslon2-resdhi/2
'define demhi=re(const(dem, 0, -u), 'rehi', ba)'
'define demlo=re(const(dem, 0, -u), 'relo', ba)'
'close 1'

* open the input forcing ctl file
'xdfopen 'fctl

* 
'set lat 'gfslat1+resdhi/2' 'gfslat2-resdhi/2
'set lon 'gfslon1+resdhi/2' 'gfslon2-resdhi/2

if (time1=time2)
    'set time 'time1
    'q dim'
    line=sublin(result, 5)
    t1=subwrd(line, 9)
    t2=t1
else
    'set time 'time1' 'time2
    'q dim'
    line=sublin(result, 5)
    t1=subwrd(line, 11)
    t2=subwrd(line, 13)
endif

t=t1
while (t<=t2)
    
    'set t 't-1
    tstamp2=dtime()
    
    'set t 't
    tstamp1=dtime()
    
    if (flag_shift=1)
        tstamp=tstamp2
    else
        tstamp=tstamp1
    endif
    
    hh=math_mod(t-1, 6)
    
    nflxlos="tmp prs sfh wsu wsv"
    nflxins="tmp2m pressfc spfh2m ugrd10m vgrd10m"
    fluxlos="pcp dlw dsw"
    fluxins="pratesfc dlwrfsfc dswrfsfc"
    
    tlast=math_int(t/3)*3
    tnext=tlast+3
    if (tnext>384)
        tnext=384
    endif

    tlastflx=math_int((t-1)/3)*3
    tnextflx=tlastflx+3
    if (tnextflx>384)
        tnextflx=384
    endif
    
    say "Input file timestamp "tstamp1"; Forcing file timestamp "tstamp"; timestep = "t
    if (t<=120)
        say "Lead time <= 5 days (t<=120), hourly fcst steps: flux variables (pratesfc, dlwrfsfc, dswrfsfc) accumulating from "hh+1" hour(s) ago"
    else
        say "Lead time > 5 days (t>120), 3-hourly fcst steps: nonflux variables interpolated between t="tlast" and t="tnext", flux variables remain constant between t="tlastflx+1" and t="tnextflx
    endif

*   non-flux variables
    v=1
    while (v<=5)
        vlo=subwrd(nflxlos, v)
        vin=subwrd(nflxins, v)
        if (t<=120)
*           hourly output steps
            'define 'vlo'lo='vin'(t='t')'
        else
*           3-hourly output steps
            w1=(tnext-t)/3
            w2=(t-tlast)/3
*            say "define "vlo"lo="vin"(t="tlast")*"w1"+"vin"(t="tnext")*"w2
            'define 'vlo'lo='vin'(t='tlast')*'w1'+'vin'(t='tnext')*'w2
        endif
        v=v+1
    endwhile
    
*   flux variables
    v=1
    while (v<=3)
        vlo=subwrd(fluxlos, v)
        vin=subwrd(fluxins, v)
        if (t<=120)
*           hourly output steps
            if (hh=0)
                'define 'vlo'lo='vin'(t='t')'
            else
                'define 'vlo'lo='vin'(t='t')*'hh+1'-'vin'(t='t-1')*'hh
            endif
        else
*           3-hourly output steps    
            if (hh<3)
*                say "define "vlo"lo="vin"(t="tnextflx")"
                'define 'vlo'lo='vin'(t='tnextflx')'
            else
*                say "define "vlo"lo="vin"(t="tnextflx")*2-"vin"(t="tlastflx")"
                'define 'vlo'lo='vin'(t='tnextflx')*2-'vin'(t='tlastflx')'
            endif
        endif
        v=v+1
    endwhile
    
*   For precipitation, simple interpolation
    'define pcphi=re(pcplo, 'rehi', bl)'
    
*   For temperature, pressure, humidiy, and longwave, the procedure is:
*   1) Adjust temperature and pressure to mean sea level at low resolution
*   2) Calculate relative humidity and vapor pressure at low resolution
*   3) Calculate longwave emissivity and Stephan-Boltzman emission from vapor pressure and temperature at low resolution
*   4) Calculate the ratio between longwave and Stephan-Boltzman emission at low resolution
*   5) Interpolate temperature (sea level), pressure (sea level), relative humidity, and longwave emission ratio to high resolution
*   6) Re-adjust temperature, pressure to actual elevation at high resolution
*   7) Calculate specific humidity/vapor pressure from new temperature, pressure, and relative humidity at high resolution
*   8) Calculate longwave emissivity and Stephan-Boltzman emission from vapor pressure and temperature at high resolution
*   9) Calculate longwave from emission ratio and Stephan-Boltzman emission at high resolution
    
    'define tmpls=tmplo-demlo*('lapse')'
    
    'define prsls=prslo*exp('g/ra'*demlo/((tmplo+tmpls)/2))'
    
    'define pvalo=sfhlo*prslo/(0.622+0.378*sfhlo)'
    'define pvslo=6.112*100*exp(17.67*(tmplo-273.15)/(tmplo-273.15+243.5))'
    'define rlhlo=pvalo/pvslo'

    'define emvlo=1.08*(1-exp(-pow(pvalo/100,tmplo/2016)))'
    'define emilo=emvlo*'sb'*pow(tmplo, 4)'
    'define remlo=dlwlo/emilo'
    
    'define tmphs=re(nfill(tmpls,lat,'buff'), 'rehi', bl)'
    'define prshs=re(nfill(prsls,lat,'buff'), 'rehi', bl)'
    'define rlhhi=re(nfill(rlhlo,lat,'buff'), 'rehi', bl)'
    'define remhi=re(nfill(remlo,lat,'buff'), 'rehi', bl)'
    
    'define tmphi=tmphs+demhi*('lapse')'
    'define prshi=prshs/exp('g/ra'*demhi/((tmphi+tmphs)/2))'

    'undefine tmphs'
    'undefine prshs'
    
    'define pvshi=6.112*100*exp(17.67*(tmphi-273.15)/(tmphi-273.15+243.5))'
    'define pvahi=pvshi*rlhhi'
    'define sfhhi=0.622*pvahi/(prshi-0.378*pvahi)'

    'undefine rlhhi'
    'undefine pvshi'
    
    'define emvhi=1.08*(1-exp(-pow(pvahi/100,tmphi/2016)))'
    'define emihi=emvhi*'sb'*pow(tmphi, 4)'
    'define dlwhi=emihi*remhi'

    'undefine remhi'
    'undefine emvhi'
    'undefine emihi'
    'undefine pvahi'
   
*   Simple interpolation for shortwave and wind speed
    'define dswhi=re(nfill(dswlo,lat,'buff'), 'rehi', bl)'
    'define wsuhi=re(nfill(wsulo,lat,'buff'), 'rehi', bl)'
    'define wsvhi=re(nfill(wsvlo,lat,'buff'), 'rehi', bl)'
    
*   Output in WRF-Hydro units:
*   T2D [K], Q2D [1], PSFC [Pa], U2D [m/s], V2D [m/s], SWDOWN [W/m^2], LWDOWN [W/m^2], RAINRATE [kg/m^2/s]
    vars_vic="tmphi sfhhi prshi wsuhi wsvhi dswhi dlwhi pcphi"
    vars_upper="T2D Q2D PSFC U2D V2D SWDOWN LWDOWN RAINRATE"
    vars_short="t2d q2d psfc u2d v2d swdown lwdown rainrate"
    vars_long.1="Air Temperature"
    vars_long.2="Specific Humidity"
    vars_long.3="Pressure"
    vars_long.4="U Wind"
    vars_long.5="V Wind"
    vars_long.6="Downward Shortwave Radiation"
    vars_long.7="Downward Longwave Radiation"
    vars_long.8="Precipitation"
    vars_cf="air_temperature specific_humidity air_pressure eastward_wind northward_wind surface_downwelling_shortwave_flux_in_air surface_downwelling_longwave_flux_in_air precipitation_flux"
    units="K 1 Pa m/s m/s W/m^2 W/m^2 kg/m^2/s"

    curryear=substr(tstamp, 1, 4)
    currmonn=substr(tstamp, 5, 2)
    fdir=outpath%"/"%curryear%"/"%curryear%currmonn
    '!mkdir -p 'fdir
    fout=fdir%"/"tstamp%".LDASIN_DOMAIN1"

'set lat 'nwmlat1+resdnw/2' 'nwmlat2-resdnw/2
'set lon 'nwmlon1+resdnw/2' 'nwmlon2-resdnw/2

    v=1
    while (v<=8)
       
        vi=subwrd(vars_vic, v)
        vs=subwrd(vars_short, v)
        vup=subwrd(vars_upper, v)
        vc=subwrd(vars_cf, v)
        vu=subwrd(units, v)
       
*        'define 'vs'=nfill('vi', msks, 30)*msks'
        'define 'vs'=re('vi', 'renw', bl)'
        'define 'vs'='vs'*msks'
        
        'clear sdfwrite'

*        say "set sdfattr "vs" String units "vu

        'set sdfattr 'vs' String units 'vu
        'set sdfattr 'vs' String long_name 'vars_long.v
        'set sdfattr 'vs' String standard_name 'vc
        
        'set sdfwrite -flt -nc4 -3dt tmp_'tstamp'_'v'.nc'
        'sdfwrite 'vs

        '!ncrename -h -v 'vs','vup' tmp_'tstamp'_'v'.nc'

        'undefine 'vi
        'undefine 'vs
        
        v=v+1
    endwhile

    '!cdo --history -O -f nc4 -z zip merge tmp_'tstamp'_?.nc 'fout
    '!/bin/rm tmp_'tstamp'_?.nc'

*    return
    if (debug=1)
      
        check1()
        'gxyat -x 1600 -y 1200 check_tmp_prs_'resdhi'd.png'
        
        check2()
        'gxyat -x 1600 -y 1200 check_sfh_dlw_'resdhi'd.png'
        
        return
        
    endif
 
    'c'   
    t=t+step
endwhile

'quit'

return

***********************************
* function to create regridding parameters

function mkre(args)

lat1=subwrd(args, 1)
lat2=subwrd(args, 2)
lon1=subwrd(args, 3)
lon2=subwrd(args, 4)
resd=subwrd(args, 5)

nrows=(lat2-lat1)/resd
ncols=(lon2-lon1)/resd

repar=ncols", linear, "lon1+resd/2", "resd", "nrows", linear, "lat1+resd/2", "resd

return repar

*************************************
* function to display time string


function dtime()

  'q time'
  gradst=subwrd(result, 3)
  
  zflag=substr(gradst, 3, 1)
  
  if (zflag="Z")
    year=substr(gradst, 9, 4)
    mon=substr(gradst, 6, 3)
    day=substr(gradst, 4, 2)
    hour=substr(gradst, 1, 2)
    min="00"
  else
    year=substr(gradst, 12, 4)
    mon=substr(gradst, 9, 3)
    day=substr(gradst, 7, 2)
    hour=substr(gradst, 1, 2)
    min=substr(gradst, 4, 2)
  endif
  
  monstrs="JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC"
  
  i=1
  while (i<=12)
    monstr=subwrd(monstrs, i)
    if (mon=monstr)
      m=i
    endif
    i=i+1
  endwhile
  
  if (m<10)
    mo="0"%m
  else
    mo=m
  endif
    
  dt=year%"-"%mo%"-"%day%":"hour
  dt=year%mo%day%hour
  
return dt

*************************************
* function to display time string


function dtime2()

  'q time'
  gradst=subwrd(result, 3)
  
  zflag=substr(gradst, 3, 1)
  
  if (zflag="Z")
    year=substr(gradst, 9, 4)
    mon=substr(gradst, 6, 3)
    day=substr(gradst, 4, 2)
    hour=substr(gradst, 1, 2)
    min="00"
    dmy=substr(gradst, 4, 9)
  else
    year=substr(gradst, 12, 4)
    mon=substr(gradst, 9, 3)
    day=substr(gradst, 7, 2)
    hour=substr(gradst, 1, 2)
    min=substr(gradst, 4, 2)
    dmy=substr(gradst, 7, 9)
  endif
  
  monstrs="JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC"
  
  i=1
  while (i<=12)
    monstr=subwrd(monstrs, i)
    if (mon=monstr)
      m=i
    endif
    i=i+1
  endwhile
  
  if (m<10)
    mo="0"%m
  else
    mo=m
  endif
    
  dt=year%"-"%mo%"-"%day%":"hour
  dt=year%" "%mo%" "%day%" "%hour%" "%dmy
  
return dt



function check1()

'c'

rc=gsfallow("on")
panels("2 2")

'set lat 36 39'
'set lon -115 -111'
'set gxout grfill'
'set mpdset hires'

_vpg.1
'set clevs 266 268 270 272 274 276 278 280 282 284 286'
'set grads off'
'd tmplo'
'draw title 2m Air Temperature (K)'

_vpg.2
'set clevs 266 268 270 272 274 276 278 280 282 284 286'
'set grads off'
'd tmphi'
'cbarn'

_vpg.3
'set clevs 70000 75000 80000 85000 90000 95000 100000'
'set grads off'
'd prslo'
'draw title Surface Pressure (Pa)'

_vpg.4
'set clevs 70000 75000 80000 85000 90000 95000 100000'
'set grads off'
'd prshi'
'cbarn'

return

function check2()

'c'

rc=gsfallow("on")
panels("2 2")

'set lat 36 39'
'set lon -115 -111'
'set gxout grfill'
'set mpdset hires'

_vpg.1
'set clevs 0.0022 0.0024 0.0026 0.0028 0.003 0.0032 0.0034 0.0036 0.0038'
'set grads off'
'd sfhlo'
'draw title 2m Specific Humidity (kg/kg)'

_vpg.2
'set clevs 0.0022 0.0024 0.0026 0.0028 0.003 0.0032 0.0034 0.0036 0.0038'
'set grads off'
'd sfhhi'
'cbarn'

_vpg.3
'set clevs 210 220 230 240 250 260 270 280 290'
'set grads off'
'd dlwlo'
'draw title Downward Longwave Radiation (w/m`a2`n)'

_vpg.4
'set clevs 210 220 230 240 250 260 270 280 290'
'set grads off'
'd dlwhi'
'cbarn'

return

