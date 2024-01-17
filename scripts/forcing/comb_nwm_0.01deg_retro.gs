function comb(args)

time1=subwrd(args, 1)
time2=subwrd(args, 2)

basedir=".."

* time step size,  hours
step=1

* flag to shift time stamp by 1 hour:
* All input products have the file name time-stamped at the end of the hour,
* i.e. the 01z file has the mean flux/accumulation from 00z to 01z
* Here we re-stamp it with the starting time of the accumulation period, i.e. 1-hour shift backward
*flag_shift=1
flag_shift=0

* flag to rescale Stage IV daily total P and to offset mean T against PRISM
* we do it when PRISM "recent history" version is available
prismp_flag=1
prismt_flag=1

* flag to enable/disable interpolation of downward longwave from 3-hourly to hourly
flag_dlitp=1

* some constants
lapse=-6.5/1000
g=9.81
ra=286.9
rv=455
sb=5.67e-8
pi=3.1415926535897384626

mindaily=0.4
minhourly=0.2

* some domain parameters
lat1=25
lat2=53
lon1=-125
lon2=-67

resdhi=0.01
resdme=0.04
resd12=0.125
buff=4

rehi=mkre(lat1%" "%lat2%" "%lon1%" "%lon2%" "%resdhi)
reme=mkre(lat1%" "%lat2%" "%lon1%" "%lon2%" "%resdme)
relo=mkre(lat1%" "%lat2%" "%lon1%" "%lon2%" "%resd12)

say "Hires configuration: "rehi
say "Meres configuration: "reme
say "Lores configuration: "relo

'xdfopen domain/wrfinput.d01.lat-lon.modis.ctl'
'xdfopen domain/wrfinput.d01.lat-lon.usgs.ctl'
'sdfopen domain/pfconus2/CONUS2.0.Final1km.NWM.Mask.0.01deg.nc'

'set lat 'lat1+resdhi/2' 'lat2-resdhi/2
'set lon 'lon1+resdhi/2' 'lon2-resdhi/2

'define msks1=const(maskout(1, 1.5-xland.1), 0, -u)'
'define msks2=const(maskout(1, 1.5-xland.2), 0, -u)'
'define msks3=const(band1.3(t=1), 0, -u)'
'define msks=maskout(1, msks1+msks2+msks3-0.5)'
'define msks=nfill(msks, lat, 2)'

'close 3'
'close 2'
'close 1'

'open domain/gtopo30_nldas.ctl'
'define demhi=re(const(dem, 0, -u), 'rehi', ba)'
'define demme=re(const(dem, 0, -u), 'reme', ba)'
'define demlo=re(const(dem, 0, -u), 'relo', ba)'

'close 1'

year=substr(time1, strlen(time1)-3, 4)

* NLDAS-2 as the backbone
if (year<2023)
    'xdfopen 'basedir'/nldas2/nldas2_retro.ctl'
else
    'xdfopen 'basedir'/nldas2/nldas2_nrt.ctl'
endif

* Gap filled Stage IV
'xdfopen 'basedir'/stage4/st4nl2.ctl'
'xdfopen 'basedir'/stage4/st4nl2_daily.ctl'

* PRISM
'xdfopen 'basedir'/prism/recent/prism_ppt_recent.ctl'
'xdfopen 'basedir'/prism/recent/prism_tmean_recent.ctl'

'set lat 'lat1+resdhi/2' 'lat2-resdhi/2
'set lon 'lon1+resdhi/2' 'lon2-resdhi/2

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

lstdatep=0
lstdatet=0
debug=0

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
    
    say "Input file timestamp "tstamp1"; Forcing file timestamp "tstamp
    
*   For precipitation, use Stage IV-II gap-fill with NLDAS-2
*   then rescaled to match PRISM

    'define st42me=nfill(ave(apcpsfc.2, t='t', t='t+step-1'),lat.2,'buff')'

    if (prismp_flag=1)
    
*       determine the date, for precipitation, 00z is yesterday, so we use tstamp2
        curdatep = substr(tstamp2, 1, 8)
    
        if (curdatep!=lstdatep)

            dstr = gradsdate(curdatep)
*            say dstr
        
            'define st42med=apcpsfc.3(time='dstr')'
            'define prsmmed=re(ppt.4(time='dstr'), 'reme')'
            
*           we do not rescale if Stage IV is zero or below certain daily minimum because (1) impossible to rescale zero (2) scaling factor may be too high
            'define fscme=maskout(prsmmed/st42med, st42med-'mindaily')'
            'define fscme=const(fscme, 1, -u)'
        
            'set gxout stat'
            'd fscme'
            line=sublin(result, 8)
            fmax=subwrd(line, 5)
            say curdatep%": maximum rescaling factor = "%fmax
            'set gxout fwrite'
        
            lstdatep = curdatep
        endif
    
        'define st42me=st42me*fscme'
    
    endif
    
    'define st42hi=re(st42me, 'rehi', bl)'
    'define pcphi=st42hi/3600'
    'undefine st42hi'

*   For temperature, use NLDAS-2, then offset to match PRISM

    'define tmplo=ave(tmp2m.1, t='t', t='t+step-1')'
    'define tmpls=tmplo-demlo*('lapse')'

    if (prismt_flag=1)
    
*       determine the date, for temperature, 00z is today, so we use tstamp1
        curdatet = substr(tstamp1, 1, 8)
    
        if (curdatet!=lstdatet)

            dstr = gradsdate(curdatet)
*            say dstr
        
            'define tnl2lod=ave(tmp2m.1, time=00z'dstr', time=23z'dstr')'
            'define tnl2lsd=tnl2lod-demlo*('lapse')'
            'define tnl2msd=re(nfill(tnl2lsd,lat,'buff'), 'reme', bl)'
            'define tnl2med=tnl2msd+demme*('lapse')'
            'define tprsmed=re(tmean.5(time='dstr')+273.15, 'reme')'
*           we do not rescale if Stage IV is zero or below certain daily minimum because (1) impossible to rescale zero (2) scaling factor may be too high
            'define offme=const(tprsmed-tnl2med, 0, -u)'
        
            'set gxout stat'
            'd abs(offme)'
            line=sublin(result, 8)
            fmax=subwrd(line, 5)
            say curdatet%": maximum offset magnitude = "%fmax
            'set gxout fwrite'
        
            lstdatet = curdatet
        endif

        'define tmpms=re(nfill(tmpls,lat,'buff'), 'reme', bl)'
        'define tmpms=tmpms+offme'
        'define tmphs=re(nfill(tmpms,lat.2,'buff'), 'rehi', bl)'
    
    else
    
        'define tmphs=re(nfill(tmpls,lat,'buff'), 'rehi', bl)'
    
    endif
    
    'define tmphi=tmphs+demhi*('lapse')'

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
    
    'define prslo=ave(pressfc.1, t='t', t='t+step-1')'
    'define prsls=prslo*exp('g/ra'*demlo/((tmplo+tmpls)/2))'
    
    'define sfhlo=ave(spfh2m.1, t='t', t='t+step-1')'
    'define pvalo=sfhlo*prslo/(0.622+0.378*sfhlo)'
    'define pvslo=6.112*100*exp(17.67*(tmplo-273.15)/(tmplo-273.15+243.5))'
    'define rlhlo=pvalo/pvslo'

    if (flag_dlitp=1&step=1)
*       interpolate 3-hour downward longwave to hourly
        hh=math_mod(t-1, 3)
*        say hh
        if (hh=0)
            'define dlwlo=dlwrfsfc.1'
        endif
        if (hh=1)
            'define dlwnx=dlwrfsfc.1(t='t+2')'
            'define dlwlo=dlwrfsfc.1/3*2+dlwnx/3'
        endif
        if (hh=2)
            'define dlwnx=dlwrfsfc.1(t='t+1')'
            'define dlwlo=dlwrfsfc.1/3+dlwnx/3*2'
        endif
    else
      'define dlwlo=ave(dlwrfsfc.1, t='t', t='t+step-1')'
    endif

    'define emvlo=1.08*(1-exp(-pow(pvalo/100,tmplo/2016)))'
    'define emilo=emvlo*'sb'*pow(tmplo, 4)'
    'define remlo=dlwlo/emilo'
    
    'define prshs=re(nfill(prsls,lat,'buff'), 'rehi', bl)'
    'define rlhhi=re(nfill(rlhlo,lat,'buff'), 'rehi', bl)'
    'define remhi=re(nfill(remlo,lat,'buff'), 'rehi', bl)'
    
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
   
    'define dswlo=ave(dswrfsfc.1, t='t', t='t+step-1')'
    
*   Simple interpolation for shortwave and wind speed
    'define dswhi=re(nfill(dswlo,lat,'buff'), 'rehi', bl)'
    
    'define wsulo=ave(ugrd10m.1, t='t', t='t+step-1')'
    'define wsuhi=re(nfill(wsulo,lat,'buff'), 'rehi', bl)'
    'define wsvlo=ave(vgrd10m.1, t='t', t='t+step-1')'
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
    fdir="0.01deg/"%curryear%"/"%curryear%currmonn
    '!mkdir -p 'fdir
    fout=fdir%"/"tstamp%".LDASIN_DOMAIN1"

    v=1
    while (v<=8)
       
        vi=subwrd(vars_vic, v)
        vs=subwrd(vars_short, v)
        vup=subwrd(vars_upper, v)
        vc=subwrd(vars_cf, v)
        vu=subwrd(units, v)
       
        'define 'vs'=nfill('vi', msks, 30)*msks'
*        'define 'vs'='vi'*msks'
        
        'clear sdfwrite'

        say "set sdfattr "vs" String units "vu

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

function gradsdate(args)

    curdatep=subwrd(args, 1)
    
    monnums = "01 02 03 04 05 06 07 08 09 10 11 12"
    monstrs = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC"
    curryear=substr(curdatep, 1, 4)
    currmonn=substr(curdatep, 5, 2)
    currday=substr(curdatep, 7, 2)
    i=1
    while (i<=12)
        monn=subwrd(monnums, i)
        if (currmonn=monn)
            currmon=subwrd(monstrs, i)
        endif
        i=i+1
    endwhile
    dstr = currday%currmon%curryear
    
return dstr
