* GrADS script to merge different forcing products, perform bias correction, and elevation-based downscaling for NRT time horizon
* Usage: opengrads -lbc "comb_nwm_0.01deg_nrt_lstm.gs [time1] [time2] [flag_stg4] [flag_anal]"
*
* author: Ming Pan
* email: m3pan@ucsd.edu
* status: Development

function comb(args)

time1=subwrd(args, 1)
time2=subwrd(args, 2)
flag_stg4=subwrd(args, 3)
flag_anal=subwrd(args, 4)

* Output in LSTM units:
* Tmean [K], Tmax [K], Tmin [K], P [mm/day], Shortwave [W/m^2], Wind [m/s]
vars_vic="tmeand tmaxd tmind precipd swdownd windd"
vars_short="tmean tmax tmin precip swdown wind"
vars_long.1="Mean Temperature"
vars_long.2="Maximum Temperature"
vars_long.3="Minimum Temperature"
vars_long.4="Precipitation"
vars_long.5="Downward Shortwave"
vars_long.6="Wind Speed"
units="K K K mm/day W/m^2 m/s"

basedir=".."

* time step size,  hours
step=1

* flag to shift time stamp by 1 hour:
* All input products have the file name time-stamped at the end of the hour,
* i.e. the 01z file has the mean flux/accumulation from 00z to 01z
* Here we re-stamp it with the starting time of the accumulation period, i.e. 1-hour shift backward
*flag_shift=1
flag_shift=0

* flag to rescale Stage IV daily total against NLDAS-2
* we no longer rescale it !!
resc_flag=0

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

resdlo=0.125
buff=4

rehi=mkre(lat1%" "%lat2%" "%lon1%" "%lon2%" "%resdhi)
relo=mkre(lat1%" "%lat2%" "%lon1%" "%lon2%" "%resdlo)
if (flag_anal="hrrr")
*    relo="2503, linear, -134.095, 0.0292402, 1155, linear, 21.1405, 0.0272727"
    relo="1856, linear, -124.984375, 0.03125, 896, linear, 25.015625, 0.03125"
    flag_dlitp=0
endif

say "Hires configuration: "rehi
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
'define demlo=re(const(dem, 0, -u), 'relo', ba)'

'close 1'

year=substr(time1, strlen(time1)-3, 4)

* NLDAS-2 as the backbone
if (year<2024)
    'xdfopen 'basedir'/nldas2/nldas2_retro.ctl'
else
    if (flag_anal="hrrr")
        'xdfopen 'basedir'/hrrr/hrrr_anal.ctl'
    else
        'xdfopen 'basedir'/nldas2/nldas2_nrt.ctl'
    endif
endif

* 
'set time 'time1
'q dim'
line=sublin(result, 5)
t1=subwrd(line, 9)
'set time 20jul2020'
'q dim'
line=sublin(result, 5)
tgrb2=subwrd(line, 9)

* Merged Stage IV
if (year<2018)
*   use Stage IV diaggretated against Stage II over CN/NW RFCs before 2018-01-01
    'open 'basedir'/stage4/archive/ST4n2a_archive.ctl'
else
*   use Stage IV diaggretated against MRMS over CN/NW RFCs after 2018-01-01
    if (t1<tgrb2)
        'open 'basedir'/stage4/archive/ST4.ctl'
    else
        'xdfopen 'basedir'/stage4/st4_conus_'flag_stg4'.ctl'
    endif
endif

* skip GSIP for long-term consisency
use_gsip = 0

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

* initialize stats
'set time 'time1
v=1
while (v<=6)   
    vi=subwrd(vars_vic, v)
    if (vi="tmaxd")
        'define 'vi'=msks-10000'
    else
        if (vi="tmind")
            'define 'vi'=msks+10000'
        else
            'define 'vi'=msks*0'
        endif   
    endif
    v=v+1
endwhile

lastdate=0
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
    
*   For precipitation, use Stage IV-II (no longer rescale against NLDAS-2),
*   then gap-fill with NLDAS-2

    if (resc_flag=1)
    
*       determine the date
        currdate = substr(tstamp1, 1, 8)
        monnums = "01 02 03 04 05 06 07 08 09 10 11 12"
        monstrs = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC"
    
        if (currdate!=lastdate)
        
            curryear=substr(currdate, 1, 4)
            currmonn=substr(currdate, 5, 2)
            currday=substr(currdate, 7, 2)
            i=1
            while (i<=12)
                monn=subwrd(monnums, i)
                if (currmonn=monn)
                    currmon=subwrd(monstrs, i)
                endif
                i=i+1
            endwhile
            dstr = currday%currmon%curryear
*            say dstr
        
            'define nl2lod=ave(apcpsfc.1, time=00Z'dstr', time=23Z'dstr')'
            'define st42hid=ave(apcpsfc.2, time=00Z'dstr', time=23Z'dstr')'
            'define st42lod=re(st42hid, 'relo', bl)'
*           we do not rescale if Stage IV is zero or below certain daily minimum because (1) impossible to rescale zero (2) scaling factor may be too high
            'define fsclo=maskout(nl2lod/st42lod, st42lod-'mindaily')'
            'define fschi=re(fsclo, 'rehi', ba)'
        
            'set gxout stat'
            'd fsclo'
            line=sublin(result, 8)
            fmax=subwrd(line, 5)
            say currdate%": maximum rescaling factor = "%fmax
            'set gxout fwrite'
        
            lastdate = currdate
        endif

    endif

    if (flag_anal="hrrr")
        'define nl2lo=ave(pratesfc.1*3600, t='t', t='t+step-1')'
        'define nl2lo=const(nl2lo, 0, -u)'
    else
        'define nl2lo=ave(apcpsfc.1, t='t', t='t+step-1')'
    endif
    'define nl2hi=re(nfill(nl2lo,lat,'buff'), 'rehi', bl)'
    'define st42lo=ave(apcpsfc.2, t='t', t='t+step-1')'
    'define st42hi=re(st42lo, 'rehi', bl)'
    
    if (resc_flag=1)
        'define st42hi=st42hi*fschi'
    endif
    
    'define mks=const(st42hi*0+1, 0, -u)'
    'define mkn=const(nl2hi*0+1, 0, -u)'
    'define mki=mkn*(1-mks)'
    'define pcphi=(const(st42hi, 0, -u)+const(nl2hi, 0, -u)*mki)*maskout(1, mks+mkn-0.5)'
    'define pcphi=pcphi/3600'
    
    'undefine nl2hi'
    'undefine st42hi'
    'undefine mks'
    'undefine mkn'
    'undefine mki'

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
    
    'define tmplo=ave(tmp2m.1, t='t', t='t+step-1')'
    'define tmpls=tmplo-demlo*('lapse')'
    
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
   
    'define dswlo=ave(dswrfsfc.1, t='t', t='t+step-1')'
    
*   Simple interpolation for shortwave and wind speed
    'define dswhi=re(nfill(dswlo,lat,'buff'), 'rehi', bl)'
    
    'define wsulo=ave(ugrd10m.1, t='t', t='t+step-1')'
    'define wsuhi=re(nfill(wsulo,lat,'buff'), 'rehi', bl)'
    'define wsvlo=ave(vgrd10m.1, t='t', t='t+step-1')'
    'define wsvhi=re(nfill(wsvlo,lat,'buff'), 'rehi', bl)'
    
    curryear=substr(tstamp, 1, 4)
    currmonn=substr(tstamp, 5, 2)
    currhour=substr(tstamp, 9, 2)

    'define tmeand=tmeand+tmphi'
    'define tmaxd=(tmaxd+tmphi+abs(tmaxd-tmphi))/2'
    'define tmind=(tmind+tmphi-abs(tmind-tmphi))/2'
    'define precipd=precipd+pcphi'
    'define swdownd=swdownd+dswhi'
    'define windd=windd+sqrt(wsuhi*wsuhi+wsvhi*wsvhi)'

    if (currhour=23)

        'define tmeand=tmeand/24'
        'define precipd=precipd*3600'
        'define swdownd=swdownd/24'
        'define windd=windd/24'
        
        fdir="../lstm/nrt/"%curryear
        '!mkdir -p 'fdir
        tstampd=substr(tstamp, 1, 8)
        fout=fdir%"/lstm_forcing_"tstampd%".nc"

        v=1
        while (v<=6)
       
            vi=subwrd(vars_vic, v)
            vs=subwrd(vars_short, v)
            vu=subwrd(units, v)
       
            'define 'vs'=nfill('vi', msks, 30)*msks'
*            'define 'vs'='vi'*msks'
        
            'clear sdfwrite'

            say "set sdfattr "vs" String units "vu

            'set sdfattr 'vs' String units 'vu
            'set sdfattr 'vs' String long_name 'vars_long.v
        
            'set sdfwrite -flt -nc4 -3dt tmp_'tstampd'_'v'.nc'
            'sdfwrite 'vs

*            'undefine 'vi
            'undefine 'vs

            if (vi="tmaxd")
                'define 'vi'=msks-10000'
            else
                if (vi="tmind")
                    'define 'vi'=msks+10000'
                else
                    'define 'vi'=msks*0'
                endif   
            endif

            v=v+1
        endwhile

        '!cdo --history -O -f nc4 -z zip merge tmp_'tstampd'_?.nc 'fout
        '!/bin/rm tmp_'tstampd'_?.nc'

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

