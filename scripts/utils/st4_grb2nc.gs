function grb2nc(args)
    
    time1=subwrd(args, 1)
    time2=subwrd(args, 2)
    
*    'open /cw3e/mead/projects/cwp101/wrf_hydro/forcing/stage4/archive/ST4n2a_archive.ctl'
*    'open /cw3e/mead/projects/cwp101/wrf_hydro/forcing/stage4/archive/ST4n2a_2020.ctl'
    'open /cw3e/mead/projects/cnt107/nrt_hydro/forcing/stage4/archive/grb1/ST4.ctl'

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


*   some domain parameters
    lat1=25
    lat2=53
    lon1=-125
    lon2=-67

    resdme=0.04
    reme=mkre(lat1%" "%lat2%" "%lon1%" "%lon2%" "%resdme)

    t=t1
    while (t<=t2)
        
        'set t 't
        tstamp=dtime()
        yy=substr(tstamp, 1, 4)

        'set t 't' 't+23


        vs="apcpsfc"
        vu="kg/m^2"
        vc="precipitation_amount"
        vl="Total precipitation at surface"
        'define 'vs'=re('vs', 'reme')'

        'clear sdfwrite'
        
        say tstamp" set sdfattr "vs" String units "vu

        'set sdfattr 'vs' String units 'vu
        'set sdfattr 'vs' String long_name 'vl
        'set sdfattr 'vs' String standard_name 'vc
        
        'set sdfwrite -flt -nc4 -zip -3dt /cw3e/mead/projects/cnt107/nrt_hydro/forcing/stage4/archive/'yy'/st4_conus.'tstamp'.01h.nc'
        'sdfwrite 'vs
        
        'undefine 'vs

        t=t+24
    endwhile

    'close 1'
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
  dt=year%mo%day
  
return dt
