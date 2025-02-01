#### Configuration data here

import dash_leaflet as dl
import pandas as pd
from glob import glob
from datetime import datetime

## global configs

# system status
cloud_url = 'https://storage.googleapis.com/cw3e-water-panel.appspot.com'
base_url = '.'
fcsv = f'{cloud_url}/data/system_status.csv'
df_system_status = pd.read_csv(fcsv, parse_dates=True)

# map domain setup
#domain_config = {'bounds': [[32, -125], [44, -113]], 'center': [38.2, -119], 'zoom': 6}    # cnrfc
#domain_config = {'bounds': [[35, -124], [42, -117]], 'center': [38.7, -121], 'zoom': 7}    # basins24
#domain_config = {'bounds': [[25, -125], [50, -67]], 'center': [38.7, -98], 'zoom': 5}    # conus 
domain_config = {'bounds': [[29, -119], [45, -103]], 'center': [37.5, -110], 'zoom': 6}    # cbrfc

# image snapshot export options
graph_config = {'toImageButtonOptions': {'format': 'png', 'filename': 'cw3e_water_panel_plot', 'scale': 3}, 'displaylogo': False} 

# styles
tool_style = {'min-height': '312px', 'background-color': 'white', 'font-size': 'small', 'border': '1px solid lightgray', 'border-top-style': 'none'}
tabtitle_style = {'padding': '2px', 'height': '28px', 'font-size': 'small'}
tabtitle_selected_style = tabtitle_style.copy()
tabtitle_selected_style.update ({'font-weight': 'bold'})
popup_ts_style = {'opacity': '1', 'width': '90%', 'min-width': '1000px', 'min-height': '540px', 'margin-top': '150px', 'margin-left': 'auto', 'margin-right': 'auto', 'font-size': 'smaller'}
fig_ts_style = {'height': '400px', 'padding-top': '40px'}

## maps

# some available map tiles
map_tiles = [
    dl.TileLayer(url='https://tiles.stadiamaps.com/tiles/alidade_smooth/{z}/{x}/{y}{r}.png',
        #attribution='&copy; <a href="https://stadiamaps.com/">Stadia Maps</a>, <a href="https://openmaptiles.org/">OpenMapTiles</a>, <a href="http://openstreetmap.org">OpenStreetMap</a> contributors'
    ),
    #dl.TileLayer(url='https://stamen-tiles-{s}.a.ssl.fastly.net/toner-lite/{z}/{x}/{y}{r}.png',
    dl.TileLayer(url='https://tiles.stadiamaps.com/tiles/stamen_toner_lite/{z}/{x}/{y}{r}.png',
        #attribution='<a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a>, &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    ),
    dl.TileLayer(url='http://services.arcgisonline.com/arcgis/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}',
        #attribution='&copy; Esri and Community'
    )
]

## data variables
output_url  = f'{cloud_url}/imgs/cbrfc/output'
forcing_url = f'{cloud_url}/imgs/cbrfc/forcing'
data_vars = [
    {'label': 'SWE Percentile (daily)',    'name': 'swe_r',    'cat': 'hydro', 'url': f'{output_url}/%Y/swe_r_%Y%m%d.png',   'cbar': f'{output_url}/swe_r_cbar.png'},
    {'label': '2-m SM Percentile (daily)', 'name': 'smtot_r',  'cat': 'hydro', 'url': f'{output_url}/%Y/smtot_r_%Y%m%d.png', 'cbar': f'{output_url}/smtot_r_cbar.png'},
    {'label': 'Precipitation (daily)',     'name': 'precip',   'cat': 'met',   'url': f'{forcing_url}/%Y/precip_%Y%m%d.png', 'cbar': f'{forcing_url}/precip_cbar.png'},
    {'label': 'Air Temperature (daily)',   'name': 'tair2m',   'cat': 'met',   'url': f'{forcing_url}/%Y/tair2m_%Y%m%d.png', 'cbar': f'{forcing_url}/tair2m_cbar.png'},
    {'label': 'P Percentile (monthly)',    'name': 'precip_r', 'cat': 'met',   'url': f'{forcing_url}/%Y/precip_r_%Y%m.png', 'cbar': f'{forcing_url}/precip_r_cbar.png'},
    {'label': 'T Percentile (monthly)',    'name': 'tair2m_r', 'cat': 'met',   'url': f'{forcing_url}/%Y/atir2m_r_%Y%m.png', 'cbar': f'{forcing_url}/tair2m_r_cbar.png'},
]
          
## site lists
fcsv = f'{cloud_url}/data/cbrfc/fnf_stations.csv'
df_fnf = pd.read_csv(fcsv, dtype={'station_id': str})
fnf_stations = df_fnf['station_id'].to_list()

snow_pillow_stations = ['ADM', 'AGP', 'ALP', 'BCB', 'BCH', 'BFL', 'BGP', 'BIM', 'BKL', 'BLA', 'BLC', 'BLD', 'BLK', 'BLS', 'BMW', 'BNK', 'BSH', 'BSK', 'CAP', 'CBT', 'CDP', 'CHM', 'CHP', 'CRL', 'CSL', 'CSV', 'CWD', 'CWF', 'CXS', 'DAN', 'DDM', 'DPO', 'DSS', 'EBB', 'EP5', 'FDC', 'FLL', 'FOR', 'FRN', 'FRW', 'GEM', 'GIN', 'GKS', 'GNF', 'GNL', 'GOL', 'GRM', 'GRV', 'GRZ', 'HGM', 'HHM', 'HIG', 'HMB', 'HNT', 'HOR', 'HRK', 'HRS', 'HVN', 'HYS', 'IDC', 'IDP', 'INN', 'KIB', 'KSP', 'KTL', 'KUB', 'KUP', 'LBD', 'LLP', 'LOS', 'LVM', 'LVT', 'MB3', 'MDW', 'MED', 'MHP', 'MNT', 'MRL', 'MSK', 'MTM', 'MUM', 'NLS', 'PDS', 'PET', 'PLP', 'PSC', 'PSN', 'PSR', 'QUA', 'RBB', 'RBP', 'RCC', 'RCK', 'REL', 'RP2', 'RRM', 'RTL', 'SCN', 'SCT', 'SDF', 'SDW', 'SHM', 'SIL', 'SLI', 'SLK', 'SLM', 'SLT', 'SNM', 'SPS', 'SPT', 'SQV', 'SSM', 'STL', 'STM', 'STR', 'SWM', 'TCC', 'TK2', 'TMR', 'TNY', 'TUM', 'TUN', 'UBC', 'UTY', 'VLC', 'VRG', 'VVL', 'WC3', 'WHW', 'WTM', 'WWC']

snow_course_stations = ['3LK', 'ABN', 'ABY', 'ADI', 'AGP', 'ANR', 'APH', 'ASH', 'ATP', 'ATS', 'BBS', 'BCB', 'BCP', 'BDF', 'BEM', 'BFT', 'BGH', 'BHM', 'BHV', 'BKL', 'BLD', 'BLF', 'BLK', 'BLS', 'BLU', 'BMD', 'BMN', 'BMS', 'BNH', 'BNM', 'BNP', 'BNS', 'BOM', 'BP2', 'BP3', 'BSH', 'BSP', 'BV1', 'BWH', 'BWR', 'BXC', 'CAP', 'CBM', 'CBT', 'CC5', 'CCO', 'CDP', 'CFM', 'CHF', 'CHK', 'CHM', 'CHQ', 'CHU', 'CKT', 'CLM', 'CLT', 'CMC', 'CRA', 'CRF', 'CRM', 'CSV', 'CUR', 'CW1', 'CWP', 'CYT', 'DAN', 'DDF', 'DDM', 'DHC', 'DHM', 'DMN', 'DNS', 'DPO', 'DSM', 'DTL', 'DYM', 'ECS', 'EGM', 'ELL', 'EML', 'ENM', 'EPP', 'ERB', 'ETN', 'EUR', 'FBN', 'FCV', 'FDM', 'FEM', 'FLC', 'FLK', 'FNF', 'FNP', 'FOD', 'FP3', 'FRW', 'GBN', 'GEM', 'GFL', 'GFR', 'GL2', 'GML', 'GNL', 'GRZ', 'GYF', 'GYR', 'HCL', 'HCM', 'HHM', 'HIG', 'HKM', 'HLK', 'HLM', 'HMS', 'HRF', 'HRG', 'HRS', 'HRT', 'HS2', 'HTT', 'HYS', 'IHS', 'JCM', 'KRC', 'KSR', 'KTL', 'LB2', 'LCP', 'LCR', 'LKB', 'LLL', 'LLP', 'LMD', 'LSH', 'LTT', 'LWM', 'LXN', 'LYN', 'MAM', 'MB3', 'MBL', 'MCB', 'MCP', 'MD2', 'MDC', 'MDY', 'MHG', 'MLF', 'MMT', 'MN2', 'MNK', 'MNP', 'MRO', 'MSH', 'MSV', 'MUM', 'MWL', 'NFS', 'NGF', 'NGM', 'NLL', 'NMN', 'NRF', 'NTH', 'OEM', 'ONN', 'PDS', 'PDT', 'PFV', 'PGM', 'PHL', 'PLP', 'PMD', 'PNB', 'PPS', 'PRK', 'PRM', 'PSM', 'PTM', 'QKA', 'QRS', 'RBV', 'RC1', 'RC2', 'RC3', 'RCH', 'RCR', 'RCW', 'RDC', 'RDM', 'REL', 'RFM', 'RGT', 'RLD', 'RMD', 'RMM', 'RMR', 'RP1', 'RP2', 'RRM', 'RTT', 'RWL', 'RWM', 'SA2', 'SCE', 'SCF', 'SDL', 'SDM', 'SFT', 'SHM', 'SHW', 'SIB', 'SIL', 'SLM', 'SLT', 'SMD', 'SMT', 'SNF', 'SPD', 'SPF', 'SQ2', 'SRP', 'SSM', 'STM', 'STR', 'SVR', 'SWJ', 'SWL', 'SWM', 'SWT', 'TBC', 'TGP', 'THE', 'THL', 'TMF', 'TMK', 'TND', 'TNY', 'TRG', 'TRL', 'TUM', 'UBC', 'UCP', 'UKR', 'UTR', 'VLC', 'VNN', 'VRG', 'WBB', 'WBM', 'WDH', 'WHE', 'WHN', 'WLC', 'WLF', 'WLW', 'WPK', 'WR2', 'WRG', 'WRN', 'YBP']

# huc8 stations
fcsv = f'{cloud_url}/data/cbrfc/huc8_conus_list.csv'
df_huc8 = pd.read_csv(fcsv, dtype={'HUC8': str})
huc8_basins = df_huc8['HUC8'].to_list()

