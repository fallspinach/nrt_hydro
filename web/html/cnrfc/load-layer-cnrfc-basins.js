/**
 * @file Load CNRFC basins map layer
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

export function loadLayerCnrfcBasins(map, id, initialVisibility=false, legendColor='darkgray') {

  const layerName = id;
  const idField = 'Basin';

  const lineColorRules = legendColor;

  map.on('styledata', () => {

    if (!map.getSource(layerName+'_source')) {
      map.addSource(layerName+'_source', {
        type: 'vector',
        url: 'pmtiles://https://cw3e.ucsd.edu/wrf_hydro/cnrfc/pmtiles/CNRFC_Basins.pmtiles',
        attribution: '<a href="https://www.cnrfc.noaa.gov/">CNRFC</a>'
      });

      map.addLayer({
        'id': layerName,
        'source': layerName+'_source',
        'source-layer': 'CNRFC_Basins',
        'type': 'fill',
        'paint': {
          'fill-color': 'rgba(0, 100, 255, 0.2)',
          'fill-outline-color': lineColorRules
        },
        filter: ['==', idField, '']  // no feature highlighted initially
      });
      
      map.setLayoutProperty(layerName, 'visibility', initialVisibility ? 'visible' : 'none');
    }
    
  });

}