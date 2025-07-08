/**
 * @file Load CNRFC boundary map layer
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

export function loadLayerCnrfc(map, id, initialVisibility=false, legendColor='darkgray') {

  const layerName = id;

  const lineColorRules = legendColor;

  map.on('styledata', () => {

    if (!map.getSource(layerName+'_source')) {
      map.addSource(layerName+'_source', {
        type: 'geojson',
        data: 'https://cw3e.ucsd.edu/hydro/cnrfc/csv/cnrfc_line.geojson',
        attribution: '<a href="https://cw3e.ucsd.edu/">CW3E</a>'
      });

      map.addLayer({
        'id': layerName,
        'source': layerName+'_source',
        'type': 'line',
        'paint': {
          'line-color': lineColorRules,
          'line-width': 2
        }
      });
      
      map.setLayoutProperty(layerName, 'visibility', initialVisibility ? 'visible' : 'none');
    }
    
  });
    
}