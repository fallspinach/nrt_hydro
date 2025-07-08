/**
 * @file Load data image overlay map layer
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { getYesterday } from './utils.js';
import { variableImages, cnrfcCoords } from './overlay-control.js';

export function loadLayerOverlay(map, id, initialVisibility=false, legendColor='darkgray') {

  const layerName = id;
  
  const dateInput = document.getElementById('datepicker');
  const varibleSelector = document.getElementById('variable-selector');

  const input = dateInput.value;
  var   dataDate = new Date(input);
  if (isNaN(dataDate)) dataDate = getYesterday();
  // Read selected variable
  const variable = varibleSelector.value;

  const { folder, timestamp, colorStops, legendTitle } = variableImages[variable];
  const ymd = dataDate.toISOString().split('T')[0].replaceAll('-', '');
  const y = ymd.slice(0, 4);
  var tString = ymd;
  if (timestamp=='month') tString = ymd.slice(0, 6);
  const imgUrl = `https://cw3e.ucsd.edu/hydro/cnrfc/imgs/${folder}/${y}/${variable}_${tString}.png`;

  map.on('styledata', () => {

    if (!map.getSource(layerName+'_source')) {
      map.addSource(layerName+'_source', {
        type: 'image',
        url: imgUrl,
        coordinates: cnrfcCoords
      });
    }

    if (!map.getLayer(layerName)) {
      map.addLayer({
        'id': layerName,
        'source': layerName+'_source',
        'type': 'raster',
        'paint': {
          'raster-opacity': 0.7
        }
      });
      
      map.setLayoutProperty(layerName, 'visibility', initialVisibility ? 'visible' : 'none');
    }
    
  });
    
}