/**
 * @file Load DWR official forecast points map layer
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { loadDwrPoints } from './utils.js';

export async function loadLayerDwrPoints(map, id, initialVisibility=false, legendColor='darkgray') {

  const layerName = id;
  const idField = 'ID';
  const geojson = await loadDwrPoints(`https://cw3e.ucsd.edu/hydro/cnrfc/csv/${layerName}.csv`);

  const circleColorRules = legendColor;
  const circleRadiusRules = ["interpolate", ["linear"], ["zoom"],
                                0, 3,
                                5, 3,
                                6, 4,
                                12, 8
                            ];

  map.on('styledata', () => {

    if (!map.getSource(layerName+'_source')) {
      map.addSource(layerName+'_source', {
        type: 'geojson',
        data: geojson,
        attribution: '<a href="https://water.ca.gov/">CA DWR</a>'
      });

      map.addLayer({
        'id': layerName,
        'source': layerName+'_source',
        'type': 'circle',
        'paint': {
          'circle-color': circleColorRules,
          'circle-radius': circleRadiusRules
        }
      });

      map.addLayer({
        'id': layerName+'-highlight',
        'source': layerName+'_source',
        'type': 'circle',
        'paint': {
          'circle-color': 'red',
          'circle-radius': circleRadiusRules
        },
        filter: ['==', idField, '']  // no feature highlighted initially
      });

      map.addLayer({
        id: layerName+'-labels',
        type: 'symbol',
        source: layerName+'_source', // your vector or geojson source
        layout: {
          'text-field': ['get', idField],
          'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
          'text-size': 12,
          'text-offset': [0, 1.5],
          'text-anchor': 'top'
        },
        paint: {
          'text-color': '#333',
          'text-halo-color': '#fff',
          'text-halo-width': 1
        },
        minzoom: 9
      });
      
      map.setLayoutProperty(layerName, 'visibility', initialVisibility ? 'visible' : 'none');
      const extras = ['highlight', 'labels'];
      extras.forEach(extra => {
        map.setLayoutProperty(layerName+'-'+extra, 'visibility', initialVisibility ? 'visible' : 'none');
      });
    }
    
  });

  const popupHover =  new maplibregl.Popup({closeButton: false, closeOnClick: false});

  map.on('mousemove', layerName, (e) => {
    map.getCanvas().style.cursor = 'pointer';
    const feature = e.features[0];
    const id = feature.properties[idField];

    map.setFilter(layerName+'-highlight', ['==', idField, id]);
    map.setFilter('cnbasins', ['==', 'Basin', id]);

    var popuptext = `<strong>ID: ${feature.properties.ID}</strong><br/>`;
    popuptext += `River: ${feature.properties.River}<br/>`;
    popuptext += `Location: ${feature.properties.Location.split(" - ")[1]}<br/>`;
    popuptext += `Matching NWM Reach: ${feature.properties.ReachID}`;
    // popuptext += `Matching NWM Reach 2: ${feature.properties.link}`;

    popupHover.setLngLat(e.lngLat)
        .setHTML(popuptext)
        .addTo(map);
  });

  map.on('mouseleave', layerName, () => {
    map.getCanvas().style.cursor = '';
    map.setFilter(layerName+'-highlight', ['==', idField, '']);
    map.setFilter('cnbasins', ['==', 'Basin', '']);
    popupHover.remove();
  });
    
}
