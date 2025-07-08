/**
 * @file Load snow obs network map layer
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { loadSnowNetwork } from './utils.js';

export async function loadLayerSnowNetwork(map, id, initialVisibility=false, legendColor='darkgray') {

  const layerName = id;
  const idField = 'STA';
  const geojson = await loadSnowNetwork(`https://cw3e.ucsd.edu/hydro/cnrfc/csv/${layerName}.csv`);

  const textColorRules = legendColor;
  const textSizeRules  = ["interpolate", ["linear"], ["zoom"],
                                0, 12,
                                5, 14,
                                12, 22
                            ];

  map.on('styledata', () => {

    if (!map.getSource(layerName+'_source')) {
      map.addSource(layerName+'_source', {
        type: 'geojson',
        data: geojson,
        attribution: '<a href="https://water.ca.gov/">CA DWR</a>',
        maxzoom: 22
      });

      map.addLayer({
        'id': layerName,
        'source': layerName+'_source',
        'type': 'symbol',
        'layout': {
          'text-field': '*',
          'text-font': ['Noto Sans Regular', 'Arial Unicode MS Regular'],
          'text-size': textSizeRules
        },
        'paint': {
          'text-color': textColorRules,
          'text-halo-color': '#fff',
          'text-halo-width': 1
        }
      });

      map.addLayer({
        'id': layerName+'-highlight',
        'source': layerName+'_source',
        'type': 'symbol',
        'layout': {
          'text-field': '*',
          'text-font': ['Noto Sans Regular', 'Arial Unicode MS Regular'],
          'text-size': textSizeRules
        },
        'paint': {
          'text-color': 'red',
          'text-halo-color': '#fff',
          'text-halo-width': 1
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

    var popuptext = `<strong>Station ID: ${feature.properties.STA}</strong><br/>`;
    popuptext += `Station Name: ${feature.properties.StationName}<br/>`;
    popuptext += `Elevation: ${feature.properties.Elevation} ft<br/>`;
    popuptext += `Basin Name: ${feature.properties.BasinName}<br/>`;
    popuptext += `Hydro Area: ${feature.properties.HydroArea}`;

    popupHover.setLngLat(e.lngLat)
        .setHTML(popuptext)
        .addTo(map);
  });

  map.on('mouseleave', layerName, () => {
    map.getCanvas().style.cursor = '';
    map.setFilter(layerName+'-highlight', ['==', idField, '']);
    popupHover.remove();
  });
    
}