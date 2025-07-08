/**
 * @file Load NWM/NHDPlus2 rivers map layer
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { setupColormap } from './colormap.js';

export function loadLayerNwmRivers(map, id, initialVisibility=false, legendColor='darkgray') {

  const layerName = id;
  const idField = 'feature_id';

  const lineColorRules = legendColor;
  const lineWidthRules = ["interpolate", ["linear"], ["zoom"],
                    0, ["-", ["get", "stream_order"], 6],
                    1, ["-", ["get", "stream_order"], 6],
                    2, ["-", ["get", "stream_order"], 6],
                    3, ["-", ["get", "stream_order"], 5.5],
                    4, ["-", ["get", "stream_order"], 5],
                    5, ["-", ["get", "stream_order"], 5],
                    6, ["-", ["get", "stream_order"], 4],
                    7, ["-", ["get", "stream_order"], 3],
                    8, ["-", ["get", "stream_order"], 2],
                    9, ["-", ["get", "stream_order"], 1],
                    10, ["-", ["get", "stream_order"], 1],
                    11, ["-", ["get", "stream_order"], 1],
                    12, ["+", ["get", "stream_order"], 0],
                    13, ["+", ["get", "stream_order"], 1]
                ];
  const lineWidthRulesCasing = ["interpolate", ["linear"], ["zoom"],
                    0, ["-", ["get", "stream_order"], 5],
                    1, ["-", ["get", "stream_order"], 5],
                    2, ["-", ["get", "stream_order"], 5],
                    3, ["-", ["get", "stream_order"], 4.5],
                    4, ["-", ["get", "stream_order"], 4],
                    5, ["-", ["get", "stream_order"], 4],
                    6, ["-", ["get", "stream_order"], 3],
                    7, ["-", ["get", "stream_order"], 2],
                    8, ["-", ["get", "stream_order"], 1],
                    9, ["-", ["get", "stream_order"], 0],
                    10, ["-", ["get", "stream_order"], 0],
                    11, ["-", ["get", "stream_order"], 0],
                    12, ["+", ["get", "stream_order"], 1],
                    13, ["+", ["get", "stream_order"], 2]
                ];

  map.on('styledata', () => {

    if (!map.getSource(layerName+'_source')) {
      map.addSource(layerName+'_source', {
        type: 'vector',
        url: 'pmtiles://https://cw3e.ucsd.edu/wrf_hydro/cnrfc/pmtiles/nwm_reaches_cnrfc.pmtiles',
        attribution: '<a href="https://water.noaa.gov/about/nwm">National Water Model</a> | <a href="https://mghydro.com/">mghydro.com</a>'
      });

      map.addLayer({
        'id': layerName+'-casing',
        'source': layerName+'_source',
        'source-layer': 'NWM_v2.1_channels',
        'type': 'line',
        'paint': {
          'line-color': 'white',
          'line-width': lineWidthRulesCasing
        }
      });

      map.addLayer({
        'id': layerName,
        'source': layerName+'_source',
        'source-layer': 'NWM_v2.1_channels',
        'type': 'line',
        'paint': {
          'line-color': lineColorRules,
          'line-width': lineWidthRules
        }
      });

      map.addLayer({
        'id': layerName+'-highlight',
        'source': layerName+'_source',
        'source-layer': 'NWM_v2.1_channels',
        'type': 'line',
        'paint': {
          'line-color': 'red',
          'line-width': lineWidthRules
        },
        filter: ['==', idField, '']  // no feature highlighted initially
      });
      
      map.setLayoutProperty(layerName, 'visibility', initialVisibility ? 'visible' : 'none');
      const extras = ['highlight', 'casing'];
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

    var popuptext = `<strong>River ID: ${id}</strong><br/>`;
    popuptext += `Name: ${feature.properties.gnis_name}<br/>`;
    popuptext += `Source: ${feature.properties.source}<br/>`;
    popuptext += `Length: ${(feature.properties.Shape_Length*111.1).toFixed(1)} km<br/>`;
    popuptext += `Stream Order: ${feature.properties.stream_order}`;

    popupHover.setLngLat(e.lngLat)
        .setHTML(popuptext)
        .addTo(map);
  });

  map.on('mouseleave', layerName, () => {
    map.getCanvas().style.cursor = '';
    map.setFilter(layerName+'-highlight', ['==', idField, '']);
    popupHover.remove();
  });

  // set up colormap for flow percentiles
  const colorStops = [
      { threshold:   98, color: "rgba(  0,  38, 115, 1)" },
      { threshold:   95, color: "rgba( 20,  90,   0, 1)" },
      { threshold:   90, color: "rgba( 56, 168,   0, 1)" },
      { threshold:   80, color: "rgba( 76, 230,   0, 1)" },
      { threshold:   70, color: "rgba(170, 245, 150, 1)" },
      { threshold:   30, color: "rgba(255, 255, 255, 1)" },
      { threshold:   20, color: "rgba(254, 254,   0, 1)" },
      { threshold:   10, color: "rgba(254, 211, 127, 1)" },
      { threshold:    5, color: "rgba(230, 152,   0, 1)" },
      { threshold:    2, color: "rgba(230,   0,   0, 1)" },
      { threshold: null, color: "rgba(115,   0,   0, 1)" }
  ];
  // draw colormap
  setupColormap(colorStops, '&percnt;ile');

}