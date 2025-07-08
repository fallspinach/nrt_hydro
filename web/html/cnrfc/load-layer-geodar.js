/**
 * @file Load GSHA map layer
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

export function loadLayerGeodar(map, id, initialVisibility=false, legendColor='darkgray') {

  const layerName = id;
  const idField = 'id_v11';
  const textColorRules = legendColor;
  const textSizeRules = ["interpolate", ["linear"], ["zoom"],
                                0, ["+", ["get", "order"], 5],
                                1, ["+", ["get", "order"], 6],
                                2, ["+", ["get", "order"], 7],
                                3, ["+", ["get", "order"], 8],
                                4, ["+", ["get", "order"], 9],
                                5, ["+", ["get", "order"], 10],
                                6, ["+", ["get", "order"], 11],
                                7, ["+", ["get", "order"], 12],
                                8, ["+", ["get", "order"], 13],
                                9, ["+", ["get", "order"], 14],
                                10, ["+", ["get", "order"], 15],
                                11, ["+", ["get", "order"], 16],
                                12, ["+", ["get", "order"], 17]
                ];

  map.on('styledata', () => {

    if (!map.getSource(layerName+'_source')) {
      map.addSource(layerName+'_source', {
        type: 'vector',
        url: 'pmtiles://https://cw3e.ucsd.edu/hydro/geodar/GeoDAR_MERIT.pmtiles',
        attribution: '<a href="https://zenodo.org/records/10433905">GeoDAR</a>'
      });

      map.addLayer({
        'id': layerName,
        'source': layerName+'_source',
        'source-layer': 'GeoDAR_MERIT',
        'type': 'symbol',
        'layout': {
          'text-field': 'Δ',
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
        'source-layer': 'GeoDAR_MERIT',
        'type': 'symbol',
        'layout': {
          'text-field': 'Δ',
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
      
      map.setLayoutProperty(layerName, 'visibility', initialVisibility ? 'visible' : 'none');
      const extras = ['highlight'];
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

    var popuptext = `<strong>GeoDAR ID: ${feature.properties.id_v11}</strong><br/>`;
    popuptext += `Reservoir Volume: ${feature.properties.rv_mcm_v11.toFixed(0)}<br/>`;

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