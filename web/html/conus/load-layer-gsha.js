/**
 * @file Load GSHA map layer
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

export function loadLayerGsha(map, id, initialVisibility=false, legendColor='darkgray') {

  const layerName = id;
  const idField = 'Sttn_Nm';

  const circleColorRules = legendColor;
  const circleRadiusRules = ["interpolate", ["linear"], ["zoom"],
                                0, ["-", ["get", "order"], 2],
                                1, ["-", ["get", "order"], 2],
                                2, ["-", ["get", "order"], 2],
                                3, ["-", ["get", "order"], 1.5],
                                4, ["-", ["get", "order"], 1],
                                5, ["-", ["get", "order"], 1],
                                6, ["+", ["get", "order"], 0],
                                7, ["+", ["get", "order"], 1],
                                8, ["+", ["get", "order"], 2],
                                9, ["+", ["get", "order"], 3],
                                10, ["+", ["get", "order"], 3],
                                11, ["+", ["get", "order"], 4],
                                12, ["+", ["get", "order"], 5]
                ];

  map.on('styledata', () => {

    if (!map.getSource(layerName+'_source')) {
      map.addSource(layerName+'_source', {
        type: 'vector',
        url: 'pmtiles://https://cw3e.ucsd.edu/hydro/gsha/pmtiles/GSHA_MERIT.pmtiles',
        attribution: '<a href="https://zenodo.org/records/10433905">GSHA</a>'
      });

      map.addLayer({
        'id': layerName,
        'source': layerName+'_source',
        'source-layer': 'GSHA_MERIT',
        'type': 'circle',
        'paint': {
          'circle-color': circleColorRules,
          'circle-radius': circleRadiusRules
        }
      });

      map.addLayer({
        'id': layerName+'-highlight',
        'source': layerName+'_source',
        'source-layer': 'GSHA_MERIT',
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
        'source-layer': 'GSHA_MERIT',
        layout: {
          'text-field': ['format', ['get', 'agency'], {}, ' ', {}, ['get', 'Sttn'], {}],
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
        minzoom: 10
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

    var popuptext = `<strong>${feature.properties.agency} ${feature.properties.Sttn_Nm.split("_")[0]} (GSHA #${feature.properties.dindex})</strong><br/>`;
    popuptext += `Watershed Area: ${feature.properties.WatershedArea.toFixed(1)} km<sup>2</sup><br/>`;
    popuptext += `MERIT-Basins Reach (${feature.properties.verification}):<br/>`;
    popuptext += `&nbsp;&nbsp;&nbsp;&nbsp;COMID: ${feature.properties.COMID}<br/>`;
    popuptext += `&nbsp;&nbsp;&nbsp;&nbsp;Upstream Area: ${feature.properties.uparea.toFixed(1)} km<sup>2</sup>`;

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