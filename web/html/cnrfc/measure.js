/**
 * @file distance measure tool
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

let distanceStartPoint = null;
let distancePreviewHandler = null;
let distanceClickHandler = null;

export function enterDistanceMeasureMode(map, startLngLat) {
  distanceStartPoint = startLngLat;

  // Mousemove to update preview
  distancePreviewHandler = (e) => {
    drawDistanceLine(map, distanceStartPoint, e.lngLat, true); // true = preview
  };

  // Final click to confirm
  distanceClickHandler = (e) => {
    const endLngLat = e.lngLat;
    const distance = turf.distance(
      [distanceStartPoint.lng, distanceStartPoint.lat],
      [endLngLat.lng, endLngLat.lat],
      { units: 'kilometers' }
    );

    drawDistanceLine(map, distanceStartPoint, endLngLat, false); // false = permanent

    // Clean up
    map.off('mousemove', distancePreviewHandler);
    map.off('click', distanceClickHandler);
    distancePreviewHandler = null;
    distanceClickHandler = null;
    distanceStartPoint = null;
  };

  map.on('mousemove', distancePreviewHandler);
  map.once('click', distanceClickHandler);
}

function drawDistanceLine(map, from, to, isPreview = false) {
  const lineId = isPreview ? 'distance-line-preview' : 'distance-line';
  const labelId = isPreview ? 'distance-label-preview' : 'distance-label';

  const coords = [
    [from.lng, from.lat],
    [to.lng, to.lat]
  ];

  const geojsonLine = {
    type: 'FeatureCollection',
    features: [{
      type: 'Feature',
      geometry: {
        type: 'LineString',
        coordinates: coords
      }
    }]
  };

  const midPoint = turf.midpoint(coords[0], coords[1]);
  const distance = turf.distance(coords[0], coords[1], { units: 'kilometers' });
  let label = '';
  if (distance < 1) {
    label = `${(distance * 1000).toFixed(0)} m`;
  } else {
    label = `${distance.toFixed(2)} km`;
  }

  const geojsonLabel = {
    type: 'FeatureCollection',
    features: [{
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: midPoint.geometry.coordinates
      },
      properties: {
        label: label
      }
    }]
  };

  // LINE
  if (map.getLayer(lineId)) map.removeLayer(lineId);
  if (map.getSource(lineId)) map.removeSource(lineId);
  map.addSource(lineId, { type: 'geojson', data: geojsonLine });
  map.addLayer({
    id: lineId,
    type: 'line',
    source: lineId,
    paint: {
      'line-color': isPreview ? '#f00' : '#f00',
      'line-width': isPreview ? 2 : 3,
      'line-dasharray': [3, 2]
    }
  });

  // LABEL
  if (map.getLayer(labelId)) map.removeLayer(labelId);
  if (map.getSource(labelId)) map.removeSource(labelId);
  map.addSource(labelId, { type: 'geojson', data: geojsonLabel });
  map.addLayer({
    id: labelId,
    type: 'symbol',
    source: labelId,
    layout: {
      'text-field': ['get', 'label'],
      'text-size': 14,
      'text-offset': [0, 1.2],
      'text-anchor': 'top'
    },
    paint: {
      'text-color': isPreview ? '#f00' : '#f00',
      'text-halo-color': '#fff',
      'text-halo-width': 1
    }
  });

  // Clean up preview if finalized
  if (!isPreview) {
    ['distance-line-preview', 'distance-label-preview'].forEach(id => {
      if (map.getLayer(id)) map.removeLayer(id);
      if (map.getSource(id)) map.removeSource(id);
    });
  }
}
