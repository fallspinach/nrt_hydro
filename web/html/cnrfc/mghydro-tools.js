/**
 * @file enable API tools provided by mghydro.com
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */
import { showModal } from './utils.js';

export let currentShapeGeoJSON = null;

export async function fetchAndDisplayShape(map, comidShape, lat, lng, shapeType) {
  const url = `https://mghydro.com/app/${shapeType}_api?lat=${lat}&lng=${lng}&precision=low`;

  try {
    const res = await fetch(url);
    const geojson = await res.json();

    // Store and display
    currentShapeGeoJSON = geojson; // Store it
    // Remove existing shape layer/source if present
    if (map.getLayer(`${shapeType}-layer`)) map.removeLayer(`${shapeType}-layer`);
    if (map.getSource(shapeType)) map.removeSource(shapeType);

    // Add GeoJSON as a new source and layer
    map.addSource(shapeType, {
      type: 'geojson',
      data: geojson
    });

    if (shapeType=='watershed') {
      var style_type = 'fill';
      var style_paint = {
        'fill-color': 'rgba(0, 100, 255, 0.2)',
        'fill-outline-color': 'blue'
      };
    } else if (shapeType=='upstream_rivers') {
      var style_type = 'line';
      var style_paint = {
        'line-color': 'blue',
        'line-width': 2
      };
    } else {
      var style_type = 'line';
      var style_paint = {
        'line-color': 'rgba(255, 0, 0, 0.6)',
        'line-width': 4
      };
    }
    map.addLayer({
      id: `${shapeType}-layer`,
      type: style_type,
      source: shapeType,
      paint: style_paint
    });

    const bbox = turf.bbox(geojson);
    map.fitBounds(bbox, { padding: 40 });

    // Wait for user decision via modal
    await showDownloadPromptModal(comidShape, shapeType); // Await user's click

    return true;
  } catch (err) {
    console.error("Error fetching shape:", err);
    showModal(`Unable to identify ${shapeType}.`, "Error");
    return false;
  }
}

function showDownloadPromptModal(comidShape, shapeType) {
  return new Promise((resolve) => {
    const shapeName = (shapeType.charAt(0).toUpperCase() + shapeType.slice(1)).replace('_', ' ');
    document.getElementById('downloadPromptMessage').textContent = `${shapeName} identified. Do you want to download it?`;
    const modalElement = document.getElementById('downloadPromptModal');
    const modal = new bootstrap.Modal(modalElement);
    modal.show();

    const yesBtn = document.getElementById('confirmDownloadBtn');
    const noBtn = modalElement.querySelector('.btn-secondary');

    let userResponse = null;

    const cleanUp = () => {
      yesBtn.removeEventListener('click', onYes);
      noBtn.removeEventListener('click', onNo);
      modalElement.removeEventListener('hidden.bs.modal', onHidden);
    };

    const onYes = () => {
      userResponse = true;
      modal.hide(); // Wait for modal to hide before downloading
    };

    const onNo = () => {
      userResponse = false;
      modal.hide(); // Still trigger hidden event
    };

    const onHidden = () => {
      cleanUp();
      if (userResponse === true) {
        // Safe to trigger download *after* modal is closed
        downloadShapeGeoJSON(comidShape, shapeType);
        resolve(true);
      } else {
        resolve(false);
      }
    };

    yesBtn.addEventListener('click', onYes);
    noBtn.addEventListener('click', onNo);
    modalElement.addEventListener('hidden.bs.modal', onHidden);
  });
}

export async function identifyAll(map, comidShape, lat, lng) {
  for (const shapeType of ["watershed", "upstream_rivers", "flowpath"]) {
    await fetchAndDisplayShape(map, comidShape, lat, lng, shapeType);
  }
}

function downloadShapeGeoJSON(comidShape, shapeType) {
  if (!currentShapeGeoJSON) {
    showModal(`No ${shapeType} data available.`, "Warning");
    return;
  }

  const blob = new Blob([JSON.stringify(currentShapeGeoJSON, null, 2)], {
    type: 'application/vnd.geo+json'
  });

  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  if (comidShape!=null) {
    a.download = `${shapeType}_${comidShape}.geojson`;
  } else {
    a.download = `${shapeType}.geojson`;
  }
  a.click();
  URL.revokeObjectURL(url);
}

