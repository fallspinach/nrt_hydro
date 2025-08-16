/**
 * @file utility functions
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

export function formatLatLng(lat, lng) {
  const latAbs = Math.abs(lat).toFixed(4);
  const lngAbs = Math.abs(lng).toFixed(4);

  const latDir = lat >= 0 ? 'N' : 'S';
  const lngDir = lng >= 0 ? 'E' : 'W';

  return `${latAbs}°${latDir}, ${lngAbs}°${lngDir}`;
}

export function showModal(message, title = "Notice") {
  document.getElementById('messageModalLabel').textContent = title;
  document.getElementById('messageModalBody').innerHTML = message;
  const modal = new bootstrap.Modal(document.getElementById('messageModal'));
  modal.show();
}

export async function loadJson(url) {
  try {
    const response = await fetch(url, { cache: 'no-cache' });
    if (!response.ok) {
      throw new Error(`Failed to load JSON (${response.status} ${response.statusText})`);
    }
    const data = await response.json();
    return data;  // This is your parsed dictionary (JavaScript object)
  } catch (error) {
    console.error("Error loading JSON:", error);
    return null;
  }
}

export function generateDateArray(start, end, freq='daily') {
  const [sy, sm, sd] = start.split("-").map(Number);
  const [ey, em, ed] = end  .split("-").map(Number);
  const dates = [];

  // monthIndex is zero‑based
  let curr = new Date(sy, sm - 1, sd);
  const endDate = new Date(ey, em - 1, ed);

  while (curr <= endDate) {
    dates.push(new Date(curr));
    if (freq=='yearly') {
      curr.setFullYear(curr.getFullYear() + 1);
    } else if (freq=='monthly') {
      curr.setMonth(curr.getMonth() + 1);
    } else {
      curr.setDate(curr.getDate() + 1);
    }
  }

  return dates;
}

export async function loadDwrPoints(csvUrl) {
  const response = await fetch(csvUrl);
  const csvText = await response.text();

  const data = Papa.parse(csvText, {
    header: true,
    skipEmptyLines: true
  }).data;

  const geojson = {
    type: 'FeatureCollection',
    features: data.map(row => ({
      type: 'Feature',
      properties: {
        ID: row.ID,
        Location: row.Location,
        River: row.River,
        ReachID: row.ReachID //,
        //link: row.link
      },
      geometry: {
        type: 'Point',
        coordinates: [parseFloat(row.Longitude), parseFloat(row.Latitude)]
      }
    }))
  };

  return geojson;
}

export async function loadSnowNetwork(csvUrl) {
  const response = await fetch(csvUrl);
  const csvText = await response.text();

  const data = Papa.parse(csvText, {
    header: true,
    skipEmptyLines: true
  }).data;

  const geojson = {
    type: 'FeatureCollection',
    features: data.map(row => ({
      type: 'Feature',
      properties: {
        STA: row.STA,
        StationName: row.StationName,
        Elevation: row.Elevation,
        BasinName: row.BasinName,
        HydroArea: row.HydroArea
      },
      geometry: {
        type: 'Point',
        coordinates: [parseFloat(row.Longitude), parseFloat(row.Latitude)]
      }
    }))
  };

  return geojson;
}

export function getYesterday() {

  const today = new Date();
  const offset = today.getTimezoneOffset() + 10*60;
  const todayLocal = new Date(today.getTime() - (offset*60*1000));
  const yesterday = new Date(todayLocal);
  yesterday.setDate(todayLocal.getDate() - 1);

  return yesterday;
}


