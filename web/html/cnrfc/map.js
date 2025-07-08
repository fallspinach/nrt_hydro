/**
 * @file setup map
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { formatLatLng, showModal, loadJson } from './utils.js';
import { setupLayerControl, restoreLayerVisibility, getVisibleLayerIds } from './layer-control.js';
import { loadLayerCnrfc } from './load-layer-cnrfc.js';
import { loadLayerCnrfcBasins } from './load-layer-cnrfc-basins.js';
import { loadLayerNwmRivers } from './load-layer-nwm-rivers.js';
import { loadLayerDwrPoints } from './load-layer-dwr-points.js';
import { loadLayerSnowNetwork } from './load-layer-snow-network.js';
import { loadLayerOverlay } from './load-layer-overlay.js';

let flagDebug = false;

function isAsync(fn) {
  return typeof fn === 'function' && fn.constructor.name === 'AsyncFunction';
}

export async function setupMap(containerID='map', coordsID='mouse-coords') {

  const styleURLBase = 'https://cw3e.ucsd.edu/hydro/styles/';
  let currentStyle = 'carto';
  let currentProjection = 'mercator';
  let terrainEnabled = false;

  const visibleLayers = [
    { id: 'dataoverlay', name: 'Data Overlay',          load: loadLayerOverlay,     initialVisibility: true,  legendSymbol: '&#9632;',  legendColor: 'tomato' },
    { id: 'cnrfc',       name: 'CNRFC',                 load: loadLayerCnrfc,       initialVisibility: true,  legendSymbol: '&#9634;',  legendColor: 'gray' },
    { id: 'cnbasins',    name: 'CNRFC Fcst Basins',     load: loadLayerCnrfcBasins, initialVisibility: true,  legendSymbol: '&#9634;',  legendColor: 'blue' },
    { id: 'nwmrivers',   name: 'NWM Rivers (v2.1)',     load: loadLayerNwmRivers,   initialVisibility: true,  legendSymbol: '&#65374;', legendColor: 'darkcyan' },
    { id: 'dwrofficial', name: 'Official Points',       load: loadLayerDwrPoints,   initialVisibility: true,  legendSymbol: '&#9679;',  legendColor: 'magenta' },
    { id: 'dwrreservoir',name: 'Reservoir Points',      load: loadLayerDwrPoints,   initialVisibility: true,  legendSymbol: '&#9679;',  legendColor: 'blue' },
    { id: 'dwrother',    name: 'Other Points',          load: loadLayerDwrPoints,   initialVisibility: false, legendSymbol: '&#9679;',  legendColor: 'tan' },
    { id: 'dwrens',      name: 'Ens Points',            load: loadLayerDwrPoints,   initialVisibility: false, legendSymbol: '&#9679;',  legendColor: 'plum' },
    { id: 'snowcourse',  name: 'Snow Courses',          load: loadLayerSnowNetwork, initialVisibility: false, legendSymbol: '*',        legendColor: 'brown' },
    { id: 'snowpillow',  name: 'Snow Pillows',          load: loadLayerSnowNetwork, initialVisibility: false, legendSymbol: '*',        legendColor: 'darkorange' }
  ];

  function updateURLHash() {
    const center = map.getCenter();
    const zoom = map.getZoom().toFixed(2);
    const bearing = map.getBearing().toFixed(1);
    const pitch = map.getPitch().toFixed(1);
    currentProjection = map.getProjection()?.type=='globe' ? 'globe' : 'mercator';
    terrainEnabled = !!map.getTerrain();
    const terrain = terrainEnabled ? 1 : 0;
    const layers = getVisibleLayerIds()
      .map(({ id, visible }) => `${encodeURIComponent(id)}:${visible}`)
      .join(',');
    const date = $('#datepicker').datepicker('getDate').toISOString().slice(0, 10);
    const variable = document.getElementById('variable-selector').value;

    const hash = `${center.lat.toFixed(5)},${center.lng.toFixed(5)},${zoom},${bearing},${pitch},${currentStyle},${currentProjection},${terrain}!${layers}!${date},${variable}`;
    
    // Replaces current history entry instead of adding a new one
    history.replaceState(null, '', `#${hash}`);
  }

  // function to restore map view
  function loadMapStateFromHash() {
    const hash = window.location.hash.replace(/^#/, '');
    const [viewPart, layerPart, overlayPart] = hash.split('!');
    const viewParams = viewPart?.split(',') || [];
    const layers = layerPart?.split(',') || [];
    const [date, variable] = overlayPart?.split(',') || [];

    const lat = parseFloat(viewParams[0]);
    const lng = parseFloat(viewParams[1]);
    const zoom = parseFloat(viewParams[2]);
    const bearing = parseFloat(viewParams[3]);
    const pitch = parseFloat(viewParams[4]);
    const style = viewParams[5];
    const projection = viewParams[6];
    const terrain = (viewParams.length>7) ? parseInt(viewParams[7]) : 1;

    const layerStates = layers.map(entry => {
      const [id, vis] = entry.split(':');
      return { id, visible: vis === 'true' };
    });
    
    if (flagDebug) {
      var debug_text = `viewParams.length=${viewParams.length}, layers.length=${layers.length}<br>`;
      viewParams.forEach((p) => {
        debug_text += `${p}<br>`;
      });
      layers.forEach((p) => {
        debug_text += `${p}<br>`;
      });
      showModal(debug_text);
    }
    if (viewParams.length>=7) {

      currentStyle = style; // update tracking variable
      map.setStyle(styleURLBase+style+'.json');
      currentProjection = projection;
      terrainEnabled = (terrain==1) ? true : false;

      map.once('style.load', () => {
        map.jumpTo({
          center: [lng, lat],
          zoom: zoom,
          bearing: bearing,
          pitch: pitch
        });
      
        // must update it again here since it may get changed by the time map.once('style.load') is called
        terrainEnabled = (terrain==1) ? true : false;
        applyTerrainStatus(terrainEnabled);
        
        setupLayerControl(map, visibleLayers);
        waitForLayerMenuAndSync(layerStates);
      
        // Re-highlight the correct button
        document.querySelectorAll('.style-btn').forEach(btn => {
          btn.classList.toggle('active', btn.getAttribute('data-style') === style);
        });
      });
    
      map.on('style.load', () => {
        map.setProjection({'type': projection});
        layerStates.forEach(({ id, visible }) => {
          map.setLayoutProperty(id, 'visibility', visible ? 'visible' : 'none');
          const extras = ['highlight', 'labels', 'casing'];
          extras.forEach(extra => {
            if (map.getLayer(id+'-'+extra)) {
              map.setLayoutProperty(id+'-'+extra, 'visibility', visible ? 'visible' : 'none');
            }
          });
          // update data overlay
          //if (id=='dataoverlay') {
          //  document.getElementById('datepicker').dispatchEvent(new Event('change'));
          //}
        });
        // console.log(`date = ${date}; variable =  ${variable}`);
        const [year, month, day] = date.split('-').map(Number);
        const dateObj = new Date(year, month - 1, day);
        $('#datepicker').datepicker('setDate', dateObj);
        document.getElementById('variable-selector').value = variable;
      });
      setupLayerControl(map, visibleLayers);
    }

  }

  function updateLayerMenuColors(visibleState) {
    visibleState.forEach(({ id, visible }) => {
      const match = visibleLayers.find(layer => layer.id === id);
      const legendColor = match ? match.legendColor : 'gray';
      const label = document.querySelector(`label[data-layer-id="${id}"]`);
      if (label) {
      label.style.color = visible ? 'inherit' : 'gray';
      }
      const span = document.querySelector(`span[data-layer-id="${id}"]`);
      if (span) {
        span.style.color = visible ? legendColor : 'gray';
      }
    });
  }

  function waitForLayerMenuAndSync(visibleState, timeout = 5000) {
    const start = Date.now();
    const check = setInterval(() => {
      const allReadyLabel = visibleState.every(({ id }) =>
        document.querySelector(`label[data-layer-id="${id}"]`)
      );
      const allReadySpan  = visibleState.every(({ id }) =>
        document.querySelector(`span[data-layer-id="${id}"]`)
      );

      if (allReadyLabel&&allReadySpan) {
        clearInterval(check);
        updateLayerMenuColors(visibleState);
      } else if (Date.now() - start > timeout) {
       clearInterval(check);
        console.warn('Layer control menu items not found in time');
      }
    }, 100);
  }

  function applyTerrainStatus(terrainStatus) {
    map.once('idle', () => {
      // Match the terrain toggle button regardless of its current state
      const globeButton = document.querySelector(
        '.maplibregl-ctrl-terrain, .maplibregl-ctrl-terrain-enabled'
      );

      if (!globeButton) {
        console.warn("Terrain control button not found.");
        return;
      }

      const isActive = globeButton.classList.contains('maplibregl-ctrl-terrain-enabled');

      if (terrainStatus && !isActive) {
        globeButton.click(); // turn ON terrain
      } else if (!terrainStatus && isActive) {
        globeButton.click(); // turn OFF terrain
      }
    });
  }

  // add the PMTiles plugin to the maplibregl global.
  const protocol = new pmtiles.Protocol();
  maplibregl.addProtocol('pmtiles', protocol.tile);

  const PMTILES_URL = 'https://cw3e.ucsd.edu/hydro/cnrfc/pmtiles/nwm_reaches_cnrfc.pmtiles';

  const p = new pmtiles.PMTiles(PMTILES_URL);

  // this is so we share one instance across the JS code and the map renderer
  protocol.add(p);

  const map = new maplibregl.Map({
    container: containerID,
    zoom: 5.3,
    center: [-119, 38.1],
    style: styleURLBase+currentStyle+'.json'
  });

  maplibregl.setRTLTextPlugin('https://api.mapbox.com/mapbox-gl-js/plugins/mapbox-gl-rtl-text/v0.2.1/mapbox-gl-rtl-text.js');

  map.on('load', () => {
    map.addControl(new maplibregl.FullscreenControl());
    map.addControl(new maplibregl.NavigationControl({
        visualizePitch: true,
        showZoom: true,
        showCompass: true
        })
    );
    map.addControl(new maplibregl.TerrainControl({
        source: 'terrain_source'
        })
    );
    map.addControl(new maplibregl.GlobeControl());
    
    const scale = new maplibregl.ScaleControl({
      maxWidth: 100,         // Optional: width of scale bar in pixels
      unit: 'metric'         // 'imperial', 'nautical' also available
    });
    map.addControl(scale, 'bottom-left');
    
  });

  map.on('mousemove', (e) => {
    const lat = e.lngLat.lat.toFixed(5);
    const lng = e.lngLat.lng.toFixed(5);
    const formatted = formatLatLng(lat, lng);
    document.getElementById(coordsID).textContent = formatted;
  });

  // update URL hash when map view changes
  map.on('moveend', updateURLHash);
  map.on('pitchend', updateURLHash);
  map.on('rotateend', updateURLHash);

  // enable copy view url button
  document.getElementById('copy-view-btn').addEventListener('click', () => {
    updateURLHash();
    const fullURL = `${location.origin}${location.pathname}${location.search}${location.hash}`;
    navigator.clipboard.writeText(fullURL).then(() => {
      showModal(`Map view <a href="${fullURL}">URL</a> is copied to clipboard! You can also use this QR code:<br><div id="qr-code" style="margin-top: 10px; display: flex; justify-content: center;"></div>`, 'Bookmark/Share');
      const container = document.getElementById('qr-code');
      container.innerHTML = ''; // clear any old QR code
      new QRCode(container, {
        text: fullURL,
        width: 200,
        height: 200,
        colorDark: "#000000",
        colorLight: "#ffffff",
        correctLevel: QRCode.CorrectLevel.M
      });
    }).catch(err => {
      showModal("Failed to copy map view URL.", "Error");
    });
  });

  // enable switching map styles
  document.querySelectorAll('.style-btn').forEach(button => {
    button.addEventListener('click', () => {
      // Remove active state from all buttons
      document.querySelectorAll('.style-btn').forEach(btn => btn.classList.remove('active'));
      button.classList.add('active');

      // record projection
      currentProjection = map.getProjection()?.type=='globe' ? 'globe' : 'mercator';
      
      // Set the new map style
      currentStyle = button.getAttribute('data-style');
      map.setStyle(styleURLBase+currentStyle+'.json');
      // restore layers
      restoreLayerVisibility(map, visibleLayers);

      // restore projection
      map.on('style.load', () => {
        map.setProjection({'type': currentProjection});
      });
      // restore layers
      restoreLayerVisibility(map, visibleLayers);
      
      // update url hash
      updateURLHash();

    });
  });

  // load layers
  visibleLayers.forEach( ({id, name, load, initialVisibility, legendColor}) => {
    load(map, id, initialVisibility, legendColor);
  });

  // set up layer control
  setupLayerControl(map, visibleLayers);

  // load view if provided
  loadMapStateFromHash();

  return map;

}

