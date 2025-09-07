/**
 * @file Add layer control
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

// layer-control.js
// Combines layer state tracking and layer control UI

const layerVisibilityMap = new Map();

export function setupLayerControl(map, layers) {
  const toggleBtn = document.getElementById('layer-toggle-btn');
  const menu = document.getElementById('layer-menu');

  toggleBtn.addEventListener('click', (e) => {
    e.stopPropagation();
    menu.classList.toggle('hidden');
  });

  document.addEventListener('click', (e) => {
    if (!menu.classList.contains('hidden') && !menu.contains(e.target) && e.target !== toggleBtn) {
      menu.classList.add('hidden');
    }
  });

  const waitForLayers = (layerIds, callback) => {
    const interval = setInterval(() => {
      const allReady = layerIds.every(id => map.getLayer(id));
      if (allReady) {
        clearInterval(interval);
        callback();
      }
    }, 100);
  };

  waitForLayers(layers.map(l => l.id), () => {
    menu.innerHTML = ''; // Clear existing menu entries

    layers.forEach(({ id, name, initialVisibility, legendSymbol, legendColor }) => {
      const item = createLayerOption(map, id, name, initialVisibility, legendSymbol, legendColor);
      menu.appendChild(item);
    });
    
  });
}

export function restoreLayerVisibility(map, visibleLayers) {
  const waitForLayers = (layerIds, callback) => {
    const interval = setInterval(() => {
      const allReady = layerIds.every(id => map.getLayer(id));
      if (allReady) {
        clearInterval(interval);
        callback();
      }
    }, 100);
  };

  const layerIds = Array.from(layerVisibilityMap.keys());

  waitForLayers(layerIds, () => {
    layerIds.forEach(id => {
      const visible = layerVisibilityMap.get(id);
      if (map.getLayer(id)) {
        map.setLayoutProperty(id, 'visibility', visible ? 'visible' : 'none');
        const extras = ['highlight', 'labels', 'casing'];
        extras.forEach(extra => {
          if (map.getLayer(id+'-'+extra)) {
            map.setLayoutProperty(id+'-'+extra, 'visibility', visible ? 'visible' : 'none');
          }
        });
        // restore layer option colors in the layer menu
        const match = visibleLayers.find(layer => layer.id === id);
        const legendColor = match ? match.legendColor : 'gray';
        //console.log(`legendColor for layer ${id} is ${legendColor}.`);
        const label = document.querySelector(`label[data-layer-id="${id}"]`);
        if (label) {
          label.style.color = visible ? 'inherit' : 'gray';
        }
        const span = document.querySelector(`span[data-layer-id="${id}"]`);
        if (span) {
          span.style.color = visible ? legendColor : 'gray';
        }
        // update data overlay
        if (id=='dataoverlay') {
          document.getElementById('datepicker').dispatchEvent(new Event('change'));
        }
      }
    });
  });
      
}

export function getVisibleLayerIds() {
  return Array.from(layerVisibilityMap.entries()).map(([id, visible]) => ({ id, visible }));
}

export function clearTrackedLayers() {
  layerVisibilityMap.clear();
}

function createLayerOption(map, layerId, label, initialVisibility, legendSymbol, legendColor) {

  const visible = map.getLayoutProperty(layerId, 'visibility') !== 'none';
  layerVisibilityMap.set(layerId, visible);
  
  const wrapper = document.createElement('label');
  wrapper.className = 'layer-option';
  wrapper.setAttribute('data-layer-id', layerId);
  wrapper.style.color = initialVisibility ? 'inherit' : 'gray';

  const symbol = document.createElement('span');
  symbol.className = 'legend-symbol';
  symbol.setAttribute('data-layer-id', layerId);
  symbol.style.color = initialVisibility ? legendColor : 'gray';
  symbol.innerHTML = legendSymbol;

  const checkbox = document.createElement('input');
  checkbox.type = 'checkbox';
  checkbox.checked = visible;

  checkbox.addEventListener('change', () => {
    const isVisible = checkbox.checked;
    map.setLayoutProperty(layerId, 'visibility', isVisible ? 'visible' : 'none');
    const extras = ['highlight', 'labels', 'casing'];
    extras.forEach(extra => {
      if (map.getLayer(layerId+'-'+extra)) {
        map.setLayoutProperty(layerId+'-'+extra, 'visibility', isVisible ? 'visible' : 'none');
      }
    });
    layerVisibilityMap.set(layerId, isVisible);
    
    wrapper.style.color = isVisible ? 'inherit' : 'gray';
    symbol.style.color = isVisible ? legendColor : 'gray';
    
    // update data overlay
    //if (layerId=='dataoverlay') {
    //  document.getElementById('datepicker').dispatchEvent(new Event('change'));
    //}
        
    // update url hash
    const center = map.getCenter();
    const zoom = map.getZoom().toFixed(2);
    const bearing = map.getBearing().toFixed(1);
    const pitch = map.getPitch().toFixed(1);
    const projection = map.getProjection()?.type=='globe' ? 'globe' : 'mercator';
    const terrainEnabled = !!map.getTerrain();
    const terrain = terrainEnabled ? 1 : 0;
    const layers1 = getVisibleLayerIds()
        .map(({ id, visible }) => `${encodeURIComponent(id)}:${visible}`)
        .join(',');
    const date = $('#datepicker').datepicker('getDate').toISOString().slice(0, 10);
    const variable = document.getElementById('variable-selector').value;
    const sourceSelector = document.getElementById('source-selector');
    var source = 'nrt';
    if (sourceSelector.disabled==false) {
      source = sourceSelector.value;
    }

    var style = 'carto';
    document.querySelectorAll('.style-btn').forEach(button => {
      if (button.classList.contains('active')) {
        style = button.getAttribute('data-style');
      }
    });
    const hash = `${center.lat.toFixed(5)},${center.lng.toFixed(5)},${zoom},${bearing},${pitch},${style},${projection},${terrain}!${layers1}!${date},${variable},${source}`;
    // Replaces current history entry instead of adding a new one
    history.replaceState(null, '', `#${hash}`);
  });

  wrapper.appendChild(checkbox);
  wrapper.appendChild(symbol);
  wrapper.append(` ${label}`);

  return wrapper;
}
