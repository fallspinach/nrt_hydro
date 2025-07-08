/**
 * @file Display both NWM rivers and WBD watersheds on a map and show attributes on mouse hover.
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

// add the PMTiles plugin to the maplibregl global.
const protocol = new pmtiles.Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

const PMTILES_URL = 'https://cw3e.ucsd.edu/wrf_hydro/conus/pmtiles/nwm_reaches_conus.pmtiles';

const p = new pmtiles.PMTiles(PMTILES_URL);

// this is so we share one instance across the JS code and the map renderer
protocol.add(p);

// we first fetch the header so we can get the center lon, lat of the map.
p.getHeader().then(h => {
    const map = new maplibregl.Map({
        container: 'map',
        zoom: 3,
        center: [-100, 40],
        style: 'https://cw3e.ucsd.edu/wrf_hydro/styles/nwm_wbdhu_carto.json'
    });

    const popup = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false
    });

    map.on('mousemove', 'watersheds', (e) => {
      map.getCanvas().style.cursor = 'pointer';
      const feature = e.features[0];
      const id = feature.globalid || feature.properties.globalid;

      map.setFilter('watersheds-highlight', ['==', 'globalid', id]);

      for (let h=2; h<=12; h+=2) {
          var hucid = feature.properties['huc'+h];
          if (hucid!=null) {
              popuptext = `<strong>HUC ID: ${hucid}</strong><br/>`;
          }
      }
      popuptext += `Name: ${feature.properties.name}<br/>`;
      popuptext += `Area: ${feature.properties.areasqkm} km<sup>2</sup>`;

      popup.setLngLat(e.lngLat)
        .setHTML(popuptext)
        .addTo(map);
    });

    map.on('mouseleave', 'watersheds', () => {
      map.getCanvas().style.cursor = '';
      map.setFilter('watersheds-highlight', ['==', 'globalid', '']);
      popup.remove();
    });
    
    map.on('mousemove', 'rivers', (e) => {
      map.getCanvas().style.cursor = 'pointer';
      const feature = e.features[0];
      const id = feature.feature_id || feature.properties.feature_id;

      map.setFilter('rivers-highlight', ['==', 'feature_id', id]);

      popuptext = `<strong>River ID: ${id}</strong><br/>`;
      popuptext += `Name: ${feature.properties.gnis_name}<br/>`;
      popuptext += `Source: ${feature.properties.source}<br/>`;
      popuptext += `Length: ${(feature.properties.Shape_Length*111.1).toFixed(1)} km<br/>`;
      popuptext += `Stream Order: ${feature.properties.stream_order}`;

      popup.setLngLat(e.lngLat)
        .setHTML(popuptext)
        .addTo(map);
    });

    map.on('mouseleave', 'rivers', () => {
      map.getCanvas().style.cursor = '';
      map.setFilter('rivers-highlight', ['==', 'feature_id', '']);
      popup.remove();
    });

});

