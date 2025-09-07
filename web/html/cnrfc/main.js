/**
 * @file Load and create a plotly line graph up click on MERIT-Basins river reach on a map for NRT data.
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { setupMap } from './map.js';
import { setupNwmRivers } from './plot-nwm-rivers.js';
import { setupMenu } from './menu.js';
import { setupBasinTs } from './plot-basin-ts.js';

// set up the map
const map = await setupMap();

// set up NWM
// await setupNwmRivers(map);

// set up tool menu
setupMenu(map);

// set up basin time series
const fcstGroups = ['dwrofficial', 'dwrreservoir', 'dwrother', 'dwrens'];
fcstGroups.forEach(fcstGroup => {
      setupBasinTs(map, fcstGroup);
});
