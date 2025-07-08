/**
 * @file Load and create a plotly line graph up click on MERIT-Basins river reach on a map for NRT data.
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { setupMap } from './map.js';
import { setupNwmRivers } from './plot-nwm-rivers.js';
import { setupOverlayControl } from './overlay-control.js';
import { setupMenu } from './menu.js';

// set up the map
const map = await setupMap();

// set up NWM
// await setupNwmRivers(map);

// set up data image overlay
setupOverlayControl(map);

// set up tool menu
setupMenu(map);

