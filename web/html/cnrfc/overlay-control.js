/**
 * @file Add data image overlay control
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { loadJson } from './utils.js';
import { setupColormap } from './colormap.js';

var latestNrtDate;

export const variableImages = {
  precip: {
    folder: 'forcing',
    timestamp: 'day',
    colorStops: [
      { threshold: 750, color: "rgba(102,  51,   0)" },
      // { threshold: 600, color: "rgba(160, 108,  60)" },
      { threshold: 500, color: "rgba(218, 166, 120)" },
      { threshold: 400, color: "rgba(238, 212, 188)" },
      { threshold: 300, color: "rgba(224, 224, 224)" },
      { threshold: 250, color: "rgba(128, 128, 128)" },
      { threshold: 200, color: "rgba( 32,  32,  32)" },
      { threshold: 150, color: "rgba( 32,  32, 128)" },
      { threshold: 100, color: "rgba( 64,  64, 255)" },
      { threshold:  70, color: "rgba(128,  32, 255)" },
      { threshold:  50, color: "rgba(240,  64, 255)" },
      { threshold:  40, color: "rgba(255,  32, 128)" },
      { threshold:  30, color: "rgba(255,   0,   0)" },
      { threshold:  20, color: "rgba(255, 160,   0)" },
      { threshold:  15, color: "rgba(255, 255,   0)" },
      { threshold:  10, color: "rgba(128, 224,   0)" },
      { threshold: 7.5, color: "rgba(  0, 192,   0)" },
      { threshold:   5, color: "rgba(  0, 224, 128)" },
      { threshold: 2.5, color: "rgba(  0, 255, 255)" },
      { threshold:   1, color: "rgba( 80, 208, 208)" },
      { threshold:null, color: "rgba(235, 235, 235)" } 
    ],
    legendTitle: 'mm'
  },
  precip_r: {
    folder: 'forcing',
    timestamp: 'month',
    colorStops: [
       { threshold:   99, color: "rgba( 80,  35, 130)"},
       { threshold:   95, color: "rgba(119, 100, 164)"},
       { threshold:   90, color: "rgba(163, 154, 198)"},
       { threshold:   80, color: "rgba(202, 201, 226)"},
       { threshold:   65, color: "rgba(233, 233, 241)"},
       { threshold:   35, color: "rgba(255, 255, 255)"},
       { threshold:   20, color: "rgba(253, 209, 152)"},
       { threshold:   10, color: "rgba(244, 168,  75)"},
       { threshold:    5, color: "rgba(215, 121,  17)"},
       { threshold:    1, color: "rgba(173,  85,   6)"},
       { threshold: null, color: "rgba(127,  59,   8)"}
    ],
    legendTitle: '&percnt;ile'
  },
  tair2m: {
    folder: 'forcing',
    timestamp: 'day',
    colorStops: [
       { threshold:  39, color: "rgba(255,   0,   0)" },
       { threshold:  36, color: "rgba(255,  46,  23)" },
       { threshold:  33, color: "rgba(255,  92,  46)" },
       { threshold:  30, color: "rgba(255, 134,  69)" },
       { threshold:  27, color: "rgba(255, 171,  92)" },
       { threshold:  24, color: "rgba(232, 203, 113)" },
       { threshold:  21, color: "rgba(202, 228, 134)" },
       { threshold:  18, color: "rgba(172, 245, 153)" },
       { threshold:  15, color: "rgba(142, 253, 171)" },
       { threshold:  12, color: "rgba(112, 253, 188)" },
       { threshold:   9, color: "rgba( 82, 245, 203)" },
       { threshold:   6, color: "rgba( 52, 228, 216)" },
       { threshold:   3, color: "rgba( 22, 203, 228)" },
       { threshold:   0, color: "rgba(  7, 171, 237)" },
       { threshold:  -3, color: "rgba( 37, 134, 245)" },
       { threshold:  -6, color: "rgba( 67,  92, 250)" },
       { threshold:  -9, color: "rgba( 97,  46, 253)" },
       { threshold: -12, color: "rgba(127,   0, 255)" }
    ],
    legendTitle: '&deg;C'
  },
  tair2m_r: {
    folder: 'forcing',
    timestamp: 'month',
    colorStops: [
       { threshold:   99, color: "rgba(211,  77,  64)"},
       { threshold:   95, color: "rgba(234, 123,  96)"},
       { threshold:   90, color: "rgba(245, 161, 130)"},
       { threshold:   80, color: "rgba(245, 193, 168)"},
       { threshold:   65, color: "rgba(231, 214, 204)"},
       { threshold:   35, color: "rgba(255, 255, 255)"},
       { threshold:   20, color: "rgba(180, 205, 250)"},
       { threshold:   10, color: "rgba(148, 181, 254)"},
       { threshold:    5, color: "rgba(116, 151, 245)"},
       { threshold:    1, color: "rgba( 86, 115, 224)"},
       { threshold: null, color: "rgba( 58,  76, 192)"}
    ],
    legendTitle: '%ile'
  },
  smtot_r: {
    folder: 'output',
    timestamp: 'day',
    colorStops: [
      { threshold:   98, color: "rgba(  0,  38, 115)" },
      { threshold:   95, color: "rgba( 20,  90,   0)" },
      { threshold:   90, color: "rgba( 56, 168,   0)" },
      { threshold:   80, color: "rgba( 76, 230,   0)" },
      { threshold:   70, color: "rgba(170, 245, 150)" },
      { threshold:   30, color: "rgba(255, 255, 255)" },
      { threshold:   20, color: "rgba(254, 254,   0)" },
      { threshold:   10, color: "rgba(254, 211, 127)" },
      { threshold:    5, color: "rgba(230, 152,   0)" },
      { threshold:    2, color: "rgba(230,   0,   0)" },
      { threshold: null, color: "rgba(115,   0,   0)" }
    ],
    legendTitle: '&percnt;ile'
  },
  swe_r: {
    folder: 'output',
    timestamp: 'day',
    colorStops: [
      { threshold:   99, color: "rgba( 46,  46, 180)" },
      { threshold:   95, color: "rgba( 93,  93, 255)" },
      { threshold:   90, color: "rgba(139, 139, 255)" },
      { threshold:   80, color: "rgba(185, 185, 255)" },
      { threshold:   65, color: "rgba(215, 215, 255)" },
      { threshold:   35, color: "rgba(255, 232,  93)" },
      { threshold:   20, color: "rgba(255, 185, 185)" },
      { threshold:   10, color: "rgba(255, 139, 139)" },
      { threshold:    5, color: "rgba(255,  93,  93)" },
      { threshold:    1, color: "rgba(255,  46,  46)" },
      { threshold: null, color: "rgba(180,   0,   0)" }
    ],
    legendTitle: '&percnt;ile'
  }
};

export const cnrfcCoords = [
          [-125, 44], // top-left corner
          [-113, 44], // top-right corner
          [-113, 32], // bottom-right corner
          [-125, 32]  // bottom-left corner
        ];

export async function setupOverlayControl(map, layerOverlay='dataoverlay') {

  const dateInput = document.getElementById('datepicker');
  const varibleSelector = document.getElementById('variable-selector');
  const sourceSelector = document.getElementById('source-selector');

  dateInput.addEventListener('change', () => {
    updateOverlay(map, layerOverlay);
  });

  $(dateInput).on('changeDate', () => {
    dateInput.dispatchEvent(new Event('change'));
  });

  varibleSelector.addEventListener('change', () => {
    dateInput.dispatchEvent(new Event('change'));
  });
  sourceSelector.addEventListener('change', () => {
    dateInput.dispatchEvent(new Event('change'));
  });

  const statusJson = await loadJson('https://cw3e.ucsd.edu/hydro/cnrfc/csv/status.json');
  const latestNrt = statusJson['WRF-Hydro NRT'];
  latestNrtDate = new Date(latestNrt);
  const lastFcstDate = new Date(latestNrt);
  lastFcstDate.setDate(lastFcstDate.getDate() + 16);
  
  // initialize datepicker
  $(document).ready(function () {
    $('#datepicker-wrapper input').datepicker({
      format: 'yyyy-mm-dd',
      autoclose: true,
      todayHighlight: true,
      startDate: '2023-10-01',
      endDate: lastFcstDate.toISOString().split('T')[0] //latestNrt
    }); //.datepicker('setDate', latestNrtDate.toISOString().split('T')[0]);

    // IMPORTANT: set date to the default start (latest NRT) only if not loading state from hash
    // it needs to work both on initial load (empty hash) and state-in-hash load
    const hash = window.location.hash.replace(/^#/, '');
    const [viewPart, layerPart, overlayPart] = hash.split('!');
    //console.log(`Hash status: ${overlayPart}`);
    if (overlayPart==null) {
      $('#datepicker-wrapper input').datepicker('setDate', latestNrtDate.toISOString().split('T')[0]);
    } else {
      const [tmpdate, tmpvariable, tmpsource] = overlayPart?.split(',') || [];
      const [tmpyear, tmpmonth, tmpday] = tmpdate.split('-').map(Number);
      const dateObj = new Date(tmpyear, tmpmonth - 1, tmpday);
      $('#datepicker-wrapper input').datepicker('setDate', dateObj);
    }
    updateNavButtons();
  });
  // console.log(`In setupOverlayControl(): ${latestNrt}, ${latestNrtDate}, ${latestNrtDate.toISOString().split('T')[0]}`);

  // Initial load
  //updateOverlay(map, layerOverlay, 'smtot_r', latestNrtDate);

  document.getElementById('prev-day').addEventListener('click', () => shiftDate({ days: -1 }));
  document.getElementById('next-day').addEventListener('click', () => shiftDate({ days: 1 }));
  document.getElementById('prev-month').addEventListener('click', () => shiftDate({ months: -1 }));
  document.getElementById('next-month').addEventListener('click', () => shiftDate({ months: 1 }));

}

export function updateOverlay(map, layerOverlay='dataoverlay') {

  const dateInput = document.getElementById('datepicker');
  const varibleSelector = document.getElementById('variable-selector');
  const sourceSelector = document.getElementById('source-selector');

  const input = dateInput.value;
  //const input = $('#datepicker').datepicker('getDate');
  var   dataDate = new Date(input);
  //console.log(`In updateOverlay() 1: ${input}, ${dataDate}`);

  if (isNaN(dataDate)) dataDate = latestNrtDate;
  // Read selected variable
  const variable = varibleSelector.value;
  //console.log(`In updateOverlay() 2: ${dataDate}`);

  const { folder, timestamp, colorStops, legendTitle } = variableImages[variable];
  const ymd = dataDate.toISOString().split('T')[0].replaceAll('-', '');
  const y = ymd.slice(0, 4);
  var tString = ymd;
  if (timestamp=='month') tString = ymd.slice(0, 6);
  var ptype = 'nrt';
  if (dataDate>latestNrtDate) {
    sourceSelector.disabled = false;
    sourceSelector.style.backgroundColor = 'yellow';
    sourceSelector.style.color = 'black';
    ptype = sourceSelector.value;
  } else {
    sourceSelector.disabled = true;
    sourceSelector.style.backgroundColor = '#eee';
    sourceSelector.style.color = '#999';
  }
  const imgUrl = `https://cw3e.ucsd.edu/hydro/cnrfc/imgs/${ptype}/${folder}/${y}/${variable}_${tString}.png`;

  if (map.getSource(layerOverlay+'_source')) {
    //console.log(`Setting url to ${imgUrl}`);
    map.getSource(layerOverlay+'_source').updateImage({url: imgUrl, coordinates: cnrfcCoords});
  }
  // update color map
  setupColormap(colorStops, legendTitle);
}

function shiftDate({ days = 0, months = 0 }) {
  const input = $('#datepicker');
  const oldDate = input.datepicker('getDate');
  if (!oldDate) return;

  const newDate = new Date(oldDate);
  newDate.setMonth(newDate.getMonth() + months);
  newDate.setDate(newDate.getDate() + days);

  // Clamp to allowed range
  const minDate = input.datepicker('getStartDate');
  const maxDate = input.datepicker('getEndDate');
  if (newDate < minDate || newDate > maxDate) return;

  const yyyy = newDate.getFullYear();
  const mm = String(newDate.getMonth() + 1).padStart(2, '0');
  const dd = String(newDate.getDate()).padStart(2, '0');
  const newDateStr = `${yyyy}-${mm}-${dd}`;

  input.datepicker('update', newDateStr);
  input.trigger('changeDate');
  updateNavButtons();
}

function updateNavButtons() {
  const input = $('#datepicker');
  const currentDate = input.datepicker('getDate');
  const minDate = input.datepicker('getStartDate');
  const maxDate = input.datepicker('getEndDate');
  //console.log(`Current: ${currentDate}\nmaxDate: ${maxDate}`);

  const prevDayBtn = document.getElementById('prev-day');
  const nextDayBtn = document.getElementById('next-day');
  const prevMonthBtn = document.getElementById('prev-month');
  const nextMonthBtn = document.getElementById('next-month');

  // Helper function to clone date safely
  const addDays = (date, d) => new Date(date.getFullYear(), date.getMonth(), date.getDate() + d);
  const addMonths = (date, m) => new Date(date.getFullYear(), date.getMonth() + m, date.getDate());

  // Disable based on day skip
  prevDayBtn.disabled = addDays(currentDate, -1) < minDate;
  nextDayBtn.disabled = addDays(currentDate, 1) > maxDate;

  // Disable based on month skip
  prevMonthBtn.disabled = addMonths(currentDate, -1) < minDate;
  nextMonthBtn.disabled = addMonths(currentDate, 1) > maxDate;
}
