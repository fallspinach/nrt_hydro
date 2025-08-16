/**
 * @file fetch GRADES-hydroDL data and draw plotly figures on map click
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { loadJson, generateDateArray } from './utils.js';

async function fetchGrades(hydrography, comid, dindex) {
  // GRADES-hydrDL binary data
  const grades_url = 'https://cw3e.ucsd.edu/hydro/grades_hydrodl/bin';
  const grades_json = await loadJson(`${grades_url}/grades_hydrodl.json`);
  var ndays = grades_json[hydrography]['ndays'];
  var start = grades_json[hydrography]['start'];
  var end   = grades_json[hydrography]['end'];
  if (hydrography=='MERIT') {
    var pfaf2 = Math.floor(comid/1000000);
    var cnt = comid - pfaf2*1000000 - 1;
    var url = `${grades_url}/GRADES-hydroDL_${pfaf2}.bin`;
    var byteOffset = cnt*ndays*4;
  }
  else {
    var pfaf1 = Math.floor(comid/10000000000);
    var url = `${grades_url}/SWORD_GRADES-hydroDL_${pfaf1}.bin`;
    var byteOffset = dindex*ndays*4;
  }
  const dates = generateDateArray(start, end);
  const byteLength = ndays*4;
  const response = await fetch(url, {
    headers: {
      'Range': `bytes=${byteOffset}-${byteOffset + byteLength - 1}`
    }
  });

  if (!response.ok && response.status !== 206) {
    throw new Error(`Failed to fetch byte range: ${response.status}`);
  }

  const arrayBuffer = await response.arrayBuffer();

  // Interpret the ArrayBuffer as a Float32Array
  const floatArray = new Float32Array(arrayBuffer);
  return {data: floatArray, dates: dates};
}


export async function setupSnowNetwork(map, network) {

  const gradesJson = await loadJson('https://cw3e.ucsd.edu/hydro/grades_hydrodl/bin/grades_hydrodl.json');
  const endDate = gradesJson['MERIT']['end'];

  // initialize datepicker
  $(document).ready(function () {
    $('#datepicker-wrapper input').datepicker({
      format: 'yyyy-mm-dd',
      autoclose: true,
      todayHighlight: true,
      startDate: endDate,
      endDate: endDate
    }).datepicker('setDate', endDate);
  });

  map.on('click', 'snowcourse', (e) => {

    const popupContent = `
  <ul class="nav nav-tabs" id="popupTab" role="tablist">
    <li class="nav-item" role="presentation">
      <button class="nav-link active" id="tab1-tab" data-bs-toggle="tab" data-bs-target="#tab1"
        type="button" role="tab">Recent</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="tab2-tab" data-bs-toggle="tab" data-bs-target="#tab2"
        type="button" role="tab">Retrospective</button>
    </li>
  </ul>
  <div class="tab-content mt-2">
    <div class="tab-pane fade show active" id="tab1" role="tabpanel">
      <button id="download-grades-nrt">Download CSV data</button><div id="plotly-nrt"></div>
    </div>
    <div class="tab-pane fade" id="tab2" role="tabpanel">
      <button id="download-grades-retro">Download CSV data</button><div id="plotly-retro"></div>
    </div>
  </div>
`;
    new maplibregl.Popup({maxWidth: '1110px'})
        .setLngLat(e.lngLat)
        .setHTML(popupContent)
        .addTo(map);

    var title = `GRADES-hydroDL, COMID: ${e.features[0].properties.COMID}, `;
    title += `Length: ${e.features[0].properties.lengthkm.toFixed(1)} km, `;
    title += `Area: ${e.features[0].properties.uparea.toFixed(0)} km<sup>2</sup>, `;
    title += `Order: ${e.features[0].properties.order}`;
    const comid = e.features[0].properties.COMID;

    fetchAll(e.features[0].properties.COMID).then(dataAll => {
        
      const [floatArray, pctls] = dataAll;

      const day1_nrt  = floatArray.dates.slice(-365)[0];
      const day1_pctl = new Date(day1_nrt.getFullYear(), 0, 1);

      const msPerDay = 1000 * 60 * 60 * 24;
      const diffInMs = day1_nrt - day1_pctl;
      const diffInDays = Math.round(diffInMs / msPerDay);
        
      var trace_retro = {
        x: floatArray.dates,
        y: floatArray.data,
        type: 'scatter',
        name: 'Flow',
        line: {
            color: 'darkblue',
            width: 1.5
        }
      };
      var trace_nrt = {
        x: floatArray.dates.slice(-365),
        y: floatArray.data.slice(-365),
        type: 'scatter',
        name: 'Flow',
        line: {
            color: 'darkblue',
            width: 1.5
        }
      };
      var traces_all = [];
      const ps = [5, 10, 20, 50, 80, 90, 95];
      const fillcolors = ['sienna', 'orange', 'yellow', 'lightgreen', 'lightcyan', 'lightblue', 'mediumpurple'];
      for (let p=6; p>=0; p--) {
          var trace_pctl = {
              x: floatArray.dates.slice(-365),
              y: rotateLeftTyped(pctls[p], diffInDays),
              type: 'scatter',
              fill: 'tozeroy',
              name: `${ps[p]}<sup>th</sup>`,
              line: {
                color: fillcolors[p],
                width: 1
              }
          };
          traces_all.push(trace_pctl);
      }
      traces_all.push(trace_nrt);
      var layout = {
        title: {text: title, font: {size: 15}},
        yaxis: {title: {text: 'Streamflow (m<sup>3</sup>/s)'}},
        margin: {l: 70, r: 30, b: 25, t: 50}
      };

      Plotly.newPlot('plotly-retro', [trace_retro], layout);
      Plotly.newPlot('plotly-nrt',   traces_all, layout);
        
      document.getElementById('download-grades-retro').addEventListener('click', function() {
        let csvContent = "data:text/csv;charset=utf-8,Date,Streamflow\n";
        for (let i = 0; i < floatArray.dates.length; i++) {
          csvContent += `${floatArray.dates[i].toISOString().slice(0, 10)},${floatArray.data[i].toFixed(2)}\n`;
        }
        const encodedUri = encodeURI(csvContent);
        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", `GRADES-hydroDL_retro_${comid}.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      });
      
      document.getElementById('download-grades-nrt').addEventListener('click', function() {
        let csvContent = "data:text/csv;charset=utf-8,Date,Streamflow\n";
        for (let i = floatArray.dates.length-365; i < floatArray.dates.length; i++) {
          csvContent += `${floatArray.dates[i].toISOString().slice(0, 10)},${floatArray.data[i].toFixed(2)}\n`;
        }
        const encodedUri = encodeURI(csvContent);
        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", `GRADES-hydroDL_nrt_${comid}.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      });

    }).catch(err => {
        console.error("Error fetching float data:", err);
        title += `, data retrieval error`;
    });

  });

}
