/**
 * @file fetch basin time series data and draw plotly figures on map click
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { generateDateArray } from './utils.js';

async function plotBasinTs(nwsId, freq, title) {
  try {
    const urlbase = 'https://cw3e.ucsd.edu/hydro/cnrfc/csv/basins';
    const response = await fetch(`${urlbase}/${freq}/${nwsId}_${freq}.csv.gz`);
    const compressedData = await response.arrayBuffer();

    // Decompress GZIP using pako
    const decompressedData = pako.ungzip(new Uint8Array(compressedData), { to: 'string' });

    // Parse CSV using PapaParse
    const parsed = Papa.parse(decompressedData, {
      header: true,
      skipEmptyLines: true
    });

    const data = parsed.data;

    // 
    const dates = data.map(row => row['Date']);  // Replace 'Date' with your column name
    var traces = [];

    if (freq=='daily'||freq=='monthly') {
      // precipitation
      var trace_prec = {
        x: dates,
        y: data.map(row => parseFloat(row['PREC'])),
        type: 'bar',
        name: `Precipitation (mm)`,
        yaxis: 'y',
        marker: {
          color: 'darkgray',
          width: 1
        }
      };
      traces.push(trace_prec);
      // temperature
      var trace_temp = {
        x: dates,
        y: data.map(row => parseFloat(row['T2D'])),
        mode: 'markers',
        name: `Temperature (&deg;C)`,
        yaxis: 'y2',
        marker: {
          color: 'orange',
          symbol: '.'
        }
      };
      traces.push(trace_temp);
      // SWE
      var trace_swe = {
        x: dates,
        y: data.map(row => parseFloat(row['SWE'])),
        mode: 'lines',
        name: `Snow Water Equiv (mm)`,
        yaxis: 'y3',
        line: {
          color: 'magenta',
          width: 1
        }
      };
      traces.push(trace_swe);
      // total soil moisture
      var trace_sm = {
        x: dates,
        y: data.map(row => parseFloat(row['SMTOT'])*2000),
        mode: 'lines',
        name: `Total Soil Moisture (mm)`,
        yaxis: 'y4',
        line: {
          color: 'green',
          width: 1
        }
      };
      traces.push(trace_sm);
    }
      
    // runoff
    var trace_runoff = {
      x: dates,
      y: data.map(row => parseFloat(row['RUNOFF'])),
      mode: 'lines',
      name: `Local Runoff (m<sup>3</sup>/s)`,
      yaxis: 'y5',
      line: {
        color: 'blue',
        width: 1
      }
    };
    traces.push(trace_runoff);

    var layout = {
        title: {text: title, font: {size: 15}},
        yaxis: {title: {text: `Precipitation (mm)`, font: {color: 'darkgray'}, standoff: 0}, tickfont: {color:'darkgray'}, zeroline: false, range: [0, null], automargin: true},
        yaxis2: {title: {text: `Temperature (&deg;C)`, font: {color: 'orange'}, standoff: 0}, tickfont: {color:'orange'}, zeroline: false, overlaying: 'y', side: 'left', position: 0.04, automargin: true},
        yaxis3: {title: {text: `Snow Water Equiv (mm)`, font: {color: 'magenta'}, standoff: 0}, tickfont: {color:'magenta'}, zeroline: false, range: [0, null], overlaying: 'y', side: 'right', automargin: true},
        yaxis4: {title: {text: `Total Soil Moisture (mm)`, font: {color: 'green'}, standoff: 0}, tickfont: {color:'green'}, zeroline: false, overlaying: 'y', side: 'right', position: 0.96, automargin: true},
        yaxis5: {title: {text: `Local Runoff (m<sup>3</sup>/s)`, font: {color: 'blue'}, standoff: 0}, tickfont: {color:'blue'}, zeroline: false, range: [0, null], overlaying: 'y', side: 'left', position: 0.08, automargin: true},
        legend: { x: 0.96, y: 1, xanchor: 'right', yanchor: 'top'},
        margin: {l: 50, r: 50, b: 35, t: 50}
    };

    Plotly.newPlot('plotly-basints-'+freq,  traces,  layout);
      
    document.getElementById('download-basints-'+freq).addEventListener('click', function() {
      const blob = new Blob([decompressedData], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.setAttribute("href", url);
      link.setAttribute("download", `${nwsId}_${freq}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    });

  } catch (error) {
    console.error('Error fetching or plotting data:', error);
  }
}


export async function setupBasinTs(map, layer) {

  map.on('click', layer, (e) => {

    const popupContent = `
  <ul class="nav nav-tabs" id="popupTab-basints" role="tablist">
    <li class="nav-item" role="presentation">
      <button class="nav-link active" id="tab1-basints-tab" data-bs-toggle="tab" data-bs-target="#tab1-basints"
        type="button" role="tab">Hourly</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="tab2-basints-tab" data-bs-toggle="tab" data-bs-target="#tab2-basints"
        type="button" role="tab">Daily</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="tab3-basints-tab" data-bs-toggle="tab" data-bs-target="#tab3-basints"
        type="button" role="tab">Monthly</button>
    </li>
  </ul>
  <div class="tab-content mt-2">
    <div class="tab-pane fade show active" id="tab1-basints" role="tabpanel">
      <button id="download-basints-hourly">Download CSV data</button><div id="plotly-basints-hourly"></div>
    </div>
    <div class="tab-pane fade" id="tab2-basints" role="tabpanel">
      <button id="download-basints-daily">Download CSV data</button><div id="plotly-basints-daily"></div>
    </div>
    <div class="tab-pane fade" id="tab3-basints" role="tabpanel">
      <button id="download-basints-monthly">Download CSV data</button><div id="plotly-basints-monthly"></div>
    </div>
  </div>
`;

    new maplibregl.Popup({maxWidth: '1110px'})
        .setLngLat(e.lngLat)
        .setHTML(popupContent)
        .addTo(map);

    var title = `WRF-Hydro (NWM v3.0), ${e.features[0].properties.ID}, `;
    title += `River: ${e.features[0].properties.River}, `;
    title += `Location: ${e.features[0].properties.Location.split(" - ")[1]}`;
    const nwsId = e.features[0].properties.ID;

    const freqs = ['hourly', 'daily', 'monthly'];
    freqs.forEach(freq => {
      plotBasinTs(nwsId, freq, title);
    });

  });

}
