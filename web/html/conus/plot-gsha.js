/**
 * @file fetch GSHA data and draw plotly figures on map click
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

import { generateDateArray } from './utils.js';

async function fetchGsha(dindex, dtype='yearly') {
  // GSHA binary data
  const gsha_url = 'https://cw3e.ucsd.edu/hydro/gsha/bin';
  const nrecords = { 'yearly': 43, 'monthly': 12*43 };
  const nvars  = 9;
  var chunk = 4*nvars*nrecords[dtype];
  var start = '1979-01-01'
  if (dtype=='yearly') {
    var end = '2021-01-01'
  } else {
    var end = '2021-12-01'
  }
  var url = `${gsha_url}/gsha_${dtype}.bin`;
  var byteOffset = chunk*dindex;

  const dates = generateDateArray(start, end, dtype);
  const byteLength = chunk;
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
  const floatArray2D = convertTo2D(floatArray, nvars, nrecords[dtype]);
  //return {data: floatArray2D, dates: dates};
  return trimDataArray(floatArray2D, dates);
}

function convertTo2D(array, rows, cols) {
  if (rows * cols !== array.length) {
    throw new Error("Rows and columns dimensions do not match array length.");
  }
  const newArray = [];
  for (let i = 0; i < rows; i++) {
    newArray[i] = array.slice(i * cols, (i + 1) * cols);
  }
  return newArray;
}

function trimDataArray(dataArray, dates) {
  let firstValidRowIndex = -1;
  let lastValidRowIndex = -1;

  // Find the index of the first valid row (iterating forwards)
  for (let i = 0; i < dataArray[0].length; i++) {
    // Check if the current row (which is a Float32Array) has no NaN values
    if (!Number.isNaN(dataArray[0][i])) {
      firstValidRowIndex = i;
      break;
    }
  }

  // Find the index of the last valid row (iterating backwards)
  if (firstValidRowIndex !== -1) {
    for (let i = dataArray[0].length - 1; i >= firstValidRowIndex; i--) {
      // Check if the current row (Float32Array) has no NaN values
      if (!Number.isNaN(dataArray[0][i])) {
        lastValidRowIndex = i;
        break;
      }
    }
  }

  if (firstValidRowIndex !== -1 && lastValidRowIndex !== -1) {
    // Slice the array from the first valid row's index to the last valid row's index (inclusive)
    const newArray = [];
    for (let i = 0; i < dataArray.length; i++) {
      newArray[i] = dataArray[i].slice(firstValidRowIndex, lastValidRowIndex + 1);
    }
    return { data: newArray, dates: dates.slice(firstValidRowIndex, lastValidRowIndex + 1) };
  } else {
    // If no valid rows are found, return an empty array
    return { data: [], dates: [] };
  }
}

async function fetchBoth(dindex) {
  const [dataA, dataB] = await Promise.all([
    fetchGsha(dindex, 'yearly'),
    fetchGsha(dindex, 'monthly')
  ]);
  return [dataA, dataB];
}

export async function setupGsha(map) {

  map.on('click', 'gsha', (e) => {

    const popupContent = `
  <ul class="nav nav-tabs" id="popupTab-gsha" role="tablist">
    <li class="nav-item" role="presentation">
      <button class="nav-link active" id="tab1-gsha-tab" data-bs-toggle="tab" data-bs-target="#tab1-gsha"
        type="button" role="tab">Yearly</button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="tab2-gsha-tab" data-bs-toggle="tab" data-bs-target="#tab2-gsha"
        type="button" role="tab">Monthly</button>
    </li>
  </ul>
  <div class="tab-content mt-2">
    <div class="tab-pane fade show active" id="tab1-gsha" role="tabpanel">
      <button id="download-gsha-yearly">Download CSV data</button><div id="plotly-gsha-yearly"></div>
    </div>
    <div class="tab-pane fade" id="tab2-gsha" role="tabpanel">
      <button id="download-gsha-monthly">Download CSV data</button><div id="plotly-gsha-monthly"></div>
    </div>
  </div>
`;
    new maplibregl.Popup({maxWidth: '1110px'})
        .setLngLat(e.lngLat)
        .setHTML(popupContent)
        .addTo(map);

    var title = `GSHA (v1.1), ${e.features[0].properties.agency} ${e.features[0].properties.Sttn_Nm.split("_")[0]}, `;
    title += `Area: ${e.features[0].properties.WatershedArea.toFixed(0)} km<sup>2</sup>, `;
    title += `MERIT-Basins COMID: ${e.features[0].properties.COMID}`;
    const dindex = e.features[0].properties.dindex;

    fetchBoth(e.features[0].properties.dindex).then(dataBoth => {
        
      const [yearlyArray, monthlyArray] = dataBoth;
      
      var traces_yearly = [];
      const ps = [1, 10, 25, 50, 75, 90, 99];
      const fillcolors = ['sienna', 'orange', 'yellow', 'lightgreen', 'lightcyan', 'lightblue', 'mediumpurple'];
      for (let p=6; p>=0; p--) {
          var trace_pctl = {
              x: yearlyArray.dates,
              y: yearlyArray.data[p],
              mode: 'lines',
              type: 'scatter',
              fill: 'tozeroy',
              name: `${ps[p]}<sup>th</sup>`,
              line: {
                color: fillcolors[p],
                width: 1
              }
          };
          if (p==0) {
            trace_pctl.fillcolor = 'white';
          }
          traces_yearly.push(trace_pctl);
      }
      var trace_mean = {
          x: yearlyArray.dates,
          y: yearlyArray.data[7],
          mode: 'lines',
          type: 'scatter',
          name: `Mean`,
          line: {
            color: 'black',
            width: 3
          }
      };
      traces_yearly.push(trace_mean);
      var trace_max = {
          x: yearlyArray.dates,
          y: yearlyArray.data[8],
          mode: 'markers',
          type: 'scatter',
          name: `Maximum`,
          marker: {
            symbol: 'x-thin',
            size: 8,
            line: { color: 'darkred', width: 1}
          }
      };
      traces_yearly.push(trace_max);
      var traces_monthly = [];
      for (let p=6; p>=0; p--) {
          var trace_pctl = {
              x: monthlyArray.dates,
              y: monthlyArray.data[p],
              mode: 'lines',
              type: 'scatter',
              fill: 'tozeroy',
              name: `${ps[p]}<sup>th</sup>`,
              line: {
                color: fillcolors[p],
                width: 1
              }
          };
          if (p==0) {
            trace_pctl.fillcolor = 'white';
          }
          traces_monthly.push(trace_pctl);
      }
      var trace_mean = {
          x: monthlyArray.dates,
          y: monthlyArray.data[7],
          mode: 'lines',
          type: 'scatter',
          name: `Mean`,
          line: {
            color: 'black',
            width: 2
          }
      };
      traces_monthly.push(trace_mean);
      var trace_max = {
          x: monthlyArray.dates,
          y: monthlyArray.data[8],
          mode: 'markers',
          type: 'scatter',
          name: `Maximum`,
          marker: {
            symbol: 'x-thin',
            size: 4,
            line: { color: 'darkred', width: 1}
          }
      };
      traces_monthly.push(trace_max);
      var layout = {
        title: {text: title, font: {size: 15}},
        yaxis: {title: {text: 'Streamflow (m<sup>3</sup>/s)'}},
        margin: {l: 70, r: 30, b: 25, t: 50}
      };

      Plotly.newPlot('plotly-gsha-yearly',  traces_yearly,  layout);
      Plotly.newPlot('plotly-gsha-monthly', traces_monthly, layout);
        
      document.getElementById('download-gsha-yearly').addEventListener('click', function() {
        let csvContent = "data:text/csv;charset=utf-8,Date";
        for (let p=0; p<7; p++) {
          csvContent += `,${ps[p]}-percentile`;
        }
        csvContent += ',mean,maximum\n';
        for (let i = 0; i < yearlyArray.dates.length; i++) {
          csvContent += `${yearlyArray.dates[i].toISOString().slice(0, 10)}`;
          for (let p=0; p<9; p++) {
            csvContent += `,${yearlyArray.data[p][i].toFixed(3)}`;
          }
          csvContent += '\n';
        }
        const encodedUri = encodeURI(csvContent);
        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", `GHSA_yearly_${dindex}.csv`);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
      });
      
      document.getElementById('download-gsha-monthly').addEventListener('click', function() {
        let csvContent = "data:text/csv;charset=utf-8,Date";
        for (let p=0; p<7; p++) {
          csvContent += `,${ps[p]}-percentile`;
        }
        csvContent += ',mean,maximum\n';
        for (let i = 0; i < monthlyArray.dates.length; i++) {
          csvContent += `${monthlyArray.dates[i].toISOString().slice(0, 10)}`;
          for (let p=0; p<9; p++) {
            csvContent += `,${monthlyArray.data[p][i].toFixed(3)}`;
          }
          csvContent += '\n';
        }
        const encodedUri = encodeURI(csvContent);
        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", `GSHA_monthly_${dindex}.csv`);
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
