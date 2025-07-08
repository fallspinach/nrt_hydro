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

async function fetchGradesPctl(comid) {
  // GRADES-hydrDL binary data
  const grades_url = 'https://cw3e.ucsd.edu/hydro/grades_hydrodl/bin';
  var pfaf2 = Math.floor(comid/1000000);
  var url = `${grades_url}/GRADES-hydroDL_ydrunpctl_${pfaf2}.bin`;
  var cnt = comid - pfaf2*1000000 - 1;
  var byteOffset = cnt*366*7*4;
  const byteLength = 366*7*4;

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
  const floatArray2D = convertTo2D(floatArray, 7, 366);
  return floatArray2D;
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

async function fetchAll(comid) {
  const [dataA, dataB] = await Promise.all([
    fetchGrades('MERIT', comid, 0),
    fetchGradesPctl(comid)
  ]);
  return [dataA, dataB];
}

function rotateLeftTyped(arr, n) {

  const arr1 = new arr.constructor(arr.length - 1);

  // Copy everything before the index
  arr1.set(arr.subarray(0, 59));
  // Copy everything after the index
  arr1.set(arr.subarray(59 + 1), 59);

  const count = n % arr1.length;
  const result = new arr1.constructor(arr1.length);  // same typed array type
  result.set(arr1.subarray(count));                 // copy tail to front
  result.set(arr1.subarray(0, count), arr1.length - count);  // copy head to end
  return result;
}

export async function setupNwmRivers(map) {

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

}
