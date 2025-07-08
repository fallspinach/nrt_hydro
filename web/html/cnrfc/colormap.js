/**
 * @file draw colormap
 * @author Ming Pan <m3pan@ucsd.edu>
 * @copyright Ming Pan, University of California San Diego
 */

export function setupColormap(colorStops, legendTitle, legendTitleID="legend-title", colorStripID="color-strip", labelContainerID="step-labels") {

  document.getElementById(legendTitleID).innerHTML = legendTitle;
  const colorStrip = document.getElementById(colorStripID);
  const labelContainer = document.getElementById(labelContainerID);
  colorStrip.innerHTML = '';
  labelContainer.innerHTML = '';

  const totalHeight = 250; // match container height
  const blockHeight = totalHeight / colorStops.length;

   // Create color blocks
  for (const stop of colorStops) {
    const block = document.createElement("div");
    block.style.height = `${blockHeight}px`;
    block.style.background = stop.color;
    colorStrip.appendChild(block);
  }

  // Create boundary labels (skip the first one which is < 10)
  const nStops = colorStops[colorStops.length-1].threshold==null ? colorStops.length-1 : colorStops.length;
  for (let i = 0; i < nStops; i++) {
    const label = document.createElement("div");
    label.textContent = colorStops[i].threshold;
    label.style.height = `${blockHeight}px`;
    labelContainer.appendChild(label);
  }
}

