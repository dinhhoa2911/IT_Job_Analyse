/**
 * @fileoverview Chart rendering component that wraps Chart.js via react-chartjs-2.
 * Supports bar, line, and pie chart types.
 * @module ChartRenderer
 */

import React from "react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from "chart.js";
import { Bar, Line, Pie } from "react-chartjs-2";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

/**
 * Default Chart.js options applied to bar and line charts.
 * @type {Object}
 */
const DEFAULT_OPTIONS = {
  responsive: true,
  maintainAspectRatio: true,
  plugins: {
    legend: { position: "top" },
    tooltip: { mode: "index", intersect: false },
  },
};

/**
 * Default Chart.js options applied specifically to pie charts.
 * Positions the legend on the right and uses nearest-point tooltip mode.
 * @type {Object}
 */
const PIE_OPTIONS = {
  responsive: true,
  maintainAspectRatio: true,
  plugins: {
    legend: { position: "right" },
    tooltip: { mode: "nearest", intersect: true },
  },
};

/**
 * @typedef {Object} ChartSpec
 * @property {'bar'|'line'|'pie'} type    - Chart type identifier.
 * @property {Object}             data    - Chart.js `data` object (labels + datasets).
 * @property {Object}             [options] - Optional Chart.js options to merge with defaults.
 */

/**
 * @component
 * @brief Renders a Chart.js chart from a spec object returned by the RAG API.
 *
 * Merges API-supplied options with sensible defaults.
 * Returns null when no valid chart spec is provided.
 *
 * @param {Object}    props
 * @param {ChartSpec} props.chart - The chart specification object.
 * @returns {JSX.Element|null}
 */
function ChartRenderer({ chart }) {
  if (!chart || !chart.data) return null;

  const { type, data, options: customOptions } = chart;

  const mergedOptions = type === "pie"
    ? { ...PIE_OPTIONS, ...customOptions }
    : { ...DEFAULT_OPTIONS, ...customOptions };

  return (
    <div className="mt-4 p-4 bg-white border border-gray-100 rounded-xl shadow-sm max-w-xl">
      {type === "bar" && <Bar data={data} options={mergedOptions} />}
      {type === "line" && <Line data={data} options={mergedOptions} />}
      {type === "pie" && <Pie data={data} options={mergedOptions} />}
    </div>
  );
}

export default ChartRenderer;
