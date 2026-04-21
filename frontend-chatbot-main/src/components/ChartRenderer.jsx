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

const DEFAULT_OPTIONS = {
  responsive: true,
  maintainAspectRatio: true,
  plugins: {
    legend: { position: "top" },
    tooltip: { mode: "index", intersect: false },
  },
};

const PIE_OPTIONS = {
  responsive: true,
  maintainAspectRatio: true,
  plugins: {
    legend: { position: "right" },
    tooltip: { mode: "nearest", intersect: true },
  },
};

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
