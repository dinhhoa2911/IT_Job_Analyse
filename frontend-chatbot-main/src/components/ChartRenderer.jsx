import React from "react";
import {
  Chart as ChartJS,
  CategoryScale, LinearScale, BarElement,
  LineElement, PointElement, ArcElement,
  Title, Tooltip, Legend, Filler,
} from "chart.js";
import { Bar, Line, Pie, Doughnut } from "react-chartjs-2";

ChartJS.register(
  CategoryScale, LinearScale, BarElement, LineElement,
  PointElement, ArcElement, Title, Tooltip, Legend, Filler
);

const FONT = "'Inter', 'system-ui', sans-serif";

function buildOptions(type, data, extra = {}) {
  const isSingleSeries = (data?.datasets?.length ?? 0) <= 1;

  const tooltip = {
    backgroundColor: "rgba(17, 24, 39, 0.95)",
    titleColor: "#f9fafb",
    bodyColor: "#9ca3af",
    borderColor: "rgba(255,255,255,0.06)",
    borderWidth: 1,
    padding: { x: 14, y: 10 },
    cornerRadius: 8,
    titleFont: { size: 12, weight: "600", family: FONT },
    bodyFont:  { size: 12, family: FONT },
    displayColors: !isSingleSeries,
    mode: "index",
    intersect: false,
  };

  const legend = {
    display: true,
    position: "top",
    labels: {
      usePointStyle: true,
      pointStyle: "rect",
      boxWidth: 24,
      boxHeight: 10,
      padding: 16,
      color: "#6b7280",
      font: { size: 12, family: FONT },
      generateLabels: (chart) =>
        chart.data.datasets.map((ds, i) => {
          const bg = Array.isArray(ds.backgroundColor)
            ? ds.backgroundColor[0]
            : ds.backgroundColor;
          // Line charts: borderColor là màu đường (solid), backgroundColor là fill vùng (transparent)
          // Bar/Pie: borderColor thường không set → fallback về backgroundColor
          const color = ds.borderColor || bg || "#6b7280";
          return {
            text: ds.label || "",
            fillStyle: color,
            strokeStyle: color,
            lineWidth: 0,
            pointStyle: "rect",
            hidden: false,
            datasetIndex: i,
          };
        }),
    },
  };

  if (type === "pie" || type === "doughnut") {
    return {
      responsive: true,
      maintainAspectRatio: true,
      animation: { duration: 500 },
      plugins: {
        legend: { ...legend, display: true, position: "right" },
        tooltip: { ...tooltip, mode: "nearest", intersect: true },
      },
      ...extra,
    };
  }

  const scales = {
    x: {
      grid: { display: false },
      border: { display: false },
      ticks: {
        color: "#9ca3af",
        font: { size: 11, family: FONT },
        maxRotation: 45,
        autoSkip: true,
        maxTicksLimit: 16,
      },
    },
    y: {
      grid: { color: "rgba(156,163,175,0.1)", drawBorder: false },
      border: { display: false },
      ticks: { color: "#9ca3af", font: { size: 11, family: FONT } },
      beginAtZero: true,
    },
    ...(extra?.scales ?? {}),
  };

  return {
    responsive: true,
    maintainAspectRatio: true,
    animation: { duration: 500, easing: "easeInOutQuart" },
    plugins: { legend, tooltip },
    scales,
  };
}

export default function ChartRenderer({ chart }) {
  if (!chart?.data) return null;

  const { type, data, options: extra } = chart;
  const opts = buildOptions(type, data, extra);

  return (
    <div className="mt-4 rounded-2xl border border-gray-100 dark:border-gray-800 bg-white dark:bg-[#18181b] shadow-sm overflow-hidden">
      <div className="px-5 pt-4 pb-5">
        {type === "bar"      && <Bar      data={data} options={opts} />}
        {type === "line"     && <Line     data={data} options={opts} />}
        {type === "pie"      && <Pie      data={data} options={opts} />}
        {type === "doughnut" && <Doughnut data={data} options={opts} />}
      </div>
    </div>
  );
}
