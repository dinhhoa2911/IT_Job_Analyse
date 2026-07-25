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
const TRUNCATE = 22;

const truncateLabel = (lbl) => {
  const s = String(lbl);
  return s.length > TRUNCATE ? s.slice(0, TRUNCATE) + "…" : s;
};

// ── Shared scale presets ──────────────────────────────────────────────────────
const scaleBase = {
  grid:   { color: "rgba(156,163,175,0.1)" },
  border: { display: false },
  ticks:  { color: "#9ca3af", font: { size: 11, family: FONT } },
};

const scaleHorizX = { ...scaleBase, beginAtZero: true };
const scaleHorizY = { grid: { display: false }, border: { display: false },
                      ticks: scaleBase.ticks };
const scaleVertX  = { grid: { display: false }, border: { display: false },
                      ticks: scaleBase.ticks };
const scaleVertY  = { ...scaleBase, beginAtZero: true };

// ── Legend ────────────────────────────────────────────────────────────────────
function buildLegend(position = "top") {
  return {
    display: true,
    position,
    labels: {
      usePointStyle: true,
      pointStyle: "rectRounded",
      boxWidth: 10,
      boxHeight: 10,
      padding: 16,
      color: "#6b7280",
      font: { size: 12, family: FONT },
      generateLabels: (chart) =>
        chart.data.datasets
          .map((ds, i) => {
            const bg = Array.isArray(ds.backgroundColor)
              ? ds.backgroundColor[0]
              : ds.backgroundColor;
            return {
              text: ds.label || "",
              fillStyle: bg || "#6b7280",
              strokeStyle: "transparent",
              lineWidth: 0,
              pointStyle: "rectRounded",
              hidden: false,
              datasetIndex: i,
            };
          })
          .filter((item) => item.text && !item.text.startsWith("_")),
    },
  };
}

// ── Tooltip base ──────────────────────────────────────────────────────────────
const tooltipBase = {
  backgroundColor:  "rgba(15, 23, 42, 0.96)",
  titleColor:       "#f1f5f9",
  bodyColor:        "#cbd5e1",
  borderColor:      "rgba(255,255,255,0.08)",
  borderWidth:      1,
  padding:          { x: 14, y: 10 },
  cornerRadius:     10,
  caretSize:        6,
  caretPadding:     8,
  titleFont:        { size: 13, weight: "600", family: FONT },
  bodyFont:         { size: 12, family: FONT },
  usePointStyle:    true,
  boxWidth:         9,
  boxHeight:        9,
  itemSort:         (a, b) => b.raw - a.raw,
};

// ── Main buildOptions ─────────────────────────────────────────────────────────
function buildOptions(type, data, extra = {}) {
  const nDatasets    = data?.datasets?.length ?? 1;
  const isSingle     = nDatasets <= 1;
  const titlePlugin  = extra?.plugins?.title ?? { display: false };

  // Detect grouped chart (e.g. top-N-per-category) — has skillNames on datasets
  const hasSkillNames = (data?.datasets ?? []).some(
    (ds) => Array.isArray(ds.skillNames) && ds.skillNames.some(Boolean)
  );

  // ── Tooltip callbacks ────────────────────────────────────────────────────
  const tooltipCallbacks = {
    title: (items) => items.map((i) => String(data.labels[i.dataIndex])),
    ...(hasSkillNames
      ? {
          label: (ctx) => {
            const name = ctx.dataset.skillNames?.[ctx.dataIndex];
            return name ? `${name}  ·  ${ctx.formattedValue}` : null;
          },
          filter: (item) => Boolean(item.dataset.skillNames?.[item.dataIndex]),
        }
      : {}),
  };

  const tooltip = {
    ...tooltipBase,
    displayColors: !isSingle || hasSkillNames,
    // For grouped charts show all bars of the hovered category at once
    mode:      hasSkillNames ? "index" : "index",
    intersect: false,
    callbacks: tooltipCallbacks,
  };

  // ── Pie / Doughnut ────────────────────────────────────────────────────────
  if (type === "pie" || type === "doughnut") {
    return {
      responsive:          true,
      maintainAspectRatio: true,
      animation: { duration: 450, easing: "easeOutQuart" },
      hover:     { animationDuration: 120 },
      plugins: {
        legend:  buildLegend("right"),
        tooltip: { ...tooltip, mode: "nearest", intersect: true },
        title:   titlePlugin,
      },
    };
  }

  // ── Bar / Line ────────────────────────────────────────────────────────────
  const labelCount  = data?.labels?.length ?? 0;
  const maxLabelLen = Math.max(0, ...(data?.labels ?? []).map((l) => String(l).length));
  // Honor indexAxis from backend (grouped chart sets indexAxis: "y")
  const backendHoriz = extra?.indexAxis === "y";
  const useHorizontal =
    backendHoriz || (type === "bar" && (labelCount > 8 || maxLabelLen > 20));

  const scales = useHorizontal
    ? {
        x: {
          ...scaleHorizX,
          ticks: { ...scaleBase.ticks },
        },
        y: {
          ...scaleHorizY,
          ticks: {
            ...scaleBase.ticks,
            autoSkip:      false,
            maxTicksLimit: labelCount,
            callback:      (_, idx) => truncateLabel(data.labels[idx]),
          },
        },
      }
    : {
        x: {
          ...scaleVertX,
          ticks: {
            ...scaleBase.ticks,
            maxRotation:  maxLabelLen > 12 ? 45 : 0,
            autoSkip:     true,
            maxTicksLimit: 16,
            callback:     (_, idx) => truncateLabel(data.labels[idx]),
          },
        },
        y: {
          ...scaleVertY,
          ...(extra?.scales?.y ?? {}),
        },
      };

  return {
    responsive:          true,
    maintainAspectRatio: true,
    animation:  { duration: 450, easing: "easeOutQuart" },
    hover:      { animationDuration: 120 },
    plugins: {
      legend:  buildLegend("top"),
      tooltip,
      title:   titlePlugin,
    },
    ...(useHorizontal ? { indexAxis: "y" } : {}),
    scales,
  };
}

// ── ChartRenderer ─────────────────────────────────────────────────────────────
export default function ChartRenderer({ chart }) {
  if (!chart?.data) return null;

  const { type, data, options: extra } = chart;

  const nLabels   = data?.labels?.length ?? 0;
  const maxLen    = Math.max(0, ...(data?.labels ?? []).map((l) => String(l).length));
  const nDatasets = data?.datasets?.length ?? 1;
  const backendHoriz = extra?.indexAxis === "y";
  const isHoriz   = backendHoriz || (type === "bar" && (nLabels > 8 || maxLen > 20));

  const opts = buildOptions(type, data, extra);

  // For grouped horizontal bars: each group needs more vertical space
  const rowHeight  = nDatasets > 1 ? nDatasets * 24 + 8 : 36;
  const chartStyle = isHoriz
    ? { height: Math.max(240, nLabels * rowHeight) + "px" }
    : {};

  return (
    <div className="mt-4 rounded-2xl border border-gray-100 dark:border-gray-700 bg-white dark:bg-[#18181b] shadow-sm overflow-hidden">
      <div className="px-5 pt-5 pb-6" style={chartStyle}>
        {type === "bar"      && <Bar      data={data} options={{ ...opts, maintainAspectRatio: !isHoriz }} />}
        {type === "line"     && <Line     data={data} options={opts} />}
        {type === "pie"      && <Pie      data={data} options={opts} />}
        {type === "doughnut" && <Doughnut data={data} options={opts} />}
      </div>
    </div>
  );
}
