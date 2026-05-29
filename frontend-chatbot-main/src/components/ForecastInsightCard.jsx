/**
 * @fileoverview Compact metadata badge rendered below Prophet forecast charts.
 * Displays model name, job category, MAPE accuracy, and data source attribution.
 * @module ForecastInsightCard
 */

import React from "react";
import { FiTrendingUp, FiCpu } from "react-icons/fi";

/**
 * @typedef {Object} ForecastInsightData
 * @property {string}       category      - Gold dim_job_category name.
 * @property {number|null}  mape          - Model MAPE accuracy %; null if unavailable.
 * @property {number}       periods_ahead - Number of future months forecasted.
 * @property {string}       model         - Model name (e.g. "Prophet").
 */

/**
 * Returns Tailwind colour tokens for a MAPE accuracy level.
 * @param {number|null} mape
 * @returns {{ bar: string, text: string, label: string }}
 */
function mapeConfig(mape) {
  if (mape === null || mape === undefined)
    return { bar: "bg-gray-300 dark:bg-gray-600", text: "text-gray-500", label: "N/A" };
  if (mape < 10)
    return { bar: "bg-emerald-500", text: "text-emerald-600 dark:text-emerald-400", label: "Cao" };
  if (mape < 20)
    return { bar: "bg-blue-500",    text: "text-blue-600 dark:text-blue-400",    label: "Tốt" };
  if (mape < 35)
    return { bar: "bg-amber-500",   text: "text-amber-600 dark:text-amber-400",   label: "Trung bình" };
  return   { bar: "bg-red-400",    text: "text-red-500 dark:text-red-400",      label: "Thấp" };
}

/**
 * @component
 * @brief Compact badge showing Prophet model metadata below a forecast chart.
 *
 * Bridges the Gold Iceberg layer (ml_forecast_jobs, ml_model_evaluation) and the
 * frontend — makes it explicit to the committee that the forecast came from a
 * pre-trained ML model stored in the data lakehouse.
 *
 * @param {Object}              props
 * @param {ForecastInsightData} props.insight - Forecast metadata from the API.
 * @returns {JSX.Element|null}
 */
export default function ForecastInsightCard({ insight }) {
  if (!insight) return null;

  const { bar, text, label } = mapeConfig(insight.mape);
  // MAPE bar width: lower MAPE = wider "accuracy" bar (inverted, capped at 50% for display)
  const accuracyPct = insight.mape != null ? Math.max(0, Math.round(100 - Math.min(insight.mape * 2, 100))) : 0;

  return (
    <div className="mt-3 rounded-xl border border-violet-200 dark:border-violet-800/60 bg-gradient-to-r from-violet-50 to-indigo-50 dark:from-violet-950/20 dark:to-indigo-950/20 overflow-hidden text-sm">
      <div className="px-4 py-2.5 flex flex-wrap items-center gap-x-5 gap-y-2">

        {/* Model badge */}
        <div className="flex items-center gap-1.5">
          <div className="w-6 h-6 rounded-lg bg-gradient-to-br from-violet-500 to-indigo-600 flex items-center justify-center flex-shrink-0">
            <FiCpu className="w-3 h-3 text-white" />
          </div>
          <span className="font-semibold text-violet-800 dark:text-violet-300 text-xs">
            {insight.model ?? "Prophet"}
          </span>
          <span className="text-[10px] text-gray-400 dark:text-gray-500">· Meta AI</span>
        </div>

        {/* Category */}
        <div className="flex items-center gap-1.5">
          <FiTrendingUp className="w-3.5 h-3.5 text-indigo-400 flex-shrink-0" />
          <span className="text-xs text-gray-600 dark:text-gray-300">
            <span className="font-medium">{insight.category}</span>
            {insight.periods_ahead > 0 ? (
              <span className="text-gray-400 dark:text-gray-500">
                {" "}· {insight.periods_ahead} tháng dự báo
              </span>
            ) : (
              <span className="text-amber-500 dark:text-amber-400">
                {" "}· Xu hướng lịch sử (cần chạy lại Prophet)
              </span>
            )}
          </span>
        </div>

        {/* MAPE accuracy */}
        <div className="flex items-center gap-2">
          <span className="text-[10px] text-gray-400 dark:text-gray-500 uppercase tracking-wide">
            Độ chính xác (MAPE)
          </span>
          {insight.mape != null ? (
            <>
              <div className="w-20 h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                <div className={`${bar} h-full rounded-full`} style={{ width: `${accuracyPct}%` }} />
              </div>
              <span className={`text-[11px] font-semibold ${text}`}>
                {insight.mape.toFixed(1)}% · {label}
              </span>
            </>
          ) : (
            <span className="text-[11px] text-gray-400">N/A</span>
          )}
        </div>

        {/* Attribution */}
        <div className="ml-auto flex items-center gap-1.5">
          <span className="w-1.5 h-1.5 rounded-full bg-violet-400 animate-pulse flex-shrink-0" />
          <span className="text-[10px] text-gray-400 dark:text-gray-500">
            Gold layer · ml_forecast_jobs · Iceberg
          </span>
        </div>
      </div>
    </div>
  );
}
