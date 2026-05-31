/**
 * @fileoverview Market insight card component rendered below search_job bot messages.
 * Displays Gold-layer analytics: top companies, work-mode distribution, and related skills.
 * @module MarketInsightCard
 */

import React, { useState } from "react";
import { FiBarChart2, FiChevronDown, FiChevronUp } from "react-icons/fi";

/**
 * @typedef {Object} WorkModeDist
 * @property {number} [remote] - Number of remote job postings.
 * @property {number} [hybrid] - Number of hybrid job postings.
 * @property {number} [office] - Number of office/on-site job postings.
 * @property {number} [onsite] - Alias for office postings.
 */

/**
 * @typedef {Object} MarketInsightData
 * @property {string}       primary_skill    - The main skill the insight is centred on.
 * @property {number}       total_jobs       - Total number of matching job postings.
 * @property {string[]}     top_companies    - Top hiring companies with counts, e.g. `["VNG (24)"]`.
 * @property {WorkModeDist} work_mode_dist   - Distribution of work modes across postings.
 * @property {string[]}     related_skills   - Co-occurring skills found in the postings.
 * @property {string|null}  location_filter  - Active location filter, or null for nationwide.
 */

/**
 * Colour palette keyed by work-mode name (lower-case).
 * @type {Object.<string, {bar: string, text: string, dot: string}>}
 */
const MODE_COLORS = {
  remote:  { bar: "bg-emerald-500", text: "text-emerald-700 dark:text-emerald-400", dot: "bg-emerald-500" },
  hybrid:  { bar: "bg-blue-500",    text: "text-blue-700   dark:text-blue-400",    dot: "bg-blue-500"    },
  office:  { bar: "bg-violet-500",  text: "text-violet-700 dark:text-violet-400",  dot: "bg-violet-500"  },
  onsite:  { bar: "bg-violet-500",  text: "text-violet-700 dark:text-violet-400",  dot: "bg-violet-500"  },
};

/**
 * Fallback colour used for work modes not listed in {@link MODE_COLORS}.
 * @type {{bar: string, text: string, dot: string}}
 */
const DEFAULT_MODE_COLOR = {
  bar: "bg-gray-400", text: "text-gray-600 dark:text-gray-400", dot: "bg-gray-400",
};

/**
 * Returns the Tailwind colour tokens for a given work-mode label.
 * @param {string} mode - Work-mode label (case-insensitive).
 * @returns {{bar: string, text: string, dot: string}}
 */
function modeColor(mode) {
  return MODE_COLORS[mode?.toLowerCase()] || DEFAULT_MODE_COLOR;
}

/**
 * @component
 * @brief Horizontal bar chart showing the distribution of work modes.
 *
 * Renders one labelled progress bar per mode, sorted by count descending.
 * Returns null when the distribution is empty.
 *
 * @param {Object}       props
 * @param {WorkModeDist} props.dist - Work-mode count map.
 * @returns {JSX.Element|null}
 */
function WorkModeBar({ dist }) {
  const entries = Object.entries(dist).filter(([, v]) => v > 0);
  if (entries.length === 0) return null;

  const total = entries.reduce((s, [, v]) => s + v, 0);

  return (
    <div className="space-y-1.5">
      {entries
        .sort(([, a], [, b]) => b - a)
        .map(([mode, count]) => {
          const pct = Math.round((count / total) * 100);
          const c   = modeColor(mode);
          return (
            <div key={mode} className="flex items-center gap-2">
              <span className={`text-xs w-14 flex-shrink-0 capitalize ${c.text}`}>{mode}</span>
              <div className="flex-1 bg-gray-100 dark:bg-gray-700 rounded-full h-1.5">
                <div
                  className={`${c.bar} h-1.5 rounded-full transition-all duration-700`}
                  style={{ width: `${pct}%` }}
                />
              </div>
              <span className="text-xs text-gray-500 dark:text-gray-400 w-8 text-right flex-shrink-0">
                {pct}%
              </span>
            </div>
          );
        })}
    </div>
  );
}

/**
 * @component
 * @brief Collapsible card displaying Gold-layer market intelligence for a skill/location pair.
 *
 * Bridges the Data Lake (fact_job_posting ⋈ dim_skill ⋈ dim_company ⋈ dim_work_mode
 * via Trino) and the RAG pipeline. Renders below `search_job` bot messages.
 * Returns null when no insight data is provided.
 *
 * @param {Object}            props
 * @param {MarketInsightData} props.insight - Market insight payload from the API.
 * @returns {JSX.Element|null}
 */
export default function MarketInsightCard({ insight }) {
  // {boolean} Whether the card body is expanded
  const [open, setOpen] = useState(true);

  if (!insight) return null;

  const locLabel = insight.location_filter ? ` · ${insight.location_filter}` : "";
  const title    = `${insight.primary_skill}${locLabel}`;

  return (
    <div className="mt-3 rounded-xl border border-indigo-200 dark:border-indigo-800 bg-gradient-to-br from-indigo-50 to-blue-50 dark:from-indigo-950/40 dark:to-blue-950/30 overflow-hidden text-sm">

      {/* Header — always visible, click to collapse */}
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between px-4 py-2.5 hover:bg-indigo-100/50 dark:hover:bg-indigo-900/20 transition"
      >
        <div className="flex items-center gap-2">
          <FiBarChart2 className="w-4 h-4 text-indigo-600 dark:text-indigo-400 flex-shrink-0" />
          <span className="font-medium text-indigo-800 dark:text-indigo-200 text-xs uppercase tracking-wide">
            Thị trường {title}
          </span>
          <span className="ml-1 px-2 py-0.5 rounded-full bg-indigo-600 text-white text-xs font-semibold">
            {insight.total_jobs} jobs
          </span>
        </div>
        {open
          ? <FiChevronUp   className="w-3.5 h-3.5 text-indigo-400" />
          : <FiChevronDown className="w-3.5 h-3.5 text-indigo-400" />
        }
      </button>

      {/* Body */}
      {open && (
        <div className="px-4 pb-4 pt-1 grid grid-cols-1 gap-4 sm:grid-cols-3">

          {/* Top companies */}
          {insight.top_companies.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-2">
                Top công ty
              </p>
              <ul className="space-y-1">
                {insight.top_companies.slice(0, 4).map((c, i) => (
                  <li key={c} className="flex items-center gap-2">
                    <span className="w-4 h-4 rounded-full bg-indigo-100 dark:bg-indigo-900/50 flex items-center justify-center text-indigo-600 dark:text-indigo-300 text-[10px] font-bold flex-shrink-0">
                      {i + 1}
                    </span>
                    <span className="text-gray-700 dark:text-gray-300 text-xs break-words leading-snug">{c}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Work mode distribution */}
          {Object.keys(insight.work_mode_dist).length > 0 && (
            <div>
              <p className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-2">
                Work mode
              </p>
              <WorkModeBar dist={insight.work_mode_dist} />
            </div>
          )}

          {/* Related skills */}
          {insight.related_skills.length > 0 && (
            <div>
              <p className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-2">
                Kỹ năng đi kèm
              </p>
              <div className="flex flex-wrap gap-1.5">
                {insight.related_skills.slice(0, 6).map((s) => (
                  <span
                    key={s}
                    className="text-xs bg-white dark:bg-gray-800 border border-indigo-200 dark:border-indigo-700 text-indigo-700 dark:text-indigo-300 px-2 py-0.5 rounded-full"
                  >
                    {s}
                  </span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* Footer label */}
      <div className="px-4 pb-2 flex items-center gap-1.5">
        <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse flex-shrink-0" />
        <span className="text-[10px] text-gray-400 dark:text-gray-500">
          Dữ liệu thực từ Gold layer · Iceberg + Trino
        </span>
      </div>
    </div>
  );
}
