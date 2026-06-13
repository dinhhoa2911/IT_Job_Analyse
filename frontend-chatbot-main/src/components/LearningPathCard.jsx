/**
 * @fileoverview Learning path card component rendered below learning_path bot messages.
 * Displays a data-driven skill roadmap from the Gold lakehouse (bridge_score + market_freq).
 * @module LearningPathCard
 */

import React, { useState } from "react";
import { FiChevronDown, FiChevronUp, FiMap } from "react-icons/fi";

const SKILL_DISPLAY = {
  AI:"AI", ML:"ML", SQL:"SQL", AWS:"AWS", GCP:"GCP", API:"API",
  HTML:"HTML", CSS:"CSS", PHP:"PHP", "QA":"QA", "QC":"QC", "QA QC":"QA/QC",
  OOP:"OOP", UI:"UI", UX:"UX", SDK:"SDK", ETL:"ETL", NLP:"NLP",
  LLM:"LLM", RPA:"RPA", ERP:"ERP", CRM:"CRM", SAP:"SAP", BI:"BI", IT:"IT",
  ".NET":".NET", "NODE.JS":"Node.js", "VUE.JS":"Vue.js", "REACT.JS":"React.js",
  "NEXT.JS":"Next.js", "NUXT.JS":"Nuxt.js", "CICD":"CI/CD",
  "DEVOPS":"DevOps", "POSTGRESQL":"PostgreSQL", "MONGODB":"MongoDB",
  "MYSQL":"MySQL", "JAVASCRIPT":"JavaScript", "TYPESCRIPT":"TypeScript",
  "REACT NATIVE":"React Native", "SPRING BOOT":"Spring Boot",
  "GITHUB":"GitHub", "GITLAB":"GitLab", "ELASTICSEARCH":"Elasticsearch",
  "KUBERNETES":"Kubernetes", "TENSORFLOW":"TensorFlow", "PYTORCH":"PyTorch",
  "FASTAPI":"FastAPI", "CHATGPT":"ChatGPT", "OPENAI":"OpenAI",
  "PYTHON":"Python", "JAVA":"Java", "REACT":"React", "ANGULAR":"Angular",
  "VUE":"Vue", "DOCKER":"Docker", "LINUX":"Linux", "REDIS":"Redis",
  "KAFKA":"Kafka", "SPARK":"Spark", "HADOOP":"Hadoop", "AIRFLOW":"Airflow",
  "FLUTTER":"Flutter", "SWIFT":"Swift", "KOTLIN":"Kotlin", "SCALA":"Scala",
  "RUST":"Rust", "RUBY":"Ruby", "GO":"Go", "DJANGO":"Django", "FLASK":"Flask",
  "SPRING":"Spring", "LARAVEL":"Laravel", "ORACLE":"Oracle", "AZURE":"Azure",
  "CLOUD":"Cloud", "DATABASE":"Database", "MICROSERVICE":"Microservices",
  "ENGLISH":"English", "JAPANESE":"Japanese", "KOREAN":"Korean",
  "AGILE":"Agile", "SCRUM":"Scrum", "JIRA":"Jira", "BLOCKCHAIN":"Blockchain",
  "TAILWIND":"Tailwind", "AUTOMATION TEST":"Automation Test",
  "TEAM MANAGEMENT":"Team Management", "PROJECT MANAGEMENT":"Project Management",
  "BUSINESS ANALYSIS":"Business Analysis",
};

function formatSkillName(raw) {
  if (!raw) return raw;
  const key = raw.trim().toUpperCase();
  if (SKILL_DISPLAY[key]) return SKILL_DISPLAY[key];
  // Title-case fallback
  return raw.trim().replace(/\w\S*/g, w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase());
}

/** Tailwind tokens keyed by skill_group (lower-cased prefix match). */
const GROUP_COLORS = {
  "backend":         { badge: "bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300",   bar: "bg-blue-500"    },
  "frontend":        { badge: "bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300", bar: "bg-emerald-500" },
  "data":            { badge: "bg-violet-100 dark:bg-violet-900/40 text-violet-700 dark:text-violet-300",     bar: "bg-violet-500"  },
  "devops":          { badge: "bg-orange-100 dark:bg-orange-900/40 text-orange-700 dark:text-orange-300",     bar: "bg-orange-500"  },
  "testing":         { badge: "bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300",         bar: "bg-amber-500"   },
  "management":      { badge: "bg-indigo-100 dark:bg-indigo-900/40 text-indigo-700 dark:text-indigo-300",     bar: "bg-indigo-500"  },
  "product":         { badge: "bg-pink-100 dark:bg-pink-900/40 text-pink-700 dark:text-pink-300",             bar: "bg-pink-500"    },
  "software":        { badge: "bg-cyan-100 dark:bg-cyan-900/40 text-cyan-700 dark:text-cyan-300",             bar: "bg-cyan-500"    },
};
const DEFAULT_GROUP = { badge: "bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300", bar: "bg-gray-400" };

function groupColor(skillGroup) {
  if (!skillGroup) return DEFAULT_GROUP;
  const lower = skillGroup.toLowerCase();
  for (const [key, val] of Object.entries(GROUP_COLORS)) {
    if (lower.includes(key)) return val;
  }
  return DEFAULT_GROUP;
}

/** Rank badge color: gold/silver/bronze for top 3, teal for the rest. */
function rankBadgeClass(rank) {
  if (rank === 1) return "bg-yellow-400 text-yellow-900";
  if (rank === 2) return "bg-gray-300 text-gray-700";
  if (rank === 3) return "bg-amber-600 text-white";
  return "bg-teal-600 text-white";
}

/**
 * @component
 * @brief Collapsible card showing the top-10 skills to learn for a target role.
 *
 * Each skill row shows: rank, name, group badge, market frequency bar, and
 * bridge score (if > 0 — indicates connection to user's existing stack).
 *
 * @param {Object}             props
 * @param {Object}             props.learningPath - LearningPathResult from the API.
 * @returns {JSX.Element|null}
 */
export default function LearningPathCard({ learningPath }) {
  const [open, setOpen] = useState(true);

  if (!learningPath || !learningPath.steps?.length) return null;

  const { target_role, role_category, total_jobs, known_skills, steps } = learningPath;

  return (
    <div className="mt-3 rounded-xl border border-teal-200 dark:border-teal-800 bg-gradient-to-br from-teal-50 to-emerald-50 dark:from-teal-950/40 dark:to-emerald-950/30 overflow-hidden text-sm">

      {/* Header */}
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between px-4 py-2.5 hover:bg-teal-100/50 dark:hover:bg-teal-900/20 transition"
      >
        <div className="flex items-center gap-2 flex-wrap">
          <FiMap className="w-4 h-4 text-teal-600 dark:text-teal-400 flex-shrink-0" />
          <span className="font-medium text-teal-800 dark:text-teal-200 text-xs uppercase tracking-wide">
            Lộ trình kỹ năng · {role_category}
          </span>
          <span className="ml-1 px-2 py-0.5 rounded-full bg-teal-600 text-white text-xs font-semibold">
            {total_jobs.toLocaleString()} jobs
          </span>
        </div>
        {open
          ? <FiChevronUp   className="w-3.5 h-3.5 text-teal-400 flex-shrink-0" />
          : <FiChevronDown className="w-3.5 h-3.5 text-teal-400 flex-shrink-0" />
        }
      </button>

      {open && (
        <div className="px-4 pb-4 pt-1 space-y-3">

          {/* Known skills */}
          {known_skills?.length > 0 && (
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-xs text-gray-500 dark:text-gray-400 font-medium flex-shrink-0">
                Kỹ năng đã có:
              </span>
              {known_skills.map((s) => (
                <span
                  key={s}
                  className="text-xs bg-teal-600 text-white px-2 py-0.5 rounded-full font-medium"
                >
                  {s}
                </span>
              ))}
            </div>
          )}

          {/* Section label */}
          <p className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">
            Top {steps.length} kỹ năng nên học tiếp
          </p>

          {/* Skill rows */}
          <div className="space-y-2">
            {steps.map((step, idx) => {
              const rank  = idx + 1;
              const gc    = groupColor(step.skill_group);
              const pct   = Math.min(step.market_freq, 100);
              const hasBridge = step.bridge_score > 0;
              return (
                <div
                  key={step.skill_name}
                  className="bg-white/70 dark:bg-gray-800/50 rounded-lg px-3 py-2 flex items-start gap-3"
                >
                  {/* Rank badge */}
                  <span
                    className={`w-5 h-5 rounded-full flex items-center justify-center text-[10px] font-bold flex-shrink-0 mt-0.5 ${rankBadgeClass(rank)}`}
                  >
                    {rank}
                  </span>

                  {/* Skill info */}
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap mb-1">
                      <span className="text-sm font-semibold text-gray-800 dark:text-gray-100 leading-snug">
                        {formatSkillName(step.skill_name)}
                      </span>
                      {step.skill_group && step.skill_group !== "Other" && (
                        <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${gc.badge}`}>
                          {step.skill_group}
                        </span>
                      )}
                    </div>

                    {/* Market freq bar */}
                    <div className="flex items-center gap-2">
                      <div className="flex-1 bg-gray-100 dark:bg-gray-700 rounded-full h-1.5">
                        <div
                          className={`${gc.bar} h-1.5 rounded-full transition-all duration-700`}
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                      <span className="text-xs text-gray-500 dark:text-gray-400 w-10 text-right flex-shrink-0">
                        {step.market_freq}%
                      </span>
                    </div>

                    {/* Bridge score — only shown when > 0 */}
                    {hasBridge && (
                      <p className="text-[10px] text-teal-600 dark:text-teal-400 mt-0.5">
                        Bridge score {step.bridge_score}% — kết nối tốt với stack của bạn
                      </p>
                    )}
                  </div>
                </div>
              );
            })}
          </div>

          {/* Score legend */}
          <p className="text-[10px] text-gray-400 dark:text-gray-500 pt-1">
            Xếp hạng: 60% nhu cầu thị trường + 40% điểm kết nối với kỹ năng hiện có
          </p>
        </div>
      )}

      {/* Footer */}
      <div className="px-4 pb-2 flex items-center gap-1.5">
        <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse flex-shrink-0" />
        <span className="text-[10px] text-gray-400 dark:text-gray-500">
          Dữ liệu thực từ Gold layer · Iceberg + Trino
        </span>
      </div>
    </div>
  );
}
