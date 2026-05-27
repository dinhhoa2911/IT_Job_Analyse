import { useCallback, useEffect, useRef, useState } from "react";
import {
  FiBriefcase, FiCheckCircle, FiExternalLink, FiFileText,
  FiUploadCloud, FiX, FiMapPin, FiAward, FiClock,
  FiAlertCircle, FiTrendingUp, FiBarChart2, FiAlertTriangle,
  FiRefreshCw, FiUpload, FiInfo,
} from "react-icons/fi";
import { useAuth } from "../contexts/AuthContext";
import { FiTrash2 } from "react-icons/fi";

const API_URL = process.env.REACT_APP_API_URL || "http://localhost:8100";

const PROCESSING_STEPS = [
  { id: 1, label: "Đang đọc nội dung PDF…",               duration: 1400 },
  { id: 2, label: "AI đang phân tích hồ sơ…",             duration: 2800 },
  { id: 3, label: "Đang tìm kiếm việc làm phù hợp…",      duration: 6000 },
  { id: 4, label: "Đang phân tích khoảng cách kỹ năng…",  duration: 9999 },
];

const getErrorMeta = (msg = "") => {
  const m = msg.toLowerCase();

  // Non-IT CV — user needs guidance, not a retry
  if (
    m.includes("kỹ năng it") || m.includes("công nghệ thông tin") ||
    m.includes("lĩnh vực it") || m.includes("kỹ năng kỹ thuật")
  ) {
    return {
      type:        "non-it",
      icon:        FiAlertTriangle,
      iconBg:      "bg-amber-50 dark:bg-amber-950/30",
      iconBorder:  "border-amber-100 dark:border-amber-900/40",
      iconColor:   "text-amber-500",
      badge:       "Hồ sơ không phù hợp",
      badgeBg:     "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300",
      title:       "CV không thuộc lĩnh vực IT",
      tips: [
        "Thêm phần Kỹ năng kỹ thuật (Technical Skills) vào CV",
        "Liệt kê cụ thể: ngôn ngữ lập trình, framework, công cụ",
        "Ví dụ: Python, Java, React, Docker, SQL, AWS…",
      ],
      btnLabel:    "Tải lên CV khác",
      btnStyle:    "from-amber-500 to-orange-500 shadow-amber-200 dark:shadow-amber-900/40",
      nextStage:   "list",
    };
  }

  // File / PDF problems
  if (
    m.includes("pdf") || m.includes("tệp") || m.includes("file") ||
    m.includes("mb") || m.includes("scanned") || m.includes("image") ||
    m.includes("empty") || m.includes("page")
  ) {
    return {
      type:        "file",
      icon:        FiFileText,
      iconBg:      "bg-orange-50 dark:bg-orange-950/30",
      iconBorder:  "border-orange-100 dark:border-orange-900/40",
      iconColor:   "text-orange-500",
      badge:       "Lỗi tệp tin",
      badgeBg:     "bg-orange-100 text-orange-700 dark:bg-orange-900/40 dark:text-orange-300",
      title:       "Không thể đọc tệp CV",
      tips: [
        "Đảm bảo file là PDF có thể chọn văn bản (không phải ảnh scan)",
        "Kích thước tối đa cho phép là 5 MB",
        "Thử xuất lại CV từ Word / Google Docs sang PDF",
      ],
      btnLabel:    "Chọn lại tệp",
      btnStyle:    "from-orange-500 to-amber-500 shadow-orange-200 dark:shadow-orange-900/40",
      nextStage:   "list",
    };
  }

  // Technical / server error
  return {
    type:        "technical",
    icon:        FiX,
    iconBg:      "bg-red-50 dark:bg-red-950/30",
    iconBorder:  "border-red-100 dark:border-red-900/40",
    iconColor:   "text-red-500",
    badge:       "Lỗi hệ thống",
    badgeBg:     "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300",
    title:       "Đã xảy ra lỗi",
    tips:        [],
    btnLabel:    "Thử lại",
    btnStyle:    "from-blue-600 to-indigo-600 shadow-blue-200 dark:shadow-blue-900/40",
    nextStage:   "list",
  };
};

const SCORE_CONFIG = (score) => {
  if (score >= 85) return { color: "#10b981", bg: "from-emerald-500 to-teal-400",   label: "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/50 dark:text-emerald-300" };
  if (score >= 70) return { color: "#3b82f6", bg: "from-blue-500 to-indigo-400",    label: "bg-blue-100 text-blue-700 dark:bg-blue-900/50 dark:text-blue-300" };
  if (score >= 50) return { color: "#f59e0b", bg: "from-amber-500 to-orange-400",   label: "bg-amber-100 text-amber-700 dark:bg-amber-900/50 dark:text-amber-300" };
  return             { color: "#9ca3af", bg: "from-gray-400 to-gray-300",           label: "bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400" };
};

const LEVEL_CONFIG = {
  intern:  { label: "Intern",   cls: "bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-300" },
  fresher: { label: "Fresher",  cls: "bg-teal-100 text-teal-700 dark:bg-teal-900/40 dark:text-teal-300" },
  junior:  { label: "Junior",   cls: "bg-sky-100 text-sky-700 dark:bg-sky-900/40 dark:text-sky-300" },
  mid:     { label: "Mid",      cls: "bg-indigo-100 text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300" },
  senior:  { label: "Senior",   cls: "bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300" },
  lead:    { label: "Lead",     cls: "bg-rose-100 text-rose-700 dark:bg-rose-900/40 dark:text-rose-300" },
};

// ── Score Ring ────────────────────────────────────────────────────────────────

function ScoreRing({ score }) {
  const r = 26;
  const circ = 2 * Math.PI * r;
  const offset = circ - (score / 100) * circ;
  const { color } = SCORE_CONFIG(score);

  return (
    <div className="relative w-16 h-16 flex-shrink-0">
      <svg width="64" height="64" className="-rotate-90">
        <circle cx="32" cy="32" r={r} fill="none" stroke="currentColor" strokeWidth="5"
          className="text-gray-100 dark:text-gray-800" />
        <circle cx="32" cy="32" r={r} fill="none" stroke={color} strokeWidth="5"
          strokeDasharray={circ} strokeDashoffset={offset} strokeLinecap="round"
          style={{ transition: "stroke-dashoffset 0.8s cubic-bezier(0.4,0,0.2,1)" }} />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-sm font-bold leading-none" style={{ color }}>
          {Math.round(score)}
        </span>
        <span className="text-[9px] text-gray-400 leading-none mt-0.5">%</span>
      </div>
    </div>
  );
}

// ── Job Card ──────────────────────────────────────────────────────────────────

function JobCard({ job, index }) {
  const fields = {};
  (job.text_content || "").split(". ").forEach((part) => {
    const [k, ...rest] = part.split(": ");
    if (rest.length) fields[k.trim()] = rest.join(": ").trim();
  });

  const company  = fields["Company"]   || "";
  const location = fields["Location"]  || "";
  const mode     = fields["Work Mode"] || "";
  const { label: labelCls } = SCORE_CONFIG(job.match_score);

  return (
    <div
      className="group flex items-start gap-4 p-4 rounded-2xl border border-gray-100 dark:border-gray-800 bg-white dark:bg-gray-900 hover:border-blue-200 dark:hover:border-blue-800 hover:shadow-md dark:hover:shadow-blue-950/30 transition-all duration-200"
      style={{ animationDelay: `${index * 60}ms` }}
    >
      <ScoreRing score={job.match_score} />

      <div className="flex-1 min-w-0">
        {/* Title + badge */}
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <p className="font-semibold text-[15px] text-gray-900 dark:text-gray-100 leading-snug line-clamp-1">
              {job.job_title}
            </p>
            {company && (
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-0.5 truncate">{company}</p>
            )}
          </div>
          <span className={`text-[11px] px-2.5 py-1 rounded-full font-semibold flex-shrink-0 ${labelCls}`}>
            {job.match_label}
          </span>
        </div>

        {/* Meta */}
        {(location || mode) && (
          <div className="flex items-center gap-3 mt-2 text-xs text-gray-400 dark:text-gray-500">
            {location && (
              <span className="flex items-center gap-1">
                <FiMapPin className="w-3 h-3" />{location}
              </span>
            )}
            {mode && (
              <span className="flex items-center gap-1">
                <FiClock className="w-3 h-3" />{mode}
              </span>
            )}
          </div>
        )}

        {/* Skills */}
        {job.matched_skills?.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-2.5">
            {job.matched_skills.map((skill) => (
              <span key={skill}
                className="inline-flex items-center gap-1 text-[11px] bg-emerald-50 dark:bg-emerald-950/50 text-emerald-700 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-800/60 px-2 py-0.5 rounded-full font-medium">
                <FiCheckCircle className="w-2.5 h-2.5" />{skill}
              </span>
            ))}
          </div>
        )}

        {/* Link */}
        <a href={job.job_link} target="_blank" rel="noopener noreferrer"
          className="inline-flex items-center gap-1.5 mt-3 text-xs font-medium text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 transition-colors group-hover:underline">
          Xem chi tiết <FiExternalLink className="w-3 h-3" />
        </a>
      </div>
    </div>
  );
}

// ── CV Profile Card ───────────────────────────────────────────────────────────

function CVProfileCard({ profile }) {
  const [expanded, setExpanded] = useState(false);
  const showSkills = expanded ? profile.skills : profile.skills.slice(0, 8);
  const extra = profile.skills.length - 8;
  const lvl = LEVEL_CONFIG[profile.level] || LEVEL_CONFIG.junior;
  const initials = (profile.name || "CV")
    .split(" ").slice(-2).map((w) => w[0]?.toUpperCase()).join("");

  return (
    <div className="rounded-2xl border border-blue-100 dark:border-blue-900/50 bg-gradient-to-br from-blue-50 via-indigo-50/50 to-white dark:from-blue-950/30 dark:via-indigo-950/20 dark:to-gray-900 mb-4 overflow-hidden">
      {/* Top bar */}
      <div className="h-1.5 bg-gradient-to-r from-blue-500 via-indigo-500 to-violet-500" />

      <div className="p-5">
        {/* Header */}
        <div className="flex items-center gap-4 mb-4">
          <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center text-white font-bold text-lg flex-shrink-0 shadow-lg shadow-blue-200 dark:shadow-blue-900/50">
            {initials}
          </div>
          <div className="min-w-0">
            <p className="font-bold text-base text-gray-900 dark:text-gray-100 truncate">
              {profile.name || "Candidate"}
            </p>
            <div className="flex items-center gap-2 mt-1 flex-wrap">
              {profile.level && (
                <span className={`text-xs px-2.5 py-0.5 rounded-full font-semibold ${lvl.cls}`}>
                  {lvl.label}
                </span>
              )}
              {profile.experience_years != null && (
                <span className="text-xs text-gray-500 dark:text-gray-400 flex items-center gap-1">
                  <FiAward className="w-3 h-3" />
                  {profile.experience_years} năm kinh nghiệm
                </span>
              )}
            </div>
          </div>
        </div>

        {/* Preferred roles */}
        {profile.preferred_roles?.length > 0 && (
          <div className="mb-3">
            <p className="text-[11px] font-semibold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-1.5">
              Vị trí mục tiêu
            </p>
            <div className="flex flex-wrap gap-1.5">
              {profile.preferred_roles.map((r) => (
                <span key={r}
                  className="text-xs bg-white dark:bg-gray-800 border border-blue-200 dark:border-blue-800 text-blue-700 dark:text-blue-300 px-2.5 py-0.5 rounded-full font-medium shadow-sm">
                  {r}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Skills */}
        {profile.skills?.length > 0 && (
          <div className="mb-3">
            <p className="text-[11px] font-semibold text-gray-400 dark:text-gray-500 uppercase tracking-wider mb-1.5">
              Kỹ năng phát hiện
            </p>
            <div className="flex flex-wrap gap-1.5">
              {showSkills.map((s) => (
                <span key={s}
                  className="text-xs bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 text-gray-700 dark:text-gray-300 px-2.5 py-0.5 rounded-full font-medium shadow-sm">
                  {s}
                </span>
              ))}
              {!expanded && extra > 0 && (
                <button onClick={() => setExpanded(true)}
                  className="text-xs text-blue-600 dark:text-blue-400 hover:underline font-medium px-1">
                  +{extra} kỹ năng khác
                </button>
              )}
            </div>
          </div>
        )}

        {/* Footer meta */}
        <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-gray-500 dark:text-gray-400 pt-2 border-t border-blue-100 dark:border-blue-900/40">
          {profile.preferred_locations?.length > 0 && (
            <span className="flex items-center gap-1">
              <FiMapPin className="w-3 h-3 text-blue-400" />
              {profile.preferred_locations.join(", ")}
            </span>
          )}
          {profile.work_mode && profile.work_mode !== "null" && (
            <span className="flex items-center gap-1">
              <FiClock className="w-3 h-3 text-blue-400" />
              {profile.work_mode}
            </span>
          )}
          {profile.education && (
            <span className="flex items-center gap-1 truncate">
              <FiAward className="w-3 h-3 flex-shrink-0 text-blue-400" />
              {profile.education}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

// ── Skill Gap Card ────────────────────────────────────────────────────────────

const SKILL_GROUP_COLOR = {
  "Backend":       "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300",
  "Frontend":      "bg-violet-100 text-violet-700 dark:bg-violet-900/40 dark:text-violet-300",
  "Data & Cloud":  "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300",
  "Other":         "bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-300",
};

function CoverageGauge({ score }) {
  const r = 36;
  const circ = 2 * Math.PI * r;
  const offset = circ - (score / 100) * circ;
  const color = score >= 70 ? "#10b981" : score >= 40 ? "#f59e0b" : "#ef4444";
  const label = score >= 70 ? "Tốt" : score >= 40 ? "Trung bình" : "Cần bổ sung";

  return (
    <div className="flex flex-col items-center gap-1">
      <div className="relative w-24 h-24">
        <svg width="96" height="96" className="-rotate-90">
          <circle cx="48" cy="48" r={r} fill="none" stroke="currentColor" strokeWidth="7"
            className="text-gray-100 dark:text-gray-800" />
          <circle cx="48" cy="48" r={r} fill="none" stroke={color} strokeWidth="7"
            strokeDasharray={circ} strokeDashoffset={offset} strokeLinecap="round"
            style={{ transition: "stroke-dashoffset 1s cubic-bezier(0.4,0,0.2,1)" }} />
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span className="text-xl font-bold leading-none" style={{ color }}>{Math.round(score)}</span>
          <span className="text-[10px] text-gray-400 leading-none mt-0.5">%</span>
        </div>
      </div>
      <span className="text-xs font-semibold" style={{ color }}>{label}</span>
    </div>
  );
}

function SkillGapCard({ gap }) {
  const [expanded,        setExpanded]        = useState(false);
  const [expandPresent,   setExpandPresent]   = useState(false);
  if (!gap) return null;

  const maxFreq = gap.all_market_skills[0]?.market_frequency || 1;
  const PRESENT_SHOW = 6;

  return (
    <div className="rounded-2xl border border-amber-200 dark:border-amber-800/60 bg-gradient-to-br from-amber-50 via-orange-50/40 to-white dark:from-amber-950/20 dark:via-orange-950/10 dark:to-gray-900 overflow-hidden mb-4">
      {/* Top accent */}
      <div className="h-1 bg-gradient-to-r from-amber-400 via-orange-400 to-rose-400" />

      {/* Header */}
      <div className="px-5 pt-4 pb-3 flex items-start justify-between gap-3">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-amber-400 to-orange-500 flex items-center justify-center shadow-md shadow-amber-200 dark:shadow-amber-900/40 flex-shrink-0">
            <FiBarChart2 className="w-4.5 h-4.5 text-white" />
          </div>
          <div>
            <p className="font-bold text-[14px] text-gray-900 dark:text-gray-100 leading-tight">
              Phân tích khoảng cách kỹ năng
            </p>
            <p className="text-xs text-gray-400 dark:text-gray-500 mt-0.5">
              {gap.role_category} · {gap.total_jobs_analyzed.toLocaleString()} job postings từ Gold layer
            </p>
          </div>
        </div>
        <span className="text-[10px] text-emerald-600 dark:text-emerald-400 font-semibold bg-emerald-50 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-800 px-2 py-0.5 rounded-full flex items-center gap-1 flex-shrink-0">
          <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
          Iceberg + Trino
        </span>
      </div>

      {/* Coverage + summary */}
      <div className="px-5 pb-4 flex items-center gap-5 border-b border-amber-100 dark:border-amber-900/30">
        <CoverageGauge score={gap.cv_coverage_score} />
        <div className="flex-1 space-y-2">
          <p className="text-xs text-gray-500 dark:text-gray-400 leading-relaxed">
            CV của bạn bao phủ{" "}
            <span className="font-semibold text-gray-800 dark:text-gray-200">
              {gap.present_skills.length}/{gap.all_market_skills.length}
            </span>{" "}
            kỹ năng top thị trường cho vị trí{" "}
            <span className="font-semibold text-amber-700 dark:text-amber-400">{gap.role_category}</span>.
          </p>
          {gap.present_skills.length > 0 && (
            <div className="flex flex-wrap gap-1">
              {(expandPresent ? gap.present_skills : gap.present_skills.slice(0, PRESENT_SHOW)).map((s) => (
                <span key={s}
                  className="inline-flex items-center gap-1 text-[11px] bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-800/60 px-2 py-0.5 rounded-full font-medium">
                  <FiCheckCircle className="w-2.5 h-2.5" />{s}
                </span>
              ))}
              {gap.present_skills.length > PRESENT_SHOW && (
                <button
                  onClick={() => setExpandPresent((v) => !v)}
                  className="text-[11px] text-emerald-600 dark:text-emerald-400 hover:underline font-medium px-1"
                >
                  {expandPresent
                    ? "Thu gọn ▲"
                    : `+${gap.present_skills.length - PRESENT_SHOW} kỹ năng khác ▼`}
                </button>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Missing skills */}
      {gap.missing_skills.length > 0 && (
        <div className="px-5 py-4">
          <button
            onClick={() => setExpanded((v) => !v)}
            className="w-full flex items-center justify-between mb-3 group"
          >
            <div className="flex items-center gap-2">
              <FiAlertCircle className="w-3.5 h-3.5 text-amber-500" />
              <span className="text-xs font-bold text-gray-600 dark:text-gray-300 uppercase tracking-wider">
                Kỹ năng thị trường cần bổ sung ({gap.missing_skills.length})
              </span>
            </div>
            <span className="text-[10px] text-gray-400 group-hover:text-gray-600 dark:group-hover:text-gray-300 transition-colors">
              {expanded ? "Thu gọn ▲" : "Xem chi tiết ▼"}
            </span>
          </button>

          <div className={`space-y-2.5 overflow-hidden transition-all duration-300 ${expanded ? "max-h-[600px]" : "max-h-[180px]"}`}>
            {gap.missing_skills.map((skill) => {
              const barWidth = Math.round((skill.market_frequency / maxFreq) * 100);
              const groupCls = SKILL_GROUP_COLOR[skill.skill_group] || SKILL_GROUP_COLOR["Other"];
              return (
                <div key={skill.skill_name} className="flex items-center gap-3">
                  {/* Skill name + group badge */}
                  <div className="w-[140px] flex-shrink-0 flex items-center gap-1.5 min-w-0">
                    <span className="text-[12px] font-semibold text-gray-800 dark:text-gray-200 truncate">
                      {skill.skill_name}
                    </span>
                    <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-medium flex-shrink-0 ${groupCls}`}>
                      {skill.skill_group}
                    </span>
                  </div>

                  {/* Frequency bar */}
                  <div className="flex-1 flex items-center gap-2">
                    <div className="flex-1 h-1.5 bg-gray-100 dark:bg-gray-800 rounded-full overflow-hidden">
                      <div
                        className="h-full rounded-full bg-gradient-to-r from-amber-400 to-orange-500"
                        style={{ width: `${barWidth}%`, transition: "width 0.8s ease" }}
                      />
                    </div>
                    <span className="text-[11px] font-semibold text-amber-600 dark:text-amber-400 w-10 text-right flex-shrink-0">
                      {skill.market_frequency}%
                    </span>
                  </div>
                </div>
              );
            })}
          </div>

          {!expanded && gap.missing_skills.length > 4 && (
            <button
              onClick={() => setExpanded(true)}
              className="mt-2 text-xs text-amber-600 dark:text-amber-400 hover:underline font-medium"
            >
              Xem thêm {gap.missing_skills.length - 4} kỹ năng còn thiếu ↓
            </button>
          )}
        </div>
      )}

      {/* Footer */}
      <div className="px-5 pb-3 flex items-center gap-2">
        <FiTrendingUp className="w-3 h-3 text-amber-400 flex-shrink-0" />
        <span className="text-[10px] text-gray-400 dark:text-gray-500">
          Tần suất xuất hiện trong {gap.total_jobs_analyzed.toLocaleString()} job postings · Gold layer (fact_job_posting ⋈ dim_skill ⋈ dim_job_category)
        </span>
      </div>
    </div>
  );
}

// ── Drop Zone ─────────────────────────────────────────────────────────────────

function DropZone({ onFile }) {
  const [drag, setDrag] = useState(false);
  const inputRef = useRef(null);

  const handleDrop = useCallback((e) => {
    e.preventDefault();
    setDrag(false);
    const f = e.dataTransfer.files[0];
    if (f) onFile(f);
  }, [onFile]);

  return (
    <div
      onDragOver={(e) => { e.preventDefault(); setDrag(true); }}
      onDragLeave={() => setDrag(false)}
      onDrop={handleDrop}
      onClick={() => inputRef.current?.click()}
      className={`
        relative flex flex-col items-center justify-center gap-5 py-14 px-8
        rounded-2xl border-2 border-dashed cursor-pointer
        transition-all duration-300 select-none overflow-hidden
        ${drag
          ? "border-blue-500 bg-blue-50 dark:bg-blue-950/30 scale-[1.01]"
          : "border-gray-200 dark:border-gray-700 bg-gray-50/50 dark:bg-gray-900/50 hover:border-blue-400 hover:bg-blue-50/50 dark:hover:bg-blue-950/20"
        }
      `}
    >
      {/* Background glow on drag */}
      {drag && (
        <div className="absolute inset-0 bg-gradient-to-br from-blue-400/5 to-indigo-400/5 pointer-events-none" />
      )}

      {/* Icon */}
      <div className={`
        relative w-20 h-20 rounded-2xl flex items-center justify-center
        transition-all duration-300 shadow-lg
        ${drag
          ? "bg-gradient-to-br from-blue-500 to-indigo-600 shadow-blue-200 dark:shadow-blue-900/60 scale-110"
          : "bg-gradient-to-br from-gray-100 to-gray-200 dark:from-gray-800 dark:to-gray-700 shadow-gray-100 dark:shadow-gray-900"
        }
      `}>
        <FiUploadCloud className={`w-9 h-9 transition-colors duration-300 ${drag ? "text-white" : "text-gray-400 dark:text-gray-500"}`} />
      </div>

      {/* Text */}
      <div className="text-center z-10">
        <p className={`font-semibold text-base transition-colors ${drag ? "text-blue-700 dark:text-blue-300" : "text-gray-700 dark:text-gray-200"}`}>
          {drag ? "Thả CV vào đây" : "Kéo thả CV vào đây"}
        </p>
        <p className="text-sm text-gray-400 dark:text-gray-500 mt-1">
          hoặc <span className="text-blue-600 dark:text-blue-400 font-medium">nhấn để chọn file</span>
        </p>
        <p className="text-xs text-gray-300 dark:text-gray-600 mt-2">
          PDF · Tối đa 5 MB
        </p>
      </div>

      <input ref={inputRef} type="file" accept=".pdf,application/pdf" className="hidden"
        onChange={(e) => { const f = e.target.files[0]; if (f) onFile(f); }} />
    </div>
  );
}

// ── Processing View ───────────────────────────────────────────────────────────

function ProcessingView({ fileName }) {
  const [activeStep, setActiveStep] = useState(0);

  useEffect(() => {
    let step = 0;
    const timers = [];
    function advance() {
      step += 1;
      if (step < PROCESSING_STEPS.length) {
        setActiveStep(step);
        timers.push(setTimeout(advance, PROCESSING_STEPS[step].duration));
      }
    }
    timers.push(setTimeout(advance, PROCESSING_STEPS[0].duration));
    return () => timers.forEach(clearTimeout);
  }, []);

  return (
    <div className="flex flex-col items-center justify-center gap-8 py-10">
      {/* Animated rings */}
      <div className="relative w-24 h-24">
        <div className="absolute inset-0 rounded-full border-4 border-gray-100 dark:border-gray-800" />
        <div className="absolute inset-0 rounded-full border-4 border-t-blue-500 border-r-indigo-500 border-b-transparent border-l-transparent animate-spin" />
        <div className="absolute inset-2 rounded-full border-3 border-t-transparent border-r-transparent border-b-violet-400 border-l-indigo-400 animate-spin"
          style={{ animationDirection: "reverse", animationDuration: "1.2s", borderWidth: "3px" }} />
        <div className="absolute inset-0 flex items-center justify-center">
          <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center shadow-lg shadow-blue-200 dark:shadow-blue-900/50">
            <FiFileText className="w-5 h-5 text-white" />
          </div>
        </div>
      </div>

      {/* File name */}
      <div className="text-center">
        <p className="text-sm font-medium text-gray-700 dark:text-gray-300">Đang xử lý</p>
        <p className="text-xs text-gray-400 dark:text-gray-500 mt-1 max-w-[220px] truncate">{fileName}</p>
      </div>

      {/* Steps */}
      <div className="w-full max-w-[280px] space-y-4">
        {PROCESSING_STEPS.map((step, i) => (
          <div key={step.id} className="flex items-center gap-3">
            {/* Dot */}
            <div className={`
              w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0
              transition-all duration-500 shadow-sm
              ${i < activeStep
                ? "bg-gradient-to-br from-emerald-400 to-teal-500 shadow-emerald-100 dark:shadow-emerald-900/50"
                : i === activeStep
                  ? "bg-gradient-to-br from-blue-500 to-indigo-600 shadow-blue-100 dark:shadow-blue-900/50 animate-pulse"
                  : "bg-gray-100 dark:bg-gray-800"
              }
            `}>
              {i < activeStep
                ? <FiCheckCircle className="w-4 h-4 text-white" />
                : <span className={`w-2 h-2 rounded-full ${i === activeStep ? "bg-white" : "bg-gray-300 dark:bg-gray-600"}`} />
              }
            </div>
            <span className={`text-sm font-medium transition-colors duration-300 ${
              i === activeStep
                ? "text-blue-700 dark:text-blue-400"
                : i < activeStep
                  ? "text-emerald-600 dark:text-emerald-400"
                  : "text-gray-300 dark:text-gray-600"
            }`}>
              {step.label}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Main Modal ────────────────────────────────────────────────────────────────

export default function CVUploadModal({ onClose }) {
  const { user }                = useAuth();
  const [stage, setStage]       = useState("checking");
  const [cvList, setCvList]       = useState([]);
  const [fileName, setFileName]   = useState("");
  const [result, setResult]       = useState(null);
  const [errMsg, setErrMsg]       = useState("");
  const [deleting, setDeleting]   = useState(null);
  const [uploading, setUploading] = useState(false);
  const [showUpload, setShowUpload] = useState(false);
  const [topK]                    = useState(10);

  const refreshCVList = useCallback(async () => {
    if (!user) return;
    try {
      const res = await fetch(`${API_URL}/user/${user.uid}/cvs`);
      const cvs = res.ok ? await res.json() : [];
      setCvList(cvs);
      setStage("list");
    } catch {
      setStage("list");
    }
  }, [user]);

  useEffect(() => {
    if (!user) { setStage("list"); return; }
    refreshCVList();
  }, [user]); // eslint-disable-line

  useEffect(() => {
    const handler = (e) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  const handleDeleteCV = useCallback(async (cv) => {
    setDeleting(cv.cv_id);
    try {
      await fetch(`${API_URL}/user/${user.uid}/cvs/${cv.cv_id}`, { method: "DELETE" });
      setCvList(prev => prev.filter(c => c.cv_id !== cv.cv_id));
    } catch (err) {
      console.error("Delete failed:", err);
    } finally {
      setDeleting(null);
    }
  }, [user]);

  // Upload CV → chỉ LƯU vào MinIO + Iceberg, KHÔNG match ngay
  const handleSaveCV = useCallback(async (file) => {
    if (!file.name.toLowerCase().endsWith(".pdf")) {
      setErrMsg("Chỉ hỗ trợ file PDF."); return;
    }
    if (file.size > 5 * 1024 * 1024) {
      setErrMsg(`File quá lớn (${(file.size / 1024 / 1024).toFixed(1)} MB). Tối đa 5 MB.`); return;
    }
    if (!user) { setErrMsg("Bạn cần đăng nhập để lưu CV."); return; }

    setUploading(true);
    setErrMsg("");
    try {
      const form = new FormData();
      form.append("file", file);
      const res = await fetch(`${API_URL}/user/${user.uid}/cvs`, { method: "POST", body: form });
      if (!res.ok) throw new Error("Upload thất bại.");
      const saved = await res.json();
      setCvList(prev => [saved, ...prev]);
      setShowUpload(false);
    } catch (err) {
      setErrMsg(err.message || "Lưu CV thất bại. Vui lòng thử lại.");
    } finally {
      setUploading(false);
    }
  }, [user]);

  // Dùng lại CV đã lưu → backend tải từ MinIO rồi match
  const handleReuseCV = useCallback(async (cv) => {
    setFileName(cv.filename);
    setStage("processing");
    try {
      const res  = await fetch(`${API_URL}/user/${user.uid}/cvs/${cv.cv_id}/match?top_k=${topK}`, {
        method: "POST",
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.detail || `Server error ${res.status}`);
      setResult(data);
      setStage("results");
    } catch (err) {
      setErrMsg(err.message || "Không thể tải lại CV. Vui lòng upload CV mới.");
      setStage("error");
    }
  }, [user, topK]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-md p-4"
    >
      <div className="relative w-full max-w-2xl max-h-[90vh] flex flex-col bg-white dark:bg-[#111] rounded-3xl shadow-2xl shadow-black/20 dark:shadow-black/60 overflow-hidden border border-gray-100 dark:border-gray-800">

        {/* Top gradient accent */}
        <div className="absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r from-blue-500 via-indigo-500 to-violet-500 z-10" />

        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100 dark:border-gray-800/80 flex-shrink-0">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-600 flex items-center justify-center shadow-md shadow-blue-200 dark:shadow-blue-900/40">
              <FiBriefcase className="w-4 h-4 text-white" />
            </div>
            <div>
              <p className="font-bold text-[15px] text-gray-900 dark:text-gray-100 leading-tight">
                CV Match
              </p>
              <p className="text-xs text-gray-400 dark:text-gray-500">Tìm việc phù hợp theo hồ sơ của bạn</p>
            </div>
          </div>
          <button onClick={onClose}
            className="w-8 h-8 flex items-center justify-center rounded-xl hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-all">
            <FiX className="w-4.5 h-4.5" />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-6 py-5 space-y-4">

          {/* Checking */}
          {stage === "checking" && (
            <div className="flex items-center justify-center py-16">
              <div className="w-8 h-8 border-[3px] border-blue-500 border-t-transparent rounded-full animate-spin" />
            </div>
          )}

          {/* List stage: hiển thị CVs + inline upload */}
          {stage === "list" && (
            <div className="space-y-3">

              {cvList.length === 0 ? (
                /* ── Chưa có CV: hiển thị dropzone full ── */
                <div className="space-y-4">
                  <p className="text-sm text-gray-500 dark:text-gray-400 leading-relaxed">
                    Chưa có CV nào. Upload CV của bạn để AI phân tích và gợi ý việc làm phù hợp.
                  </p>
                  {uploading ? (
                    <div className="flex flex-col items-center gap-3 py-12">
                      <div className="w-8 h-8 border-[3px] border-blue-500 border-t-transparent rounded-full animate-spin" />
                      <p className="text-sm text-gray-400">Đang lưu CV…</p>
                    </div>
                  ) : (
                    <DropZone onFile={handleSaveCV} />
                  )}
                  {errMsg && <p className="text-xs text-red-500 text-center">{errMsg}</p>}
                  <div className="flex items-center justify-center gap-4 pt-1">
                    {["Phân tích AI", "Matching thông minh", "Kết quả tức thì"].map((t) => (
                      <span key={t} className="flex items-center gap-1.5 text-xs text-gray-400 dark:text-gray-500">
                        <FiCheckCircle className="w-3 h-3 text-emerald-400" />{t}
                      </span>
                    ))}
                  </div>
                </div>
              ) : (
                /* ── Có CV: hiển thị list + toggle inline upload ── */
                <>
                  <div className="flex items-center justify-between">
                    <p className="text-sm font-semibold text-gray-700 dark:text-gray-300">
                      CV đã lưu ({cvList.length})
                    </p>
                    <button
                      onClick={() => { setShowUpload(v => !v); setErrMsg(""); }}
                      className="flex items-center gap-1.5 text-xs font-semibold text-blue-600 dark:text-blue-400 hover:underline"
                    >
                      <FiUploadCloud className="w-3.5 h-3.5" />
                      {showUpload ? "← Ẩn" : "+ Thêm CV"}
                    </button>
                  </div>

                  {/* Inline upload zone */}
                  {showUpload && (
                    <div className="rounded-xl border border-dashed border-blue-200 dark:border-blue-800 bg-blue-50/40 dark:bg-blue-950/20 p-4">
                      {uploading ? (
                        <div className="flex items-center justify-center gap-2 py-4 text-sm text-gray-400">
                          <div className="w-5 h-5 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
                          Đang lưu CV…
                        </div>
                      ) : (
                        <DropZone onFile={handleSaveCV} />
                      )}
                      {errMsg && <p className="text-xs text-red-500 mt-2 text-center">{errMsg}</p>}
                    </div>
                  )}

                  {/* CV items */}
                  {cvList.map((cv) => {
                    const date = cv.uploaded_at
                      ? new Date(cv.uploaded_at).toLocaleDateString("vi-VN") : "";
                    return (
                      <div key={cv.cv_id} className="flex items-center gap-3 p-3.5 rounded-xl border border-gray-100 dark:border-gray-800 bg-gray-50 dark:bg-gray-900 hover:border-blue-200 dark:hover:border-blue-800 transition-all">
                        <div className="w-10 h-10 rounded-xl bg-blue-50 dark:bg-blue-950/30 border border-blue-100 dark:border-blue-900/40 flex items-center justify-center flex-shrink-0">
                          <FiFileText className="w-5 h-5 text-blue-500" />
                        </div>
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium text-gray-900 dark:text-gray-100 truncate">{cv.filename}</p>
                          {date && <p className="text-xs text-gray-400 mt-0.5">Tải lên {date}</p>}
                        </div>
                        <div className="flex items-center gap-2 flex-shrink-0">
                          <button
                            onClick={() => handleReuseCV(cv)}
                            className="px-3.5 py-1.5 text-xs font-semibold bg-gradient-to-r from-blue-600 to-indigo-600 text-white rounded-lg hover:opacity-90 transition shadow-sm"
                          >
                            Dùng CV này
                          </button>
                          <button
                            onClick={() => handleDeleteCV(cv)}
                            disabled={deleting === cv.cv_id}
                            className="w-7 h-7 flex items-center justify-center rounded-lg text-gray-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 transition-all"
                          >
                            {deleting === cv.cv_id
                              ? <div className="w-3.5 h-3.5 border-2 border-red-400 border-t-transparent rounded-full animate-spin" />
                              : <FiTrash2 className="w-3.5 h-3.5" />}
                          </button>
                        </div>
                      </div>
                    );
                  })}
                </>
              )}
            </div>
          )}

          {/* Processing */}
          {stage === "processing" && <ProcessingView fileName={fileName} />}

          {/* Error */}
          {stage === "error" && (() => {
            const meta = getErrorMeta(errMsg);
            const Icon = meta.icon;
            return (
              <div className="flex flex-col items-center gap-5 py-8 px-2 text-center">

                {/* Icon */}
                <div className={`w-16 h-16 rounded-2xl ${meta.iconBg} border ${meta.iconBorder} flex items-center justify-center`}>
                  <Icon className={`w-7 h-7 ${meta.iconColor}`} />
                </div>

                {/* Badge + Title + Message */}
                <div className="flex flex-col items-center gap-2">
                  <span className={`text-xs font-semibold px-2.5 py-0.5 rounded-full ${meta.badgeBg}`}>
                    {meta.badge}
                  </span>
                  <p className="font-semibold text-base text-gray-900 dark:text-gray-100">
                    {meta.title}
                  </p>
                  <p className="text-sm text-gray-500 dark:text-gray-400 max-w-xs leading-relaxed">
                    {errMsg}
                  </p>
                </div>

                {/* Tips box — only for non-technical errors */}
                {meta.tips.length > 0 && (
                  <div className="w-full rounded-xl border border-dashed border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 p-4 text-left">
                    <div className="flex items-center gap-1.5 mb-2.5">
                      <FiInfo className="w-3.5 h-3.5 text-gray-400" />
                      <span className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide">
                        Gợi ý khắc phục
                      </span>
                    </div>
                    <ul className="space-y-1.5">
                      {meta.tips.map((tip, i) => (
                        <li key={i} className="flex items-start gap-2 text-xs text-gray-600 dark:text-gray-300">
                          <span className="mt-0.5 w-4 h-4 rounded-full bg-gray-200 dark:bg-gray-700 flex-shrink-0 flex items-center justify-center text-[10px] font-bold text-gray-500 dark:text-gray-400">
                            {i + 1}
                          </span>
                          {tip}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}

                {/* Actions */}
                <div className="flex flex-col items-center gap-2 w-full">
                  <button
                    onClick={() => { setErrMsg(""); setStage(meta.nextStage); }}
                    className={`w-full max-w-xs flex items-center justify-center gap-2 px-5 py-2.5 text-sm font-semibold bg-gradient-to-r ${meta.btnStyle} text-white rounded-xl hover:opacity-90 active:scale-95 transition-all shadow-md`}
                  >
                    {meta.type === "technical" ? (
                      <FiRefreshCw className="w-4 h-4" />
                    ) : (
                      <FiUpload className="w-4 h-4" />
                    )}
                    {meta.btnLabel}
                  </button>
                  <button
                    onClick={() => { setErrMsg(""); setStage("list"); }}
                    className="text-xs text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
                  >
                    Quay về danh sách CV
                  </button>
                </div>

              </div>
            );
          })()}

          {/* Results */}
          {stage === "results" && result && (
            <div>
              {/* Summary */}
              <div className="flex items-center justify-between mb-4 py-2.5 px-4 rounded-xl bg-gray-50 dark:bg-gray-900 border border-gray-100 dark:border-gray-800">
                <p className="text-sm text-gray-700 dark:text-gray-300 font-medium">{result.message?.replace(/\*\*/g, "")}</p>
                <span className="text-xs text-gray-400 dark:text-gray-500 flex-shrink-0 ml-3 flex items-center gap-1">
                  <FiClock className="w-3 h-3" />
                  {(result.processing_time_ms / 1000).toFixed(1)}s
                </span>
              </div>

              <CVProfileCard profile={result.cv_profile} />

              {(result.skill_gaps ?? (result.skill_gap ? [result.skill_gap] : [])).map((gap) => (
                <SkillGapCard key={gap.role_category} gap={gap} />
              ))}

              {result.matched_jobs?.length > 0 ? (
                <div className="space-y-2.5">
                  <p className="text-[11px] font-bold text-gray-400 dark:text-gray-500 uppercase tracking-widest mb-3">
                    {result.total_found} việc làm phù hợp
                  </p>
                  {result.matched_jobs.map((job, i) => (
                    <JobCard key={job.job_id} job={job} index={i} />
                  ))}
                </div>
              ) : (
                <div className="flex flex-col items-center gap-3 py-10 text-center">
                  <div className="w-14 h-14 rounded-2xl bg-gray-100 dark:bg-gray-800 flex items-center justify-center">
                    <FiBriefcase className="w-6 h-6 text-gray-300 dark:text-gray-600" />
                  </div>
                  <p className="text-sm text-gray-500 dark:text-gray-400 max-w-[260px] leading-relaxed">
                    Không tìm thấy việc làm phù hợp. Hãy thử CV với nhiều kỹ năng kỹ thuật cụ thể hơn.
                  </p>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        {stage === "results" && (
          <div className="flex items-center justify-between px-6 py-3.5 border-t border-gray-100 dark:border-gray-800 flex-shrink-0 bg-gray-50/80 dark:bg-gray-900/80 backdrop-blur-sm">
            <button
              onClick={() => { setResult(null); setFileName(""); setStage("list"); }}
              className="text-sm text-gray-500 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-100 transition-colors font-medium flex items-center gap-1.5"
            >
              ← {cvList.length > 0 ? "Quản lý CV" : "Upload CV khác"}
            </button>
            <button
              onClick={onClose}
              className="px-5 py-2 text-sm font-semibold bg-gray-900 dark:bg-white text-white dark:text-gray-900 rounded-xl hover:opacity-85 transition"
            >
              Đóng
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
