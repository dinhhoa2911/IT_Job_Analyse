import React from 'react';

const RULES = [
  { label: 'Ít nhất 8 ký tự',     test: (p) => p.length >= 8 },
  { label: 'Chữ hoa (A-Z)',        test: (p) => /[A-Z]/.test(p) },
  { label: 'Chữ thường (a-z)',     test: (p) => /[a-z]/.test(p) },
  { label: 'Số (0-9)',             test: (p) => /[0-9]/.test(p) },
  { label: 'Ký tự đặc biệt (!@#)', test: (p) => /[!@#$%^&*()_+\-=[\]{};':"\\|,.<>/?]/.test(p) },
];

function getLevel(score) {
  if (score <= 1) return { label: 'Rất yếu',   color: 'bg-red-500',    text: 'text-red-500'   };
  if (score === 2) return { label: 'Yếu',        color: 'bg-orange-400', text: 'text-orange-500'};
  if (score === 3) return { label: 'Trung bình', color: 'bg-yellow-400', text: 'text-yellow-600'};
  if (score === 4) return { label: 'Khá mạnh',   color: 'bg-blue-500',   text: 'text-blue-600'  };
  return                  { label: 'Mạnh',       color: 'bg-emerald-500',text: 'text-emerald-600'};
}

export default function PasswordStrength({ password }) {
  if (!password) return null;

  const results = RULES.map((r) => ({ ...r, valid: r.test(password) }));
  const score   = results.filter((r) => r.valid).length;
  const level   = getLevel(score);

  return (
    <div className="mt-2 space-y-2">
      {/* Bar */}
      <div className="flex gap-1">
        {[1, 2, 3, 4, 5].map((i) => (
          <div
            key={i}
            className={`h-1.5 flex-1 rounded-full transition-all duration-300 ${
              i <= score ? level.color : 'bg-gray-200 dark:bg-gray-600'
            }`}
          />
        ))}
      </div>

      {/* Label */}
      <p className={`text-xs font-medium ${level.text}`}>
        Độ mạnh mật khẩu: {level.label}
      </p>

      {/* Checklist */}
      <ul className="grid grid-cols-2 gap-x-4 gap-y-1">
        {results.map((r) => (
          <li key={r.label} className={`flex items-center gap-1.5 text-xs transition-colors ${r.valid ? 'text-emerald-600 dark:text-emerald-400' : 'text-gray-400'}`}>
            <span className={`flex-shrink-0 w-3.5 h-3.5 rounded-full flex items-center justify-center text-[9px] font-bold ${r.valid ? 'bg-emerald-500 text-white' : 'bg-gray-200 dark:bg-gray-600'}`}>
              {r.valid ? '✓' : ''}
            </span>
            {r.label}
          </li>
        ))}
      </ul>
    </div>
  );
}
