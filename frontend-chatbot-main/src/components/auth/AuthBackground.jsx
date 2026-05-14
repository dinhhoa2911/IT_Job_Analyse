import React from 'react';

const TECH_KEYWORDS = [
  { text: 'Python',     style: { top: '12%',  left: '8%',  animationDelay: '0s',    animationDuration: '6s'  } },
  { text: 'React',      style: { top: '8%',   left: '70%', animationDelay: '1s',    animationDuration: '7s'  } },
  { text: 'FastAPI',    style: { top: '25%',  left: '80%', animationDelay: '2s',    animationDuration: '5s'  } },
  { text: 'Docker',     style: { top: '55%',  left: '5%',  animationDelay: '0.5s',  animationDuration: '8s'  } },
  { text: 'Milvus',     style: { top: '65%',  left: '78%', animationDelay: '1.5s',  animationDuration: '6.5s'} },
  { text: 'RAG',        style: { top: '78%',  left: '15%', animationDelay: '3s',    animationDuration: '7s'  } },
  { text: 'SQL',        style: { top: '40%',  left: '88%', animationDelay: '0.7s',  animationDuration: '9s'  } },
  { text: 'TypeScript', style: { top: '85%',  left: '62%', animationDelay: '2.5s',  animationDuration: '6s'  } },
  { text: 'Spark',      style: { top: '18%',  left: '45%', animationDelay: '1.8s',  animationDuration: '8s'  } },
  { text: 'Iceberg',    style: { top: '72%',  left: '45%', animationDelay: '0.3s',  animationDuration: '7.5s'} },
];

const floatKeyframes = `
@keyframes float {
  0%, 100% { transform: translateY(0px) rotate(0deg); opacity: 0.6; }
  50%       { transform: translateY(-14px) rotate(2deg); opacity: 1; }
}
@keyframes fadeIn {
  from { opacity: 0; transform: translateY(20px); }
  to   { opacity: 1; transform: translateY(0); }
}
`;

export default function AuthBackground() {
  return (
    <>
      <style>{floatKeyframes}</style>

      <div
        className="hidden lg:flex flex-col justify-between relative w-1/2 overflow-hidden"
        style={{
          background: 'linear-gradient(135deg, #0f172a 0%, #1e3a8a 55%, #312e81 100%)',
        }}
      >
        {/* Floating tech keywords */}
        {TECH_KEYWORDS.map((kw) => (
          <span
            key={kw.text}
            className="absolute text-xs font-mono font-semibold text-white/40 select-none pointer-events-none"
            style={{ ...kw.style, animation: `float ${kw.style.animationDuration} ease-in-out infinite`, animationDelay: kw.style.animationDelay }}
          >
            {kw.text}
          </span>
        ))}

        {/* Decorative circles */}
        <div className="absolute top-[-60px] right-[-60px] w-64 h-64 rounded-full bg-blue-500/10 blur-3xl" />
        <div className="absolute bottom-[-80px] left-[-40px] w-80 h-80 rounded-full bg-indigo-500/10 blur-3xl" />

        {/* Content */}
        <div className="relative flex flex-col justify-between h-full px-12 py-12 z-10">
          {/* Logo */}
          <div className="flex items-center gap-3" style={{ animation: 'fadeIn 0.8s ease forwards' }}>
            <span className="text-white font-bold text-xl tracking-wide">IT Job Analyse</span>
          </div>

          {/* Main content */}
          <div style={{ animation: 'fadeIn 0.8s ease 0.2s both' }}>
            <h2 className="text-5xl font-bold text-white leading-tight mb-4">
              Xin chào!
            </h2>
            <p className="text-xl text-blue-100 mb-2 font-medium">
              Nền tảng phân tích việc làm IT
            </p>
            <p className="text-base text-blue-200/80 leading-relaxed max-w-sm">
              Tìm kiếm việc làm thông minh, phân tích thị trường tuyển dụng
              và nhận tư vấn nghề nghiệp IT với công nghệ RAG tiên tiến.
            </p>

            {/* Stats */}
            <div className="flex gap-6 mt-8">
              {[
                { value: '9,600+', label: 'Việc làm IT' },
                { value: '4 tầng', label: 'Hybrid Search' },
                { value: '3 lớp', label: 'Data Lake' },
              ].map((stat) => (
                <div key={stat.label}>
                  <p className="text-2xl font-bold text-white">{stat.value}</p>
                  <p className="text-xs text-blue-300 mt-0.5">{stat.label}</p>
                </div>
              ))}
            </div>
          </div>

          {/* Social icons */}
          <div className="flex gap-4" style={{ animation: 'fadeIn 0.8s ease 0.4s both' }}>
            {['f', 'in', '🐦', 'yt'].map((icon, i) => (
              <button
                key={i}
                className="w-9 h-9 rounded-full bg-white/10 hover:bg-white/20 transition flex items-center justify-center text-white/70 hover:text-white text-xs font-bold"
              >
                {icon}
              </button>
            ))}
          </div>
        </div>
      </div>
    </>
  );
}
