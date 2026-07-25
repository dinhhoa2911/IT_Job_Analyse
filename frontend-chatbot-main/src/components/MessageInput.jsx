import React, { useCallback, useEffect, useRef, useState } from "react";
import { FiFileText, FiMic, FiMicOff, FiSquare } from "react-icons/fi";
import { LuArrowUp } from "react-icons/lu";
import { PiWaveformBold } from "react-icons/pi";

// ── Tech-term post-correction ─────────────────────────────────────────────────
// Chrome vi-VN phonetically mangles English tech symbols.
// This table fixes the most common cases after recognition.
const CORRECTIONS = [
  // C++ / C#
  [/xi cộng cộng/gi,  'C++'],
  [/suy cộng cộng/gi, 'C++'],
  [/c cộng cộng/gi,   'C++'],
  [/xi cộng\b/gi,     'C+'],
  [/xi sắc\b/gi,      'C#'],
  [/xi thăng\b/gi,    'C#'],
  // job
  [/\bchớp\b/gi,   'job'],
  [/\bdọp\b/gi,    'job'],
  [/\bgióp\b/gi,   'job'],
  [/\bjóp\b/gi,    'job'],
  [/\bjob\b/gi,    'job'],
  // Python
  [/pay thon\b/gi, 'Python'],
  [/pi thon\b/gi,  'Python'],
  [/pây thơn\b/gi, 'Python'],
  // Docker
  [/đốc cơ\b/gi,   'Docker'],
  [/đốc ker\b/gi,  'Docker'],
  // React / Next.js
  [/ri ắc\b/gi,    'React'],
  [/nếch\b/gi,     'Next'],
  // Vue
  [/viu\b/gi,      'Vue'],
  // Java
  [/gia va\b/gi,   'Java'],
  // TypeScript / JavaScript
  [/tai pờ script\b/gi, 'TypeScript'],
  [/gia va script\b/gi, 'JavaScript'],
  // SQL
  [/ét cu el\b/gi,  'SQL'],
  [/si cu en\b/gi,  'SQL'],
  // Git
  [/gít\b/gi,   'Git'],
  // AWS
  [/a đáp liu ét\b/gi, 'AWS'],
];

function correctTechTerms(text) {
  let r = text;
  for (const [pat, fix] of CORRECTIONS) r = r.replace(pat, fix);
  return r;
}

// ─────────────────────────────────────────────────────────────────────────────

function MessageInput({
  onSendMessage,
  onOpenCVModal,
  onVoiceSubmit  = () => {},
  onStop         = () => {},
  isLoading      = false,
  isSpeaking     = false,
}) {
  const [input, setInput]             = useState("");
  const [isListening, setIsListening] = useState(false);
  const [noSupport, setNoSupport]     = useState(false);
  const recognitionRef                = useRef(null);
  const finalTranscriptRef            = useRef("");

  useEffect(() => () => recognitionRef.current?.abort(), []);

  const stopListening = useCallback(() => {
    recognitionRef.current?.stop();
    setIsListening(false);
  }, []);

  const startListening = useCallback(() => {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) { setNoSupport(true); return; }

    const r = new SR();
    r.lang            = "vi-VN";
    r.continuous      = false;
    r.interimResults  = true;
    r.maxAlternatives = 3; // more alternatives → better correction chance

    finalTranscriptRef.current = "";
    r.onstart = () => setIsListening(true);

    r.onresult = (e) => {
      let interim = "", final = "";
      for (const result of e.results) {
        if (result.isFinal) final   += result[0].transcript;
        else                interim += result[0].transcript;
      }
      const raw = final || interim;
      finalTranscriptRef.current = raw;
      setInput(correctTechTerms(raw)); // show corrected preview
    };

    r.onend = () => {
      setIsListening(false);
      const text = correctTechTerms(finalTranscriptRef.current.trim());
      if (text) {
        onSendMessage(text);
        onVoiceSubmit();
        setInput("");
        finalTranscriptRef.current = "";
      }
    };

    r.onerror = (e) => {
      if (e.error !== "no-speech" && e.error !== "aborted")
        console.warn("Speech recognition error:", e.error);
      setIsListening(false);
    };

    recognitionRef.current = r;
    r.start();
  }, [onSendMessage, onVoiceSubmit]);

  const toggleListening = useCallback(() => {
    if (isListening) stopListening();
    else             startListening();
  }, [isListening, startListening, stopListening]);

  const handleSubmit = (e) => {
    e.preventDefault();
    const trimmed = input.trim();
    if (!trimmed || isLoading) return;
    onSendMessage(trimmed);
    setInput("");
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); handleSubmit(e); }
  };

  return (
    <div className="w-full max-w-2xl mx-auto space-y-1.5">

      {/* Listening indicator — only shown while mic is active */}
      {isListening && (
        <div className="flex items-center justify-center gap-2 text-sm text-red-500 dark:text-red-400 animate-pulse">
          <span className="flex items-end gap-0.5 h-4">
            {[1,2,3,4,3,2,1].map((h, i) => (
              <span key={i} className="w-0.5 bg-red-500 dark:bg-red-400 rounded-full animate-bounce"
                style={{ height: `${h*4}px`, animationDelay: `${i*80}ms`, animationDuration: "600ms" }} />
            ))}
          </span>
          <span className="font-medium">Đang nghe… nhấn mic để dừng</span>
        </div>
      )}

      {/* Input bar */}
      <form
        onSubmit={handleSubmit}
        className={`
          flex items-center gap-2 bg-white dark:bg-[#2F2F2F]
          rounded-full px-5 py-3 border shadow-sm transition-all duration-200
          ${isListening
            ? "border-red-400 dark:border-red-500 shadow-red-100 dark:shadow-red-900/30"
            : "border-[#E2E2E2] dark:border-[#444]"
          }
        `}
      >
        {/* CV Match */}
        <button type="button" onClick={onOpenCVModal}
          disabled={isLoading || isListening}
          className="text-gray-500 hover:text-blue-600 dark:text-gray-400 dark:hover:text-blue-400 transition flex-shrink-0 disabled:opacity-40"
          title="Match CV với việc làm"
        >
          <FiFileText className="w-5 h-5" />
        </button>

        {/* Text input */}
        <input
          type="text" value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder={
            isListening ? "Đang lắng nghe… (VI + EN đều được)"  :
            isLoading   ? "Nhấn ■ để dừng, hoặc nhập câu tiếp…" :
            isSpeaking  ? "AI đang phản hồi…"                   :
            noSupport   ? "Trình duyệt không hỗ trợ mic"        :
            "Hỏi về việc làm IT, hoặc upload CV để tìm việc phù hợp…"
          }
          disabled={isListening}
          className="flex-1 bg-transparent text-black dark:text-white outline-none placeholder-gray-400 dark:placeholder-gray-500 disabled:opacity-60 text-base"
        />

        {/* Mic */}
        <button type="button" onClick={toggleListening}
          disabled={isLoading || isSpeaking || noSupport}
          title={isListening ? "Dừng ghi âm" : "Nhấn để nói (VI + EN)"}
          className={`
            relative transition flex-shrink-0 disabled:opacity-40
            ${isListening
              ? "text-red-500 dark:text-red-400"
              : "text-gray-400 hover:text-gray-700 dark:text-gray-500 dark:hover:text-gray-300"
            }
          `}
        >
          {isListening && <span className="absolute inset-0 rounded-full bg-red-400/30 animate-ping" />}
          {isListening ? <FiMicOff className="w-5 h-5 relative" /> : <FiMic className="w-5 h-5 relative" />}
        </button>

        {/* Stop (while loading) / Send (idle) */}
        {isLoading ? (
          <button
            type="button"
            onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); onStop(); }}
            className="w-8 h-8 rounded-full bg-black dark:bg-white flex items-center justify-center flex-shrink-0 cursor-pointer"
            title="Dừng"
          >
            <FiSquare className="w-3 h-3 text-white dark:text-black pointer-events-none" />
          </button>
        ) : (
          <button type="submit"
            disabled={input.trim() === "" || isListening}
            className="w-8 h-8 rounded-full bg-black dark:bg-white flex items-center justify-center transition flex-shrink-0 disabled:opacity-40"
            title="Gửi"
          >
            {input.trim() === ""
              ? <PiWaveformBold className="w-5 h-5 text-white dark:text-black" />
              : <LuArrowUp      className="w-5 h-5 text-white dark:text-black" />
            }
          </button>
        )}
      </form>
    </div>
  );
}

export default MessageInput;
