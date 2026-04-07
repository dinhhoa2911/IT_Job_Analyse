import React from "react";

function Message({ message }) {
  const isBot = message.sender === "bot";

  return (
    <div className={`flex gap-4 ${isBot ? "justify-start" : "justify-end"}`}>
      <div
        className={`max-w-2xl ${
          isBot
            ? message.isError
              ? "text-red-600 rounded-lg px-6 py-3 bg-red-50 border border-red-200"
              : "text-black rounded-lg px-6 py-3"
            : "bg-[#F4F4F4] text-black rounded-lg px-6 py-3"
        }`}
      >
        {/* Pre-wrap preserves newlines returned by the API */}
        <p className="text-base leading-relaxed break-words whitespace-pre-wrap">
          {message.text}
        </p>
        <span className="text-xs text-gray-400 mt-2 block">
          {message.timestamp.toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
          })}
        </span>
      </div>
    </div>
  );
}

export default Message;
