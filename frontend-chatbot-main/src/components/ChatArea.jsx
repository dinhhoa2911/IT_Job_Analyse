import React, { useEffect, useRef } from "react";
import Message from "./Message";

function TypingIndicator() {
  return (
    <div className="flex gap-4 justify-start">
      <div className="text-black dark:text-white rounded-lg px-6 py-4">
        <div className="flex items-center gap-1">
          <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce [animation-delay:-0.3s]" />
          <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce [animation-delay:-0.15s]" />
          <span className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" />
        </div>
      </div>
    </div>
  );
}

function ChatArea({ messages, isLoading = false }) {
  const messagesEndRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isLoading]);

  return (
    <div className="flex-1 overflow-y-auto bg-white dark:bg-[#212121] px-4 py-8">
      <div className="max-w-3xl mx-auto space-y-4">
        {messages.map((message) => (
          <Message key={message.id} message={message} />
        ))}
        {isLoading && <TypingIndicator />}
        <div ref={messagesEndRef} />
      </div>
    </div>
  );
}

export default ChatArea;
