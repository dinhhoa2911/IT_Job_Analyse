/**
 * @fileoverview Scrollable chat message list with a typing indicator.
 * @module ChatArea
 */

import React, { useEffect, useRef } from "react";
import Message from "./Message";

/**
 * @component
 * @brief Animated three-dot indicator shown while the bot is generating a reply.
 * @returns {JSX.Element}
 */
function TypingIndicator() {
  return (
    <div className="flex justify-start msg-bot">
      <div className="bg-gray-100 dark:bg-[#2F2F2F] rounded-2xl rounded-bl-sm px-4 py-3.5 flex items-center gap-1.5">
        <span className="typing-dot w-2 h-2 bg-gray-400 dark:bg-gray-500 rounded-full" style={{ animationDelay: "0ms" }} />
        <span className="typing-dot w-2 h-2 bg-gray-400 dark:bg-gray-500 rounded-full" style={{ animationDelay: "200ms" }} />
        <span className="typing-dot w-2 h-2 bg-gray-400 dark:bg-gray-500 rounded-full" style={{ animationDelay: "400ms" }} />
      </div>
    </div>
  );
}

/**
 * @component
 * @brief Renders the scrollable list of chat messages for the active conversation.
 *
 * Auto-scrolls to the bottom whenever the message list or loading state changes.
 * Displays a {@link TypingIndicator} while a bot response is in-flight.
 *
 * @param {Object}    props
 * @param {import('./Message').MessageData[]} props.messages      - Ordered list of messages to display.
 * @param {boolean}   [props.isLoading=false]                    - When true, shows the typing indicator after the last message.
 * @returns {JSX.Element}
 */
function ChatArea({ messages, isLoading = false }) {
  /** @type {React.RefObject<HTMLDivElement>} Ref to the invisible sentinel div at the bottom of the list. */
  const messagesEndRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isLoading]);

  return (
    <div className="flex-1 overflow-y-auto bg-white dark:bg-[#212121] px-4 py-6">
      <div className="max-w-3xl mx-auto space-y-1.5">
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
