/**
 * @fileoverview Chat message display component.
 * @module Message
 */

import React from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
import ChartRenderer from "./ChartRenderer";
import MarketInsightCard from "./MarketInsightCard";

/**
 * @typedef {Object} MessageData
 * @property {number}          id            - Unique message ID.
 * @property {string}          text          - Message body (markdown for bot messages).
 * @property {'user'|'bot'}    sender        - Who sent the message.
 * @property {string}          timestamp     - ISO 8601 timestamp.
 * @property {string}          [queryType]   - RAG query type (bot only).
 * @property {Object}          [chart]       - Chart.js spec (bot only).
 * @property {Object}          [marketInsight] - Gold-layer market insight data (bot only).
 * @property {boolean}         [isError]     - True when the message is an error reply.
 */

/**
 * Custom renderer overrides passed to ReactMarkdown.
 * Maps standard HTML elements to Tailwind-styled equivalents.
 * @type {Object}
 */
const markdownComponents = {
  a: ({ href, children }) => (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className="text-blue-500 hover:text-blue-600 underline break-all"
    >
      {children}
    </a>
  ),
  strong: ({ children }) => (
    <strong className="font-semibold">{children}</strong>
  ),
  hr: () => <hr className="my-3 border-gray-200 dark:border-gray-600" />,
  p: ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
  ul: ({ children }) => <ul className="list-disc pl-5 mb-2 space-y-1">{children}</ul>,
  ol: ({ children }) => <ol className="list-decimal pl-5 mb-2 space-y-1">{children}</ol>,
  li: ({ children }) => <li>{children}</li>,
  h1: ({ children }) => <h1 className="text-lg font-bold mb-2">{children}</h1>,
  h2: ({ children }) => <h2 className="text-base font-bold mb-1">{children}</h2>,
  h3: ({ children }) => <h3 className="text-sm font-bold mb-1">{children}</h3>,
  code: ({ inline, children }) =>
    inline ? (
      <code className="bg-gray-100 dark:bg-gray-700 px-1 rounded text-sm font-mono">
        {children}
      </code>
    ) : (
      <pre className="bg-gray-100 dark:bg-gray-700 rounded p-3 overflow-x-auto text-sm font-mono mb-2">
        <code>{children}</code>
      </pre>
    ),
};

/**
 * @component
 * @brief Renders a single chat message bubble.
 *
 * Bot messages are rendered as markdown with optional chart and market insight card.
 * User messages are displayed as plain text in a gray bubble.
 * Error messages from the bot use a red styling variant.
 *
 * @param {Object}      props
 * @param {MessageData} props.message - The message object to display.
 * @returns {JSX.Element}
 */
function Message({ message }) {
  const isBot = message.sender === "bot";

  return (
    <div className={`flex gap-4 ${isBot ? "justify-start" : "justify-end"}`}>
      <div
        className={`max-w-2xl ${
          isBot
            ? message.isError
              ? "text-red-600 rounded-lg px-6 py-3 bg-red-50 border border-red-200"
              : "text-black dark:text-white rounded-lg px-6 py-3"
            : "bg-[#F4F4F4] dark:bg-[#2F2F2F] text-black dark:text-white rounded-lg px-6 py-3"
        }`}
      >
        {isBot && !message.isError ? (
          <div className="text-base leading-relaxed break-words">
            <ReactMarkdown remarkPlugins={[remarkGfm]} components={markdownComponents}>
              {message.text}
            </ReactMarkdown>
          </div>
        ) : (
          <p className="text-base leading-relaxed break-words whitespace-pre-wrap">
            {message.text}
          </p>
        )}

        {isBot && message.chart && <ChartRenderer chart={message.chart} />}
        {isBot && message.marketInsight && (
          <MarketInsightCard insight={message.marketInsight} />
        )}

        <span className="text-xs text-gray-400 mt-2 block">
          {new Date(message.timestamp).toLocaleTimeString([], {
            hour: "2-digit",
            minute: "2-digit",
          })}
        </span>
      </div>
    </div>
  );
}

export default Message;
