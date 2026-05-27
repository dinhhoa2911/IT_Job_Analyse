import React, { useEffect } from "react";
import { FiX } from "react-icons/fi";

/**
 * @component
 * @brief Confirmation popup shown when an unauthenticated user clicks "New Chat"
 * while the current conversation has messages.
 *
 * Mirrors ChatGPT's "Xóa đoạn chat hiện tại?" dialog.
 * Two actions: clear the chat (lose it) OR navigate to login to save it.
 *
 * @param {Object}   props
 * @param {Function} props.onClear  - Clears the current chat and creates a fresh one.
 * @param {Function} props.onLogin  - Navigates the user to the /login page.
 * @param {Function} props.onClose  - Dismisses the modal without taking action.
 */
export default function NewChatConfirmModal({ onClear, onLogin, onClose }) {
  // Close on Escape
  useEffect(() => {
    const handler = (e) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [onClose]);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div className="bg-white dark:bg-[#2A2A2A] rounded-2xl shadow-2xl w-full max-w-sm overflow-hidden border border-gray-200 dark:border-gray-700">

        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-5 pb-1">
          <h3 className="text-base font-bold text-gray-900 dark:text-white">
            Tạo đoạn chat mới?
          </h3>
          <button
            onClick={onClose}
            className="p-1.5 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 transition"
          >
            <FiX className="w-4 h-4" />
          </button>
        </div>

        {/* Body */}
        <p className="px-6 py-3 text-sm text-gray-500 dark:text-gray-400 leading-relaxed">
          Để bắt đầu đoạn chat mới, cuộc trò chuyện hiện tại sẽ bị xóa.{" "}
          <span className="font-semibold text-gray-700 dark:text-gray-300">Đăng nhập</span>{" "}
          hoặc{" "}
          <span className="font-semibold text-gray-700 dark:text-gray-300">Đăng ký</span>{" "}
          để lưu lịch sử chat.
        </p>

        {/* Actions */}
        <div className="px-6 pb-6 pt-2 space-y-2">
          <button
            onClick={onClear}
            className="w-full py-2.5 px-4 rounded-xl border border-gray-300 dark:border-gray-600 text-gray-800 dark:text-gray-200 font-semibold text-sm bg-white dark:bg-transparent hover:bg-gray-50 dark:hover:bg-gray-700/50 transition"
          >
            Xóa đoạn chat
          </button>
          <button
            onClick={onLogin}
            className="w-full py-2.5 px-4 rounded-xl bg-gray-900 dark:bg-white text-white dark:text-gray-900 font-semibold text-sm hover:bg-gray-800 dark:hover:bg-gray-100 transition"
          >
            Đăng nhập
          </button>
        </div>
      </div>
    </div>
  );
}
