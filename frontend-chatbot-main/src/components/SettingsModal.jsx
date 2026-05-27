/**
 * @fileoverview Application settings modal component.
 * Currently exposes colour-scheme (theme) selection.
 * @module SettingsModal
 */

import React from "react";
import { GoX } from "react-icons/go";

/**
 * @component
 * @brief Full-screen backdrop modal for configuring application settings.
 *
 * Provides a two-column layout: a left navigation panel and a right content panel.
 * The only active setting is the Appearance (theme) selector.
 *
 * @param {Object}   props
 * @param {function(): void}    props.onClose   - Called to close and unmount the modal.
 * @param {'system'|'light'|'dark'} props.theme - The currently active colour-scheme preference.
 * @param {function('system'|'light'|'dark'): void} props.setTheme - Called with the newly selected theme.
 * @returns {JSX.Element}
 */
function SettingsModal({ onClose, theme, setTheme }) {
  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center">
      <div className="w-[650px] bg-white dark:bg-[#1E1E1E] rounded-3xl shadow-2xl flex overflow-hidden border border-gray-200 dark:border-gray-700">
        {/* Left Sidebar */}
        <div className="w-1/3 bg-gray-100 dark:bg-[#2A2A2A] p-4 space-y-2">
          <button className="w-full text-left px-3 py-2 rounded-lg bg-white dark:bg-[#3A3A3A] font-medium shadow-sm">
            General
          </button>
          <button className="w-full text-left px-3 py-2 rounded-lg text-gray-500 hover:bg-white/60 dark:hover:bg-[#3A3A3A] transition">
            Data controls
          </button>
        </div>

        {/* Right Content */}
        <div className="flex-1 p-6">
          {/* Header */}
          <div className="flex justify-between items-center mb-6">
            <h2 className="text-xl font-semibold">Settings</h2>
            <button
              onClick={onClose}
              className="p-2 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 transition"
            >
              <GoX size={18} />
            </button>
          </div>

          {/* Appearance */}
          <div className="mb-6">
            <p className="mb-3 text-sm font-medium text-gray-600 dark:text-gray-300">
              Appearance
            </p>

            <div className="flex gap-3">
              {["system", "light", "dark"].map((mode) => (
                <button
                  key={mode}
                  onClick={() => setTheme(mode)}
                  className={`flex-1 px-4 py-2 rounded-xl border transition-all duration-200 capitalize
                    ${
                      theme === mode
                        ? "bg-black text-white dark:bg-white dark:text-black shadow-md scale-105"
                        : "hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-600 dark:text-gray-300"
                    }
                  `}
                >
                  {mode}
                </button>
              ))}
            </div>
          </div>

          {/* Future section */}
          <div className="text-sm text-gray-400">
            More settings coming soon...
          </div>
        </div>
      </div>
    </div>
  );
}

export default SettingsModal;
