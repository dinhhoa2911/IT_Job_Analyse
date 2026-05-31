/**
 * @fileoverview Login / sign-up modal component with OAuth and email options.
 * @module LoginModal
 */

import React, { useState } from "react";
import { GoX } from "react-icons/go";
import { FcGoogle } from "react-icons/fc";
import { FaApple } from "react-icons/fa";
import { GiPhone } from "react-icons/gi";

/**
 * @component
 * @brief Modal dialog presenting login and sign-up options.
 *
 * Supports OAuth flows (Google, Apple, Phone) and an email/password entry.
 * Renders nothing when `isOpen` is false.
 *
 * @param {Object}       props
 * @param {boolean}      props.isOpen  - Controls whether the modal is visible.
 * @param {function(): void} props.onClose - Called to close and unmount the modal.
 * @returns {JSX.Element|null}
 */
function LoginModal({ isOpen, onClose }) {
  // {string} Current value of the email address input
  const [email, setEmail] = useState("");

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center">
      <div className="relative w-[420px] bg-white dark:bg-[#1E1E1E] rounded-2xl shadow-2xl p-8 border border-gray-200 dark:border-gray-700">
        {/* Close Button */}
        <button
          onClick={onClose}
          className="absolute -top-2 -right-2 p-2 bg-white dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-full transition text-black dark:text-white shadow-lg z-10"
          title="Close"
        >
          <GoX size={20} />
        </button>

        {/* Header */}
        <div className="text-center mb-6">
          <h1 className="text-2xl font-bold text-black dark:text-white mb-2">
            Log in or sign up
          </h1>
          <p className="text-sm text-gray-600 dark:text-gray-400">
            You'll get smarter responses and can upload files, images, and more.
          </p>
        </div>

        {/* OAuth Buttons */}
        <div className="space-y-3 mb-6">
          {/* Continue with Google */}
          <button className="w-full flex items-center justify-center gap-3 px-4 py-3 border border-gray-300 dark:border-gray-600 rounded-xl hover:bg-gray-50 dark:hover:bg-gray-700 transition text-gray-700 dark:text-gray-300 font-500">
            <FcGoogle size={20} />
            <span>Continue with Google</span>
          </button>

          {/* Continue with Apple */}
          <button className="w-full flex items-center justify-center gap-3 px-4 py-3 border border-gray-300 dark:border-gray-600 rounded-xl hover:bg-gray-50 dark:hover:bg-gray-700 transition text-gray-700 dark:text-gray-300 font-500">
            <FaApple size={20} className="text-black dark:text-white" />
            <span>Continue with Apple</span>
          </button>

          {/* Continue with Phone */}
          <button className="w-full flex items-center justify-center gap-3 px-4 py-3 border border-gray-300 dark:border-gray-600 rounded-xl hover:bg-gray-50 dark:hover:bg-gray-700 transition text-gray-700 dark:text-gray-300 font-500">
            <GiPhone size={20} className="text-gray-600 dark:text-gray-400" />
            <span>Continue with phone</span>
          </button>
        </div>

        {/* Divider */}
        <div className="flex items-center gap-3 mb-6">
          <div className="flex-1 h-px bg-gray-300 dark:bg-gray-600"></div>
          <span className="text-sm text-gray-500 dark:text-gray-400 font-medium">OR</span>
          <div className="flex-1 h-px bg-gray-300 dark:bg-gray-600"></div>
        </div>

        {/* Email Input */}
        <div className="mb-4">
          <input
            type="email"
            placeholder="Email address"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="w-full px-4 py-3 border border-gray-300 dark:border-gray-600 rounded-xl bg-white dark:bg-gray-800 text-black dark:text-white placeholder-gray-500 dark:placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-black dark:focus:ring-white"
          />
        </div>

        {/* Continue Button */}
        <button
          onClick={() => { /* TODO: implement email auth */ }}
          disabled={!email}
          className="w-full px-4 py-3 bg-black dark:bg-white text-white dark:text-black rounded-xl font-semibold hover:bg-gray-800 dark:hover:bg-gray-100 transition disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Continue
        </button>

        {/* Footer */}
        <p className="text-xs text-gray-500 dark:text-gray-400 text-center mt-6">
          By logging in, you agree to our <span className="underline">Terms</span> and <span className="underline">Privacy Policy</span>.
        </p>
      </div>
    </div>
  );
}

export default LoginModal;
