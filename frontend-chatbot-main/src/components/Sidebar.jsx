/**
 * @fileoverview Sidebar navigation component with conversation list management.
 * @module Sidebar
 */

import React, { useEffect, useRef, useState } from "react";
import { BsLayoutSidebar } from "react-icons/bs";
import { CiHeart } from "react-icons/ci";
import { FiChevronUp, FiEdit, FiHelpCircle, FiLogOut, FiStar, FiTrash2, FiUser } from "react-icons/fi";
import { GoTelescope } from "react-icons/go";
import { IoSearch } from "react-icons/io5";
import { IoImagesOutline, IoSettingsOutline, IoPricetagsOutline } from "react-icons/io5";
import { useAuth } from "../contexts/AuthContext";
import { LuPencilLine } from "react-icons/lu";
import { RiApps2Line } from "react-icons/ri";
import { BsThreeDots } from "react-icons/bs";

/**
 * @component
 * @brief Displays the authenticated user's avatar, name, and a logout option.
 * Shown at the bottom of the sidebar. Replaces the old "Log in" footer.
 */
function UserWidget() {
  const { user, logout } = useAuth();
  const [open, setOpen]  = useState(false);
  const ref              = useRef(null);

  // Close popup when clicking outside
  useEffect(() => {
    if (!open) return;
    const handler = (e) => { if (ref.current && !ref.current.contains(e.target)) setOpen(false); };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  if (!user) return (
    <div className="border-t border-[#E2E2E2] dark:border-[#2F2F2F] p-3 space-y-2">
      <a
        href="/login"
        className="flex items-center justify-center w-full py-2 rounded-xl bg-gray-900 dark:bg-white text-white dark:text-gray-900 text-sm font-semibold hover:opacity-90 transition"
      >
        Đăng nhập
      </a>
      <a
        href="/register"
        className="flex items-center justify-center w-full py-2 rounded-xl border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 text-sm font-medium hover:bg-gray-50 dark:hover:bg-gray-800/50 transition"
      >
        Đăng ký miễn phí
      </a>
    </div>
  );

  const displayName = user.displayName || user.email?.split("@")[0] || "User";
  const email       = user.email || "";
  const initial     = displayName[0]?.toUpperCase() ?? "U";

  return (
    <div ref={ref} className="relative border-t border-[#E2E2E2] dark:border-[#2F2F2F] p-2">
      {/* Popup menu */}
      {open && (
        <div className="absolute bottom-full left-2 right-2 mb-1 bg-white dark:bg-[#2A2A2A] border border-[#E2E2E2] dark:border-[#3F3F3F] rounded-xl shadow-lg py-1 text-sm overflow-hidden z-50">
          {/* User info (read-only) */}
          <div className="flex items-center gap-2.5 px-4 py-3 border-b border-[#E2E2E2] dark:border-[#3F3F3F]">
            <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center text-white text-sm font-bold flex-shrink-0">
              {initial}
            </div>
            <div className="min-w-0">
              <p className="font-semibold text-xs truncate">{displayName}</p>
              <p className="text-[11px] text-gray-400 truncate">{email}</p>
            </div>
          </div>
          {/* Profile */}
          <button className="flex items-center gap-3 w-full px-4 py-2 hover:bg-[#F3F3F3] dark:hover:bg-[#353535] transition text-left">
            <FiUser className="w-4 h-4 text-gray-500" />
            Thông tin tài khoản
          </button>
          {/* Logout */}
          <button
            onClick={() => { setOpen(false); logout(); }}
            className="flex items-center gap-3 w-full px-4 py-2 hover:bg-red-50 dark:hover:bg-red-900/20 text-red-500 transition text-left"
          >
            <FiLogOut className="w-4 h-4" />
            Đăng xuất
          </button>
        </div>
      )}

      {/* Trigger button */}
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-2.5 w-full px-3 py-2 rounded-xl hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition text-left group"
      >
        {/* Avatar */}
        <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center text-white text-sm font-bold flex-shrink-0">
          {initial}
        </div>
        {/* Name */}
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold truncate">{displayName}</p>
          <p className="text-[11px] text-gray-400 truncate">{email}</p>
        </div>
        {/* Chevron */}
        <FiChevronUp className={`w-4 h-4 text-gray-400 transition-transform ${open ? "" : "rotate-180"}`} />
      </button>
    </div>
  );
}

/**
 * @typedef {Object} ConversationSummary
 * @property {string}  id      - Unique conversation ID.
 * @property {string}  title   - Display title of the conversation.
 * @property {boolean} active  - Whether this conversation is currently selected.
 * @property {boolean} starred - Whether the user has starred this conversation.
 */

/**
 * @component
 * @brief Single row in the conversation list with an inline rename field and a context menu.
 *
 * Shows a three-dot menu on hover that exposes Star, Rename, and Delete actions.
 * While renaming, the title is replaced with an inline text input.
 *
 * @param {Object}               props
 * @param {ConversationSummary}  props.conv      - The conversation data to render.
 * @param {function(string): void}  props.onSelect  - Called with the conversation ID when the row is clicked.
 * @param {function(string): void}  props.onStar    - Called with the conversation ID to toggle the starred state.
 * @param {function(string, string): void} props.onRename - Called with (id, newTitle) when rename is confirmed.
 * @param {function(string): void}  props.onDelete  - Called with the conversation ID to delete it.
 * @returns {JSX.Element}
 */
function ConversationItem({
  conv,
  onSelect,
  onStar,
  onRename,
  onDelete,
}) {
  // {boolean} Whether the three-dot context menu is open
  const [menuOpen, setMenuOpen] = useState(false);
  // {boolean} Whether the inline rename input is active
  const [renaming, setRenaming] = useState(false);
  // {string} Current value of the rename input field
  const [renameValue, setRenameValue] = useState(conv.title);
  const menuRef = useRef(null);
  const inputRef = useRef(null);

  // Close menu when clicking outside
  useEffect(() => {
    if (!menuOpen) return;
    function handleClickOutside(e) {
      if (menuRef.current && !menuRef.current.contains(e.target)) {
        setMenuOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [menuOpen]);

  // Focus input when rename starts
  useEffect(() => {
    if (renaming && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [renaming]);

  /**
   * Commits the rename value and exits rename mode.
   * @returns {void}
   */
  function handleRenameSubmit() {
    onRename(conv.id, renameValue);
    setRenaming(false);
  }

  /**
   * Handles keyboard events in the rename input.
   * Enter commits; Escape cancels and restores the original title.
   * @param {React.KeyboardEvent} e
   * @returns {void}
   */
  function handleRenameKeyDown(e) {
    if (e.key === "Enter") handleRenameSubmit();
    if (e.key === "Escape") {
      setRenameValue(conv.title);
      setRenaming(false);
    }
  }

  return (
    <div
      className={`group relative flex items-center rounded-lg text-sm cursor-pointer ${
        conv.active
          ? "bg-[#E2E2E2] dark:bg-[#2F2F2F]"
          : "hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F]"
      }`}
    >
      {/* Main row */}
      <div
        className="flex items-center gap-1.5 flex-1 min-w-0 px-3 py-2"
        onClick={() => !renaming && onSelect(conv.id)}
      >
        {conv.starred && (
          <FiStar className="w-3 h-3 text-yellow-400 fill-yellow-400 flex-shrink-0" />
        )}
        {renaming ? (
          <input
            ref={inputRef}
            value={renameValue}
            onChange={(e) => setRenameValue(e.target.value)}
            onBlur={handleRenameSubmit}
            onKeyDown={handleRenameKeyDown}
            onClick={(e) => e.stopPropagation()}
            className="flex-1 min-w-0 bg-transparent outline-none border-b border-gray-400 dark:border-gray-500 text-sm"
          />
        ) : (
          <span className="truncate flex-1">{conv.title}</span>
        )}
      </div>

      {/* "..." button — visible on hover or when menu is open */}
      {!renaming && (
        <div className="relative flex-shrink-0 pr-1" ref={menuRef}>
          <button
            onClick={(e) => {
              e.stopPropagation();
              setMenuOpen((v) => !v);
            }}
            className={`p-1.5 rounded-md transition ${
              menuOpen
                ? "opacity-100 bg-[#D0D0D0] dark:bg-[#3A3A3A]"
                : "opacity-0 group-hover:opacity-100 hover:bg-[#D0D0D0] dark:hover:bg-[#3A3A3A]"
            }`}
          >
            <BsThreeDots className="w-3.5 h-3.5" />
          </button>

          {/* Dropdown */}
          {menuOpen && (
            <div className="absolute right-0 top-8 z-50 w-44 bg-white dark:bg-[#2A2A2A] border border-[#E2E2E2] dark:border-[#3F3F3F] rounded-xl shadow-lg py-1 text-sm">
              {/* Star */}
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  onStar(conv.id);
                  setMenuOpen(false);
                }}
                className="flex items-center gap-3 w-full px-4 py-2 hover:bg-[#F3F3F3] dark:hover:bg-[#353535] transition"
              >
                <FiStar className={`w-4 h-4 ${conv.starred ? "text-yellow-400 fill-yellow-400" : ""}`} />
                {conv.starred ? "Unstar" : "Star"}
              </button>

              {/* Rename */}
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  setRenameValue(conv.title);
                  setRenaming(true);
                  setMenuOpen(false);
                }}
                className="flex items-center gap-3 w-full px-4 py-2 hover:bg-[#F3F3F3] dark:hover:bg-[#353535] transition"
              >
                <LuPencilLine className="w-4 h-4" />
                Rename
              </button>

              <div className="my-1 border-t border-[#E2E2E2] dark:border-[#3F3F3F]" />

              {/* Delete */}
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  onDelete(conv.id);
                  setMenuOpen(false);
                }}
                className="flex items-center gap-3 w-full px-4 py-2 hover:bg-red-50 dark:hover:bg-red-900/20 text-red-500 transition"
              >
                <FiTrash2 className="w-4 h-4" />
                Delete
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * @component
 * @brief Full-height sidebar with navigation menu, starred/recent conversation lists, and a footer.
 *
 * Splits the conversation list into "Starred" and "Recent" sections.
 * Delegates conversation CRUD actions to the parent via callback props.
 *
 * @param {Object}   props
 * @param {ConversationSummary[]}           props.conversations         - Full list of conversations.
 * @param {function(): void}                props.onNewConversation     - Called to create a new conversation.
 * @param {function(string): void}          props.onSelectConversation  - Called with the ID of the selected conversation.
 * @param {function(string): void}          props.onStarConversation    - Called with the ID to toggle starred state.
 * @param {function(string, string): void}  props.onRenameConversation  - Called with (id, newTitle) to rename.
 * @param {function(string): void}          props.onDeleteConversation  - Called with the ID to delete.
 * @param {function(): void}                props.onCloseSidebar        - Called to collapse the sidebar.
 * @param {function(): void}                props.onOpenSettings        - Called to open the settings modal.
 * @returns {JSX.Element}
 */
function Sidebar({
  conversations,
  onNewConversation,
  onSelectConversation,
  onStarConversation,
  onRenameConversation,
  onDeleteConversation,
  onCloseSidebar,
  onOpenSettings,
}) {
  const starred = conversations.filter((c) => c.starred);
  const recent  = conversations.filter((c) => !c.starred);

  return (
    <div className="w-64 bg-[#F9F9F9] dark:bg-[#171717] border-r border-[#E2E2E2] dark:border-[#2F2F2F] flex flex-col h-screen text-black dark:text-white">

      {/* Header */}
      <div className="p-1 flex items-center justify-between">
        <button className="p-2 hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] rounded-lg transition">
          🤖
        </button>
        <button
          onClick={onCloseSidebar}
          className="p-2 hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] rounded-lg transition text-gray-500 hover:text-black dark:hover:text-white"
        >
          <BsLayoutSidebar className="w-5 h-5" />
        </button>
      </div>

      {/* Menu */}
      <div className="p-2 space-y-1">
        <button
          onClick={onNewConversation}
          className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition"
        >
          <FiEdit className="w-4 h-4" />
          <span className="text-sm">New chat</span>
        </button>
        <button className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition">
          <IoSearch className="w-4 h-4" />
          <span className="text-sm">Search chats</span>
        </button>
        <button className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition">
          <IoImagesOutline className="w-4 h-4" />
          <span className="text-sm">Images</span>
        </button>
        <button className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition">
          <RiApps2Line className="w-4 h-4" />
          <span className="text-sm">Apps</span>
        </button>
        <button className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition">
          <GoTelescope className="w-4 h-4" />
          <span className="text-sm">Deep research</span>
        </button>
        <button className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition">
          <CiHeart className="w-4 h-4" />
          <span className="text-sm">Health</span>
        </button>
      </div>

      {/* Conversation list */}
      <div className="flex-1 overflow-y-auto px-2 py-2 space-y-4">

        {starred.length > 0 && (
          <div>
            <p className="text-xs text-gray-500 px-2 mb-1">Starred</p>
            <div className="space-y-0.5">
              {starred.map((conv) => (
                <ConversationItem
                  key={conv.id}
                  conv={conv}
                  onSelect={onSelectConversation}
                  onStar={onStarConversation}
                  onRename={onRenameConversation}
                  onDelete={onDeleteConversation}
                />
              ))}
            </div>
          </div>
        )}

        {recent.length > 0 && (
          <div>
            <p className="text-xs text-gray-500 px-2 mb-1">Recent</p>
            <div className="space-y-0.5">
              {recent.map((conv) => (
                <ConversationItem
                  key={conv.id}
                  conv={conv}
                  onSelect={onSelectConversation}
                  onStar={onStarConversation}
                  onRename={onRenameConversation}
                  onDelete={onDeleteConversation}
                />
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Bottom menu */}
      <div className="flex flex-wrap p-3">
        <button className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition">
          <IoPricetagsOutline className="w-4 h-4" />
          <span className="text-sm">See plans and pricing</span>
        </button>
        <button
          onClick={onOpenSettings}
          className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition"
        >
          <IoSettingsOutline className="w-4 h-4" />
          <span className="text-sm">Setting</span>
        </button>
        <button className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] dark:hover:bg-[#2F2F2F] transition">
          <FiHelpCircle className="w-4 h-4 text-gray-500" />
          <span className="text-sm">Help</span>
        </button>
      </div>

      {/* User widget */}
      <UserWidget />
    </div>
  );
}

export default Sidebar;
