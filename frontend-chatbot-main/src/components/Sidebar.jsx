import React, { useEffect, useRef, useState } from "react";
import { BsLayoutSidebar } from "react-icons/bs";
import { CiHeart } from "react-icons/ci";
import { FiEdit, FiHelpCircle, FiStar, FiTrash2 } from "react-icons/fi";
import { GoTelescope } from "react-icons/go";
import { IoSearch } from "react-icons/io5";
import { IoImagesOutline, IoSettingsOutline, IoPricetagsOutline } from "react-icons/io5";
import { LuPencilLine } from "react-icons/lu";
import { RiApps2Line } from "react-icons/ri";
import { BsThreeDots } from "react-icons/bs";

function ConversationItem({
  conv,
  onSelect,
  onStar,
  onRename,
  onDelete,
}) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [renaming, setRenaming] = useState(false);
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

  function handleRenameSubmit() {
    onRename(conv.id, renameValue);
    setRenaming(false);
  }

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

      {/* Footer */}
      <div className="border-t border-[#E2E2E2] dark:border-[#2F2F2F] p-3 space-y-2">
        <p className="text-sm font-semibold">Get responses tailored to you</p>
        <p className="text-sm text-gray-500">
          Log in to get answers based on saved chats, plus create images and upload files.
        </p>
        <button className="w-full bg-white dark:bg-[#2A2A2A] text-black dark:text-white font-semibold border border-[#E2E2E2] dark:border-[#3F3F3F] hover:bg-[#F3F3F3] dark:hover:bg-[#333] rounded-full text-sm py-2 px-4">
          Log in
        </button>
      </div>
    </div>
  );
}

export default Sidebar;
