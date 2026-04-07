import React from "react";
import { MdExpandMore } from "react-icons/md";
import { MdMenu } from "react-icons/md";
import { MdMoreVert } from "react-icons/md";
import { MdShare } from "react-icons/md";

function Header({ onToggleSidebar }) {
  return (
    <div className="h-14 bg-white border-b border-[#E2E2E2] flex items-center justify-between px-4">
      {/* Left - Logo & Menu */}
      <div className="flex items-center gap-4">
        {/* Logo */}
        <div className="w-8 h-8 rounded-full bg-black flex items-center justify-center text-white font-bold text-lg">
          ⚡
        </div>

        {/* Sidebar Toggle */}
        <button 
          onClick={onToggleSidebar}
          className="p-2 hover:bg-gray-100 rounded-lg transition"
        >
          <MdMenu className="w-5 h-5 text-black" />
        </button>
      </div>

      {/* Center - ChatGPT Title */}
      <div className="flex items-center gap-2 cursor-pointer hover:bg-gray-100 px-3 py-2 rounded-lg transition">
        <span className="text-base font-semibold">ChatGPT</span>
        <MdExpandMore className="w-5 h-5" />
      </div>

      {/* Right - Buttons */}
      <div className="flex items-center gap-2">
        {/* Get Plus Button */}
        <button className="flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-gray-100 transition text-sm font-medium text-blue-600">
          <span>✨</span>
          <span>Get Plus</span>
          <span className="text-gray-400 hover:text-black cursor-pointer">✕</span>
        </button>

        {/* Share Button */}
        <button className="flex items-center gap-2 px-3 py-2 rounded-lg hover:bg-gray-100 transition text-gray-600">
          <MdShare className="w-5 h-5" />
          <span className="text-sm">Share</span>
        </button>

        {/* More Menu Button */}
        <button className="p-2 hover:bg-gray-100 rounded-lg transition text-gray-600">
          <MdMoreVert className="w-5 h-5" />
        </button>
      </div>
    </div>
  );
}

export default Header;
