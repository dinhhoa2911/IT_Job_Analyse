import React from "react";
import { MdExpandMore, MdMenu, MdMoreVert, MdShare } from "react-icons/md";

function Header({ onToggleSidebar }) {
  return (
    <div className="h-14 
    bg-white dark:bg-[#212121] 
    border-b border-gray-200 dark:border-gray-800 
    flex items-center justify-between px-4">

      {/* Left */}
      <div className="flex items-center gap-4">
        <div className="w-8 h-8 rounded-full 
        bg-black text-white 
        dark:bg-[#2a2a2a] dark:text-gray-200 
        flex items-center justify-center font-bold">
          ⚡
        </div>

        <button
          onClick={onToggleSidebar}
          className="p-2 rounded-lg 
          hover:bg-gray-100 dark:hover:bg-[#2a2a2a] transition"
        >
          <MdMenu className="w-5 h-5 text-gray-700 dark:text-gray-200" />
        </button>
      </div>

      {/* Center */}
      <div className="flex items-center gap-2 cursor-pointer 
      hover:bg-gray-100 dark:hover:bg-[#2a2a2a] 
      px-3 py-2 rounded-lg transition">
        <span className="text-base font-semibold">ChatGPT</span>
        <MdExpandMore className="w-5 h-5 text-gray-600 dark:text-gray-300" />
      </div>

      {/* Right */}
      <div className="flex items-center gap-2">

        <button className="flex items-center gap-2 px-3 py-2 rounded-lg 
        hover:bg-gray-100 dark:hover:bg-[#2a2a2a] 
        text-sm font-medium text-blue-600">
          ✨ Get Plus
        </button>

        <button className="flex items-center gap-2 px-3 py-2 rounded-lg 
        hover:bg-gray-100 dark:hover:bg-[#2a2a2a] 
        text-gray-600 dark:text-gray-300">
          <MdShare className="w-5 h-5" />
          <span className="text-sm">Share</span>
        </button>

        <button className="p-2 rounded-lg 
        hover:bg-gray-100 dark:hover:bg-[#2a2a2a] 
        text-gray-600 dark:text-gray-300">
          <MdMoreVert className="w-5 h-5" />
        </button>
      </div>
    </div>
  );
}

export default Header;