import React from "react";
import { CiSearch } from "react-icons/ci";
import { IoMdImages } from "react-icons/io";
import { FiEdit } from "react-icons/fi";
import { MdClose } from "react-icons/md";
import { BsLayoutSidebar } from "react-icons/bs";

function Sidebar({ conversations, onNewConversation, onCloseSidebar }) {
  return (
    <div className="w-64 bg-[#F9F9F9] border-r border-[#E2E2E2] flex flex-col h-screen">

      {/* Header */}
      <div className="p-3 border-b border-[#E2E2E2] flex items-center justify-between">

        <button className="p-2 hover:bg-[#E2E2E2] rounded-lg transition">
          🤖
        </button>

        <button
          onClick={onCloseSidebar}
          className="p-2 hover:bg-[#E2E2E2] rounded-lg transition text-gray-500 hover:text-black"
        >
          <BsLayoutSidebar className="w-5 h-5" />
        </button>

      </div>

      {/* Menu */}
      <div className="p-3 space-y-1">

        <button
          onClick={onNewConversation}
          className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] transition"
        >
          <FiEdit className="w-4 h-4" />
          <span className="text-sm">New chat</span>
        </button>

        <button className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] transition">
          <CiSearch className="w-4 h-4" />
          <span className="text-sm">Search chats</span>
        </button>

        <button className="flex items-center gap-3 w-full px-3 py-2 rounded-lg hover:bg-[#E2E2E2] transition">
          <IoMdImages className="w-4 h-4" />
          <span className="text-sm">Images</span>
        </button>

      </div>

      {/* Conversations */}
      <div className="flex-1 overflow-y-auto px-2 py-2">

        <p className="text-xs text-gray-500 px-2 mb-2">
          Your chats
        </p>

        <div className="space-y-1">
          {conversations.map((conv) => (
            <div
              key={conv.id}
              className={`px-3 py-2 text-sm rounded-lg cursor-pointer truncate ${
                conv.active
                  ? "bg-[#E2E2E2]"
                  : "hover:bg-[#E2E2E2]"
              }`}
            >
              {conv.title}
            </div>
          ))}
        </div>

      </div>

      {/* Footer */}
      <div className="border-t border-[#E2E2E2] p-3">

        <div className="flex items-center gap-3 p-2 rounded-lg hover:bg-[#E2E2E2] cursor-pointer transition">

          <div className="w-8 h-8 bg-[#E2E2E2] rounded-full flex items-center justify-center">
            👤
          </div>

          <div className="text-sm">
            <p className="font-medium">User</p>
            <p className="text-xs text-gray-500">Premium</p>
          </div>

        </div>

      </div>

    </div>
  );
}

export default Sidebar;