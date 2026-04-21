import { useCallback, useEffect, useRef, useState } from "react";
import { BsLayoutSidebar } from "react-icons/bs";
import { CiSearch } from "react-icons/ci";
import { FiEdit } from "react-icons/fi";
import { IoMdImages } from "react-icons/io";
import "./App.css";
import ChatArea from "./components/ChatArea";
import LoginModal from "./components/LoginModal";
import MessageInput from "./components/MessageInput";
import SettingsModal from "./components/SettingsModal";
import Sidebar from "./components/Sidebar";

const API_URL = process.env.REACT_APP_API_URL || "http://localhost:8100";
const STORAGE_KEY = "it_job_conversations";

function generateConvId() {
  return `conv_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
}

function makeNewConversation() {
  return { id: generateConvId(), title: "New Conversation", messages: [], active: true, starred: false };
}

function loadConversations() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length > 0) return parsed;
    }
  } catch {}
  return [makeNewConversation()];
}

function saveConversations(conversations) {
  try {
    // Strip large job/chart data before persisting to keep localStorage lean
    const slim = conversations.map((c) => ({
      ...c,
      messages: c.messages.map((m) => ({
        id: m.id,
        text: m.text,
        sender: m.sender,
        timestamp: m.timestamp,
        queryType: m.queryType,
        isError: m.isError,
        // jobs and chart are intentionally omitted — too large for localStorage
      })),
    }));
    localStorage.setItem(STORAGE_KEY, JSON.stringify(slim));
  } catch {}
}

function App() {
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [openSettings, setOpenSettings] = useState(false);
  const [loginOpen, setLoginOpen] = useState(false);
  const [theme, setTheme] = useState(localStorage.getItem("theme") || "system");
  const [isLoading, setIsLoading] = useState(false);
  const [conversations, setConversations] = useState(loadConversations);

  // Persist conversations to localStorage on every change
  useEffect(() => {
    saveConversations(conversations);
  }, [conversations]);

  useEffect(() => {
    localStorage.setItem("theme", theme);
    if (theme === "dark") {
      document.documentElement.classList.add("dark");
    } else if (theme === "light") {
      document.documentElement.classList.remove("dark");
    } else {
      const isDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
      document.documentElement.classList.toggle("dark", isDark);
    }
  }, [theme]);

  const activeConv = conversations.find((c) => c.active) || conversations[0];
  const messages = activeConv?.messages || [];

  // Ref so handleSendMessage always sees the latest activeConv without re-subscribing
  const activeConvRef = useRef(activeConv);
  useEffect(() => {
    activeConvRef.current = activeConv;
  }, [activeConv]);

  const handleSendMessage = useCallback(
    async (text) => {
      if (isLoading) return;

      const conv = activeConvRef.current;

      const userMessage = {
        id: Date.now(),
        text,
        sender: "user",
        timestamp: new Date().toISOString(),
      };

      // Auto-title the conversation from the first user message
      const isFirstMessage = conv.messages.length === 0;
      const newTitle = isFirstMessage
        ? text.slice(0, 40) + (text.length > 40 ? "…" : "")
        : null;

      setConversations((prev) =>
        prev.map((c) => {
          if (!c.active) return c;
          return {
            ...c,
            title: newTitle || c.title,
            messages: [...c.messages, userMessage],
          };
        })
      );
      setIsLoading(true);

      try {
        const res = await fetch(`${API_URL}/chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: text,
            session_id: conv.id,
            conversation_id: conv.id,
          }),
        });

        if (!res.ok) throw new Error(`Server error: ${res.status}`);

        const data = await res.json();

        const botMessage = {
          id: Date.now() + 1,
          text: data.answer,
          sender: "bot",
          timestamp: new Date().toISOString(),
          queryType: data.query_type,
          jobs: data.jobs || null,
          sqlQuery: data.sql_query || null,
          chart: data.chart || null,
        };

        setConversations((prev) =>
          prev.map((c) =>
            c.active ? { ...c, messages: [...c.messages, botMessage] } : c
          )
        );
      } catch (err) {
        const errMessage = {
          id: Date.now() + 1,
          text: `Xin lỗi, đã có lỗi xảy ra: ${err.message}. Vui lòng thử lại.`,
          sender: "bot",
          timestamp: new Date().toISOString(),
          isError: true,
        };
        setConversations((prev) =>
          prev.map((c) =>
            c.active ? { ...c, messages: [...c.messages, errMessage] } : c
          )
        );
      } finally {
        setIsLoading(false);
      }
    },
    [isLoading]
  );

  const handleNewConversation = useCallback(() => {
    setConversations((prev) => {
      // Don't create a new one if the current active conversation is already empty
      const current = prev.find((c) => c.active);
      if (current && current.messages.length === 0) return prev;

      return [
        makeNewConversation(),
        ...prev.map((c) => ({ ...c, active: false })),
      ];
    });
  }, []);

  const handleSelectConversation = useCallback((id) => {
    setConversations((prev) =>
      prev.map((c) => ({ ...c, active: c.id === id }))
    );
  }, []);

  const handleStarConversation = useCallback((id) => {
    setConversations((prev) =>
      prev.map((c) => (c.id === id ? { ...c, starred: !c.starred } : c))
    );
  }, []);

  const handleRenameConversation = useCallback((id, newTitle) => {
    const title = newTitle.trim();
    if (!title) return;
    setConversations((prev) =>
      prev.map((c) => (c.id === id ? { ...c, title } : c))
    );
  }, []);

  const handleDeleteConversation = useCallback((id) => {
    setConversations((prev) => {
      const remaining = prev.filter((c) => c.id !== id);
      if (remaining.length === 0) return [makeNewConversation()];
      // If we deleted the active one, activate the first remaining
      const hasActive = remaining.some((c) => c.active);
      if (!hasActive) return remaining.map((c, i) => ({ ...c, active: i === 0 }));
      return remaining;
    });
  }, []);

  return (
    <div className="flex h-screen bg-white dark:bg-[#212121] text-black dark:text-white">
      {/* Sidebar */}
      {sidebarOpen && (
        <div className="w-64 flex-shrink-0">
          <Sidebar
            conversations={conversations}
            onNewConversation={handleNewConversation}
            onSelectConversation={handleSelectConversation}
            onStarConversation={handleStarConversation}
            onRenameConversation={handleRenameConversation}
            onDeleteConversation={handleDeleteConversation}
            onCloseSidebar={() => setSidebarOpen(false)}
            onOpenSettings={() => setOpenSettings(true)}
          />
        </div>
      )}

      {/* Mini sidebar khi đóng */}
      {!sidebarOpen && (
        <div className="w-16 flex-shrink-0 border-r border-gray-200 dark:border-gray-700 flex flex-col items-center pt-4 space-y-2">
          <button
            onClick={() => setSidebarOpen(true)}
            className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-[#2a2a2a]"
          >
            <BsLayoutSidebar className="w-5 h-5" />
          </button>
          <button
            onClick={handleNewConversation}
            className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-[#2a2a2a]"
          >
            <FiEdit className="w-5 h-5" />
          </button>
          <button className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-[#2a2a2a]">
            <CiSearch className="w-5 h-5" />
          </button>
          <button className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-[#2a2a2a]">
            <IoMdImages className="w-5 h-5" />
          </button>
        </div>
      )}

      {/* Main content */}
      <div className="flex flex-col flex-1 min-w-0">
        {/* Header */}
        <header className="flex justify-end items-center px-4 py-3 gap-3 border-b border-gray-100 dark:border-gray-800">
          <button
            type="button"
            onClick={() => setLoginOpen(true)}
            className="px-4 py-2 text-sm font-medium bg-black text-white rounded-full hover:bg-gray-800 dark:bg-white dark:text-black dark:hover:bg-gray-100 transition"
          >
            Login
          </button>
          <button
            type="button"
            onClick={() => setLoginOpen(true)}
            className="px-4 py-2 text-sm font-medium bg-[#F9F9F9] text-black rounded-full hover:bg-[#F3F3F3] dark:bg-[#2A2A2A] dark:text-white dark:hover:bg-[#333] transition"
          >
            Sign up for free
          </button>
        </header>

        {/* Chat area */}
        {messages.length === 0 ? (
          <div className="flex-1 flex flex-col items-center justify-center px-4">
            <div className="text-center mb-8">
              <h2 className="text-5xl font-bold mb-4">🤖</h2>
              <h1 className="text-5xl font-bold mb-6">How can I help?</h1>
              <p className="text-gray-400 text-lg max-w-2xl mx-auto">
                Tìm kiếm việc làm IT, phân tích thị trường tuyển dụng, hoặc xin tư vấn nghề nghiệp.
              </p>
            </div>
            <MessageInput onSendMessage={handleSendMessage} isLoading={isLoading} />
          </div>
        ) : (
          <>
            <ChatArea messages={messages} isLoading={isLoading} />
            <div className="bg-white dark:bg-[#212121] px-4 py-4 flex justify-center">
              <MessageInput onSendMessage={handleSendMessage} isLoading={isLoading} />
            </div>
          </>
        )}
      </div>

      {/* Modals */}
      {openSettings && (
        <SettingsModal
          onClose={() => setOpenSettings(false)}
          theme={theme}
          setTheme={setTheme}
        />
      )}
      <LoginModal isOpen={loginOpen} onClose={() => setLoginOpen(false)} />
    </div>
  );
}

export default App;
