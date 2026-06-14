/**
 * @fileoverview Root application component for the IT Job Analyse chatbot.
 * Manages global state: conversations, theme, and modal visibility.
 * @module App
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { BrowserRouter, Navigate, Route, Routes, useNavigate } from "react-router-dom";
import { BsLayoutSidebar } from "react-icons/bs";
import { CiSearch } from "react-icons/ci";
import { FiBriefcase, FiEdit } from "react-icons/fi";
import { IoMdImages } from "react-icons/io";
import "./App.css";
import ChatArea from "./components/ChatArea";
import CVUploadModal from "./components/CVUploadModal";
import MessageInput from "./components/MessageInput";
import NewChatConfirmModal from "./components/NewChatConfirmModal";
import SettingsModal from "./components/SettingsModal";
import Sidebar from "./components/Sidebar";
import ForgotPasswordPage from "./pages/ForgotPasswordPage";
import LoginPage        from "./pages/LoginPage";
import RegisterPage     from "./pages/RegisterPage";
import VerifyOtpPage    from "./pages/VerifyOtpPage";
import { AuthProvider, useAuth } from "./contexts/AuthContext";

/** Base URL for the chatbot API. Falls back to localhost in development. */
const API_URL = process.env.REACT_APP_API_URL || "http://localhost:8100";

/**
 * Returns the localStorage key for a user's conversation history.
 * @param {string|null} uid - Firebase user UID, or null when not logged in.
 * @returns {string|null} Key to use, or null (no persistence when logged out).
 */
function storageKey(uid) {
  return uid ? `it_job_conversations_${uid}` : null;
}

/**
 * @typedef {Object} Message
 * @property {number}  id          - Unique numeric message ID (Date.now()).
 * @property {string}  text        - Message body (markdown for bot).
 * @property {'user'|'bot'} sender - Who sent the message.
 * @property {string}  timestamp  - ISO 8601 timestamp.
 * @property {string}  [queryType] - RAG query type returned by the API.
 * @property {Array}   [jobs]      - Matched job listings (bot only).
 * @property {string}  [sqlQuery]  - Generated SQL query (bot only).
 * @property {Object}  [chart]     - Chart.js spec (bot only).
 * @property {Object}  [marketInsight] - Gold-layer market insight data (bot only).
 * @property {boolean} [isError]   - True when the message represents an error.
 */

/**
 * @typedef {Object} Conversation
 * @property {string}    id       - Unique conversation ID (`conv_<timestamp>_<random>`).
 * @property {string}    title    - Display title (auto-generated from first message).
 * @property {Message[]} messages - Ordered list of messages.
 * @property {boolean}   active   - Whether this is the currently selected conversation.
 * @property {boolean}   starred  - Whether the user has starred this conversation.
 */

/**
 * Generates a unique conversation ID.
 * @returns {string} A string of the form `conv_<timestamp>_<randomSuffix>`.
 */
function generateConvId() {
  return `conv_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
}

/**
 * Creates a blank conversation object with a generated ID.
 * @returns {Conversation}
 */
function makeNewConversation() {
  return { id: generateConvId(), title: "New Conversation", messages: [], active: true, starred: false };
}

/**
 * Loads persisted conversations from localStorage.
 * Falls back to a single blank conversation when storage is empty or corrupt.
 * @returns {Conversation[]}
 */
/**
 * Load conversations for a user from localStorage.
 * @param {string|null} key - localStorage key, or null → return a fresh conversation.
 */
function loadConversations(key) {
  if (!key) return [makeNewConversation()];
  try {
    const raw = localStorage.getItem(key);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length > 0) return parsed;
    }
  } catch {}
  return [makeNewConversation()];
}

/**
 * Persist conversations for a logged-in user.
 * No-ops when key is null (unauthenticated users are not persisted).
 * @param {Conversation[]} conversations
 * @param {string|null}    key
 */
function saveConversations(conversations, key) {
  if (!key) return;
  try {
    const slim = conversations.map((c) => ({
      ...c,
      messages: c.messages.map((m) => ({
        id: m.id, text: m.text, sender: m.sender,
        timestamp: m.timestamp, queryType: m.queryType, isError: m.isError,
      })),
    }));
    localStorage.setItem(key, JSON.stringify(slim));
  } catch {}
}

/**
 * @component
 * @brief Root application component.
 *
 * Owns all top-level state and orchestrates communication between the sidebar,
 * chat area, message input, and modal dialogs.
 *
 * @returns {JSX.Element}
 */
/**
 * @component AppInner
 * @brief Top-level router. Auth pages are always accessible; chat is accessible
 * to everyone (logged-in users get persistence, guests get a temporary session).
 */
function AppInner() {
  const { user, loading, pendingVerification } = useAuth();

  if (loading) return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100 dark:bg-[#212121]">
      <div className="w-10 h-10 border-4 border-blue-600 border-t-transparent rounded-full animate-spin" />
    </div>
  );

  return (
    <Routes>
      {/* Auth pages — redirect to / if already logged in (and OTP done) */}
      <Route path="/login"           element={(user && !pendingVerification) ? <Navigate to="/" replace /> : <LoginPage />} />
      <Route path="/register"        element={(user && !pendingVerification) ? <Navigate to="/" replace /> : <RegisterPage />} />
      <Route path="/forgot-password" element={<ForgotPasswordPage />} />

      {/* OTP page — if logged-in user still has pending OTP, force them here */}
      <Route
        path="/verify-otp"
        element={
          (user && pendingVerification)
            ? <VerifyOtpPage />
            : (!user ? <VerifyOtpPage /> : <Navigate to="/" replace />)
        }
      />

      {/* Main chat — if logged in but OTP pending, redirect to OTP page */}
      <Route
        path="/*"
        element={
          (user && pendingVerification)
            ? <Navigate to={`/verify-otp?email=${encodeURIComponent(pendingVerification)}&type=register`} replace />
            : <App />
        }
      />
    </Routes>
  );
}

function App() {
  const { user }   = useAuth();
  const navigate   = useNavigate();
  const key        = storageKey(user?.uid ?? null); // null when logged out → no persistence

  const [sidebarOpen, setSidebarOpen]     = useState(true);
  const [sidebarClosing, setSidebarClosing] = useState(false);
  const [openSettings, setOpenSettings]   = useState(false);
  const [cvModalOpen, setCvModalOpen]   = useState(false);
  const [theme, setTheme]               = useState(localStorage.getItem("theme") || "system");
  const [isLoading, setIsLoading]       = useState(false);
  const [isSpeaking, setIsSpeaking]     = useState(false);
  const [showNewChatModal, setShowNewChatModal] = useState(false);
  // Pending new conversation — exists in UI but NOT yet in sidebar/localStorage.
  // Committed to conversations[] only when the user sends the first message.
  const [pendingConv, setPendingConv] = useState(null);
  const pendingConvRef = useRef(null);
  useEffect(() => { pendingConvRef.current = pendingConv; }, [pendingConv]);

  // Load the right conversation history whenever auth state changes
  const [conversations, setConversations] = useState(() => loadConversations(key));
  useEffect(() => {
    setConversations(loadConversations(storageKey(user?.uid ?? null)));
  }, [user?.uid]); // eslint-disable-line react-hooks/exhaustive-deps

  const wasVoiceInputRef    = useRef(false);
  const abortControllerRef  = useRef(null);
  const requestSeqRef       = useRef(0);

  const handleCloseSidebar = useCallback(() => {
    setSidebarClosing(true);
    setTimeout(() => { setSidebarOpen(false); setSidebarClosing(false); }, 240);
  }, []);

  const handleOpenCVModal = useCallback(() => {
    if (!user) {
      navigate('/login');
      return;
    }
    setCvModalOpen(true);
  }, [user, navigate]);

  // Preload TTS voices early (Chrome lazy-loads them)
  useEffect(() => {
    if (window.speechSynthesis) window.speechSynthesis.getVoices();
  }, []);

  /**
   * @function speakText
   * @brief Speak bot reply text via Web Speech synthesis.
   * Only fires when wasVoiceInputRef is true (i.e. last message was via mic).
   */
  const speakText = useCallback((text) => {
    if (!wasVoiceInputRef.current || !window.speechSynthesis) return;
    wasVoiceInputRef.current = false; // reset after one use
    const clean = text
      .replace(/[#*_`[\]()>]/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .slice(0, 600);
    window.speechSynthesis.cancel();
    const utter = new SpeechSynthesisUtterance(clean);
    utter.lang  = "vi-VN";
    utter.rate  = 1.05;
    const voices = window.speechSynthesis.getVoices();
    const viVoice = voices.find((v) => v.lang.startsWith("vi"));
    if (viVoice) utter.voice = viVoice;
    utter.onstart = () => setIsSpeaking(true);
    utter.onend   = () => setIsSpeaking(false);
    utter.onerror = () => setIsSpeaking(false);
    window.speechSynthesis.speak(utter);
  }, []);

  // Called by MessageInput when user sends a message via microphone.
  // Marks the next bot reply to be auto-spoken.
  const handleVoiceSubmit = useCallback(() => {
    wasVoiceInputRef.current = true;
  }, []);

  // Persist only when logged in (key is null for guests → no-op)
  useEffect(() => {
    saveConversations(conversations, key);
  }, [conversations, key]);

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

  // pendingConv takes priority — it's the "empty new chat" before first message
  const activeConv = pendingConv || conversations.find((c) => c.active) || conversations[0];
  const messages = activeConv?.messages || [];

  // Ref so handleSendMessage always sees the latest activeConv without re-subscribing
  const activeConvRef = useRef(activeConv);
  useEffect(() => {
    activeConvRef.current = activeConv;
  }, [activeConv]);

  /**
   * Sends a user message to the API and appends the bot reply to the conversation.
   * Auto-titles the conversation from the first message.
   * @function
   * @param {string} text - The user-typed message text.
   * @returns {Promise<void>}
   */
  const handleStop = useCallback(() => {
    requestSeqRef.current += 1;
    abortControllerRef.current?.abort();
    abortControllerRef.current = null;
    window.speechSynthesis?.cancel();
    setIsSpeaking(false);
    setIsLoading(false);
  }, []);

  const handleSendMessage = useCallback(
    async (text) => {
      if (isLoading) return;
      const requestId = ++requestSeqRef.current;

      const conv = activeConvRef.current;

      const userMessage = {
        id: Date.now(),
        text,
        sender: "user",
        timestamp: new Date().toISOString(),
      };

      const isFirstMessage = conv.messages.length === 0;
      const newTitle = isFirstMessage
        ? text.slice(0, 40) + (text.length > 40 ? "…" : "")
        : null;

      if (pendingConvRef.current) {
        // First message on a pending conv → commit it to sidebar now
        const committed = { ...conv, active: true, title: newTitle || conv.title, messages: [userMessage] };
        setPendingConv(null);
        setConversations((prev) => [committed, ...prev.map((c) => ({ ...c, active: false }))]);
      } else {
        setConversations((prev) =>
          prev.map((c) => {
            if (!c.active) return c;
            return { ...c, title: newTitle || c.title, messages: [...c.messages, userMessage] };
          })
        );
      }
      setIsLoading(true);

      try {
        const controller = new AbortController();
        abortControllerRef.current = controller;

        const res = await fetch(`${API_URL}/chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: text,
            session_id: conv.id,
            conversation_id: conv.id,
          }),
          signal: controller.signal,
        });

        if (!res.ok) throw new Error(`Server error: ${res.status}`);

        const data = await res.json();
        if (controller.signal.aborted || requestId !== requestSeqRef.current) return;

        const botMessage = {
          id: Date.now() + 1,
          text: data.answer,
          sender: "bot",
          timestamp: new Date().toISOString(),
          queryType: data.query_type,
          jobs: data.jobs || null,
          sqlQuery: data.sql_query || null,
          // charts: multi-chart (agent mode); chart: single-chart (classic)
          charts: data.charts?.length > 0 ? data.charts : (data.chart ? [data.chart] : []),
          chart: data.chart || null,
          marketInsight: data.market_insight || null,
          learningPath: data.learning_path || null,
        };

        setConversations((prev) =>
          prev.map((c) =>
            c.active ? { ...c, messages: [...c.messages, botMessage] } : c
          )
        );
        speakText(data.answer);
        if (requestId === requestSeqRef.current) {
          abortControllerRef.current = null;
          setIsLoading(false);
        }
      } catch (err) {
        // User clicked stop — abort silently, loading already reset by handleStop
        if (err?.name === "AbortError") return;
        if (requestId !== requestSeqRef.current) return;

        const raw = err?.message ?? "";
        let friendly;
        if (raw.includes("Failed to fetch") || raw.includes("NetworkError") || raw.includes("network")) {
          friendly = "Không thể kết nối đến máy chủ. Vui lòng kiểm tra kết nối mạng và thử lại.";
        } else if (raw.includes("504") || raw.includes("timeout")) {
          friendly = "Máy chủ phản hồi quá lâu. Câu hỏi của bạn có thể phức tạp — hãy thử lại sau vài giây.";
        } else if (raw.includes("503") || raw.includes("502")) {
          friendly = "Hệ thống đang bảo trì hoặc khởi động lại. Vui lòng thử lại sau ít phút.";
        } else if (raw.includes("500")) {
          friendly = "Đã có lỗi phía máy chủ khi xử lý câu hỏi của bạn. Vui lòng thử lại.";
        } else if (raw.includes("404")) {
          friendly = "Không tìm thấy dịch vụ. Vui lòng thử lại sau.";
        } else if (raw.includes("SyntaxError") || raw.includes("JSON")) {
          friendly = "Máy chủ trả về dữ liệu không hợp lệ. Vui lòng thử lại.";
        } else {
          friendly = "Có sự cố xảy ra. Vui lòng thử lại sau giây lát.";
        }
        const errMessage = {
          id: Date.now() + 1,
          text: friendly,
          sender: "bot",
          timestamp: new Date().toISOString(),
          isError: true,
        };
        setConversations((prev) =>
          prev.map((c) =>
            c.active ? { ...c, messages: [...c.messages, errMessage] } : c
          )
        );
        if (requestId === requestSeqRef.current) {
          abortControllerRef.current = null;
          setIsLoading(false);
        }
      }
    },
    [isLoading]
  );

  /**
   * Creates a new blank conversation and marks it as active.
   * No-ops when the current conversation is already empty.
   * @function
   * @returns {void}
   */
  /** Actually creates a new blank conversation (shared by the direct path and the modal confirm). */
  const createNewConversation = useCallback(() => {
    // Already on a blank pending chat — no-op
    if (pendingConvRef.current) return;
    const current = conversations.find((c) => c.active);
    // Already on an empty saved conversation — no-op
    if (current && current.messages.length === 0) return;
    // Deactivate all, set pending — sidebar unchanged until first message
    setConversations((prev) => prev.map((c) => ({ ...c, active: false })));
    setPendingConv(makeNewConversation());
  }, [conversations]);

  const handleNewConversation = useCallback(() => {
    const current = conversations.find((c) => c.active);
    const hasMessages = current && current.messages.length > 0;

    // Guest with unsaved messages → show confirmation popup
    if (!user && hasMessages) {
      setShowNewChatModal(true);
      return;
    }
    createNewConversation();
  }, [user, conversations, createNewConversation]);

  /**
   * Switches the active conversation.
   * @function
   * @param {string} id - The ID of the conversation to activate.
   * @returns {void}
   */
  const handleSelectConversation = useCallback((id) => {
    setPendingConv(null);
    setConversations((prev) =>
      prev.map((c) => ({ ...c, active: c.id === id }))
    );
  }, []);

  /**
   * Toggles the starred flag on a conversation.
   * @function
   * @param {string} id - The conversation ID to star/unstar.
   * @returns {void}
   */
  const handleStarConversation = useCallback((id) => {
    setConversations((prev) =>
      prev.map((c) => (c.id === id ? { ...c, starred: !c.starred } : c))
    );
  }, []);

  /**
   * Renames a conversation. Silently ignores empty titles.
   * @function
   * @param {string} id       - The conversation ID to rename.
   * @param {string} newTitle - The new title string.
   * @returns {void}
   */
  const handleRenameConversation = useCallback((id, newTitle) => {
    const title = newTitle.trim();
    if (!title) return;
    setConversations((prev) =>
      prev.map((c) => (c.id === id ? { ...c, title } : c))
    );
  }, []);

  /**
   * Deletes a conversation. Ensures at least one conversation always exists
   * and that an active conversation is selected after deletion.
   * @function
   * @param {string} id - The conversation ID to delete.
   * @returns {void}
   */
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
      {(sidebarOpen || sidebarClosing) && (
        <div className="w-64 flex-shrink-0">
          <Sidebar
            conversations={conversations}
            onNewConversation={handleNewConversation}
            onSelectConversation={handleSelectConversation}
            onStarConversation={handleStarConversation}
            onRenameConversation={handleRenameConversation}
            onDeleteConversation={handleDeleteConversation}
            onCloseSidebar={handleCloseSidebar}
            onOpenSettings={() => setOpenSettings(true)}
            isClosing={sidebarClosing}
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
          <button
            onClick={handleOpenCVModal}
            className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-[#2a2a2a]"
            title="Match CV với việc làm"
          >
            <FiBriefcase className="w-5 h-5" />
          </button>
        </div>
      )}

      {/* Main content */}
      <div className="flex flex-col flex-1 min-w-0">
        {/* Chat area */}
        {messages.length === 0 ? (
          <div className="flex-1 flex flex-col items-center justify-center px-4">
            <div className="flex flex-col items-center w-full max-w-2xl">
              {/* Logo */}
              <div className="mb-6">
                <span className="text-6xl">🤖</span>
              </div>

              {/* Heading */}
              <div className="text-center mb-12">
                <h1 className="text-4xl font-bold mb-3">How can I help?</h1>
                <p className="text-gray-400 text-base leading-relaxed">
                  Tìm kiếm việc làm IT, phân tích thị trường tuyển dụng, hoặc xin tư vấn nghề nghiệp.
                </p>
              </div>

              {/* Input */}
              <div className="w-full">
                <MessageInput
                  onSendMessage={handleSendMessage}
                  onOpenCVModal={handleOpenCVModal}
                  onVoiceSubmit={handleVoiceSubmit}
                  onStop={handleStop}
                  isLoading={isLoading}
                  isSpeaking={isSpeaking}
                />
              </div>
            </div>
          </div>
        ) : (
          <>
            <ChatArea messages={messages} isLoading={isLoading} />
            <div className="bg-white dark:bg-[#212121] px-4 py-4 flex justify-center">
              <MessageInput
                onSendMessage={handleSendMessage}
                onOpenCVModal={handleOpenCVModal}
                onVoiceSubmit={handleVoiceSubmit}
                onStop={handleStop}
                isLoading={isLoading}
                isSpeaking={isSpeaking}
              />
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
      {cvModalOpen && <CVUploadModal onClose={() => setCvModalOpen(false)} />}

      {/* New-chat confirmation — shown to unauthenticated users with unsaved messages */}
      {showNewChatModal && (
        <NewChatConfirmModal
          onClear={() => { setShowNewChatModal(false); createNewConversation(); }}
          onLogin={() => { setShowNewChatModal(false); navigate("/login"); }}
          onClose={() => setShowNewChatModal(false)}
        />
      )}
    </div>
  );
}

export default function Root() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <AppInner />
      </AuthProvider>
    </BrowserRouter>
  );
}
