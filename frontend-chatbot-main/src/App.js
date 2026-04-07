import { useCallback, useRef, useState } from "react";
import { BsLayoutSidebar } from "react-icons/bs";
import { CiSearch } from "react-icons/ci";
import { FiEdit } from "react-icons/fi";
import { IoMdImages } from "react-icons/io";
import "./App.css";
import ChatArea from "./components/ChatArea";
import MessageInput from "./components/MessageInput";
import Sidebar from "./components/Sidebar";

const API_URL = process.env.REACT_APP_API_URL || "http://localhost:8100";

function generateSessionId() {
  return `session_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
}

function App() {
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [messages, setMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [conversations, setConversations] = useState([
    { id: 1, title: "New Conversation", active: true },
  ]);

  // Stable session ID per browser session
  const sessionIdRef = useRef(generateSessionId());

  const handleSendMessage = useCallback(
    async (text) => {
      if (isLoading) return;

      // Append user message immediately
      const userMessage = {
        id: Date.now(),
        text,
        sender: "user",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, userMessage]);
      setIsLoading(true);

      try {
        const res = await fetch(`${API_URL}/chat`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            message: text,
            session_id: sessionIdRef.current,
          }),
        });

        if (!res.ok) {
          throw new Error(`Server error: ${res.status}`);
        }

        const data = await res.json();

        const botMessage = {
          id: Date.now() + 1,
          text: data.answer,
          sender: "bot",
          timestamp: new Date(),
          queryType: data.query_type,
        };
        setMessages((prev) => [...prev, botMessage]);
      } catch (err) {
        const errorMessage = {
          id: Date.now() + 1,
          text: `Xin lỗi, đã có lỗi xảy ra: ${err.message}. Vui lòng thử lại.`,
          sender: "bot",
          timestamp: new Date(),
          isError: true,
        };
        setMessages((prev) => [...prev, errorMessage]);
      } finally {
        setIsLoading(false);
      }
    },
    [isLoading]
  );

  const handleNewConversation = useCallback(() => {
    sessionIdRef.current = generateSessionId();
    setMessages([]);
    setConversations((prev) => {
      const updated = prev.map((c) => ({ ...c, active: false }));
      return [
        ...updated,
        {
          id: Date.now(),
          title: `Conversation ${updated.length + 1}`,
          active: true,
        },
      ];
    });
  }, []);

  return (
    <div className="flex h-screen bg-[#FFFFFF] text-black">
      {sidebarOpen && (
        <>
          <div
            className="fixed inset-0 bg-black/30 z-30 md:hidden"
            onClick={() => setSidebarOpen(false)}
          />
          <div className="fixed inset-y-0 left-0 z-40 w-64 md:relative md:inset-auto md:z-auto md:w-64">
            <Sidebar
              conversations={conversations}
              onNewConversation={handleNewConversation}
              onCloseSidebar={() => setSidebarOpen(false)}
            />
          </div>
        </>
      )}

      {!sidebarOpen && (
        <div className="fixed left-0 top-0 h-screen w-16 z-30 bg-white border-r border-gray-200 flex flex-col items-center pt-4 pb-6 space-y-2">
          <button
            onClick={() => setSidebarOpen(true)}
            className="p-2 rounded-lg hover:bg-[#E2E2E2]"
            aria-label="Open sidebar"
          >
            <BsLayoutSidebar className="w-5 h-5" />
          </button>
          <button
            onClick={handleNewConversation}
            className="p-2 rounded-lg hover:bg-[#E2E2E2]"
            aria-label="New chat"
          >
            <FiEdit className="w-5 h-5" />
          </button>
          <button className="p-2 rounded-lg hover:bg-[#E2E2E2]" aria-label="Search chats">
            <CiSearch className="w-5 h-5" />
          </button>
          <button className="p-2 rounded-lg hover:bg-[#E2E2E2]" aria-label="Images">
            <IoMdImages className="w-5 h-5" />
          </button>
        </div>
      )}

      <div className="flex flex-col flex-1 min-w-0">
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
            <div className="bg-white px-4 py-6 flex justify-center">
              <MessageInput onSendMessage={handleSendMessage} isLoading={isLoading} />
            </div>
          </>
        )}
      </div>
    </div>
  );
}

export default App;
