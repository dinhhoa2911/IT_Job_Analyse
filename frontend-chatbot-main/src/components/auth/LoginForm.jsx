import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { FiMail, FiLock, FiEye, FiEyeOff, FiAlertCircle } from 'react-icons/fi';
import { useAuth } from '../../contexts/AuthContext';

function GoogleIcon() {
  return (
    <svg className="h-5 w-5" viewBox="0 0 24 24" fill="none">
      <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4"/>
      <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/>
      <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05"/>
      <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/>
    </svg>
  );
}

function FacebookIcon() {
  return (
    <svg className="h-5 w-5" viewBox="0 0 24 24" fill="#1877F2">
      <path d="M24 12.073C24 5.405 18.627 0 12 0S0 5.405 0 12.073C0 18.1 4.388 23.094 10.125 24v-8.437H7.078v-3.49h3.047V9.41c0-3.025 1.792-4.697 4.533-4.697 1.312 0 2.686.235 2.686.235v2.97h-1.513c-1.491 0-1.956.93-1.956 1.884v2.25h3.328l-.532 3.49h-2.796V24C19.612 23.094 24 18.1 24 12.073z"/>
    </svg>
  );
}

export default function LoginForm() {
  const { login, googleLogin, facebookLogin } = useAuth();
  const navigate = useNavigate();

  const [email, setEmail]         = useState('');
  const [password, setPassword]   = useState('');
  const [showPass, setShowPass]   = useState(false);
  const [error, setError]         = useState('');
  const [loading, setLoading]     = useState(false);
  const [googleLoad,   setGoogleLoad]   = useState(false);
  const [fbLoad,       setFbLoad]       = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!email || !password) { setError('Vui lòng nhập đầy đủ thông tin.'); return; }
    setError(''); setLoading(true);
    try {
      await login(email, password);
      navigate('/');
    } catch (err) {
      const msg = {
        'auth/user-not-found':   'Email không tồn tại.',
        'auth/wrong-password':   'Mật khẩu không đúng.',
        'auth/invalid-credential': 'Email hoặc mật khẩu không đúng.',
        'auth/too-many-requests': 'Quá nhiều lần thử. Vui lòng thử lại sau.',
      }[err.code] || err.message;
      setError(msg);
    } finally { setLoading(false); }
  };

  const handleGoogle = async () => {
    setError(''); setGoogleLoad(true);
    try {
      await googleLogin();
      navigate('/');
    } catch (err) {
      if (err.code !== 'auth/popup-closed-by-user') setError('Đăng nhập Google thất bại. Thử lại.');
    } finally { setGoogleLoad(false); }
  };

  const handleFacebook = async () => {
    setError(''); setFbLoad(true);
    try {
      await facebookLogin();
      navigate('/');
    } catch (err) {
      if (err.code !== 'auth/popup-closed-by-user') setError('Đăng nhập Facebook thất bại. Thử lại.');
    } finally { setFbLoad(false); }
  };

  return (
    <div className="w-full max-w-md space-y-4 rounded-2xl bg-white bg-opacity-95 p-8 shadow-2xl backdrop-blur-sm border border-gray-100">
      {/* Header */}
      <div className="text-center space-y-1">
        <h2 className="text-3xl font-bold text-gray-900">Đăng nhập</h2>
        <p className="text-sm text-gray-500">Đăng nhập để tìm kiếm việc làm IT và nhận tư vấn AI</p>
      </div>

      {/* Error */}
      {error && (
        <div className="flex items-center gap-2 px-4 py-3 rounded-lg bg-red-50 border border-red-200 text-red-700 text-sm">
          <FiAlertCircle className="flex-shrink-0" />
          {error}
        </div>
      )}

      {/* Form */}
      <form onSubmit={handleSubmit} className="space-y-4">
        {/* Email */}
        <div className="relative">
          <input
            type="email"
            placeholder="Email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition"
          />
          <FiMail className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-4 h-4" />
        </div>

        {/* Password */}
        <div className="relative">
          <input
            type={showPass ? 'text' : 'password'}
            placeholder="Mật khẩu"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="w-full pl-10 pr-10 py-2.5 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition"
          />
          <FiLock className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-4 h-4" />
          <button type="button" onClick={() => setShowPass(!showPass)} className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600">
            {showPass ? <FiEyeOff className="w-4 h-4" /> : <FiEye className="w-4 h-4" />}
          </button>
        </div>

        {/* Forgot */}
        <div className="flex justify-end">
          <Link to="/forgot-password" className="text-sm text-blue-600 hover:text-blue-700 hover:underline font-medium">
            Quên mật khẩu?
          </Link>
        </div>

        <button
          type="submit"
          disabled={loading}
          className="w-full py-2.5 px-4 bg-gray-900 hover:bg-gray-800 text-white font-semibold rounded-lg transition disabled:opacity-60 disabled:cursor-not-allowed text-sm"
        >
          {loading ? (
            <span className="flex items-center justify-center gap-2">
              <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
              Đang đăng nhập...
            </span>
          ) : 'Đăng nhập'}
        </button>
      </form>

      {/* Divider */}
      <div className="flex items-center gap-3">
        <div className="flex-1 h-px bg-gray-200" />
        <span className="text-xs text-gray-400">Hoặc đăng nhập với</span>
        <div className="flex-1 h-px bg-gray-200" />
      </div>

      {/* OAuth buttons */}
      <div className="flex flex-col gap-2.5">
        <button
          onClick={handleGoogle}
          disabled={googleLoad || fbLoad}
          className="w-full flex items-center justify-center gap-2 py-2.5 px-4 border border-gray-300 rounded-lg hover:bg-gray-50 transition font-medium text-sm text-gray-700 disabled:opacity-60"
        >
          <GoogleIcon />
          {googleLoad ? 'Đang xử lý...' : 'Đăng nhập với Google'}
        </button>

        <button
          onClick={handleFacebook}
          disabled={fbLoad || googleLoad}
          className="w-full flex items-center justify-center gap-2 py-2.5 px-4 border border-[#1877F2] rounded-lg bg-[#1877F2] hover:bg-[#166fe5] transition font-medium text-sm text-white disabled:opacity-60"
        >
          <FacebookIcon />
          {fbLoad ? 'Đang xử lý...' : 'Đăng nhập với Facebook'}
        </button>
      </div>

      {/* Register link */}
      <p className="text-center text-sm text-gray-500">
        Chưa có tài khoản?{' '}
        <Link to="/register" className="font-semibold text-blue-600 hover:text-blue-700 hover:underline">
          Đăng ký ngay
        </Link>
      </p>
    </div>
  );
}
