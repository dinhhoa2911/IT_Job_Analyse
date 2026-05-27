import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { FiMail, FiAlertCircle, FiCheckCircle, FiArrowLeft } from 'react-icons/fi';
import { useAuth } from '../../contexts/AuthContext';

export default function ForgotPasswordForm() {
  const { forgotPassword } = useAuth();
  const [email, setEmail]   = useState('');
  const [error, setError]   = useState('');
  const [success, setSuccess] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!email) { setError('Vui lòng nhập email.'); return; }
    setError(''); setLoading(true);
    try {
      await forgotPassword(email);
      setSuccess(true);
    } catch (err) {
      const msg = {
        'auth/user-not-found': 'Email này chưa được đăng ký.',
        'auth/invalid-email':  'Email không hợp lệ.',
      }[err.code] || 'Gửi email thất bại. Vui lòng thử lại.';
      setError(msg);
    } finally { setLoading(false); }
  };

  if (success) {
    return (
      <div className="w-full max-w-md rounded-2xl bg-white bg-opacity-95 p-8 shadow-2xl border border-gray-100 text-center space-y-4">
        <div className="w-16 h-16 rounded-full bg-emerald-100 flex items-center justify-center mx-auto">
          <FiCheckCircle className="w-8 h-8 text-emerald-600" />
        </div>
        <h2 className="text-2xl font-bold text-gray-900">Kiểm tra email!</h2>
        <p className="text-sm text-gray-500 leading-relaxed">
          Chúng tôi đã gửi link đặt lại mật khẩu đến <span className="font-semibold text-gray-700">{email}</span>.
          Vui lòng kiểm tra hộp thư (kể cả mục Spam).
        </p>
        <Link
          to="/login"
          className="inline-flex items-center gap-2 text-sm font-semibold text-blue-600 hover:text-blue-700 hover:underline"
        >
          <FiArrowLeft /> Quay lại đăng nhập
        </Link>
      </div>
    );
  }

  return (
    <div className="w-full max-w-md space-y-5 rounded-2xl bg-white bg-opacity-95 p-8 shadow-2xl backdrop-blur-sm border border-gray-100">
      {/* Header */}
      <div className="text-center space-y-1">
        <div className="w-14 h-14 rounded-2xl bg-blue-50 flex items-center justify-center mx-auto mb-3">
          <FiMail className="w-7 h-7 text-blue-600" />
        </div>
        <h2 className="text-3xl font-bold text-gray-900">Quên mật khẩu</h2>
        <p className="text-sm text-gray-500">
          Nhập email của bạn để khôi phục mật khẩu
        </p>
      </div>

      {/* Error */}
      {error && (
        <div className="flex items-center gap-2 px-4 py-3 rounded-lg bg-red-50 border border-red-200 text-red-700 text-sm">
          <FiAlertCircle className="flex-shrink-0" />
          {error}
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="relative">
          <input
            type="email"
            placeholder="name@example.com"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition"
          />
          <FiMail className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-4 h-4" />
        </div>

        <button
          type="submit"
          disabled={loading}
          className="w-full py-2.5 px-4 bg-gray-900 hover:bg-gray-800 text-white font-semibold rounded-lg transition disabled:opacity-60 text-sm"
        >
          {loading ? (
            <span className="flex items-center justify-center gap-2">
              <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
              Đang gửi...
            </span>
          ) : 'Gửi'}
        </button>
      </form>

      <div className="flex items-center justify-center">
        <Link to="/login" className="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-700 font-medium">
          <FiArrowLeft className="w-4 h-4" /> Quay lại đăng nhập
        </Link>
      </div>
    </div>
  );
}
