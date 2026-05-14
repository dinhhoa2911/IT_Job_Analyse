import React, { useState, useEffect, useRef } from 'react';
import { useNavigate, useSearchParams, useLocation } from 'react-router-dom';
import { FiAlertCircle, FiShield } from 'react-icons/fi';
import { useAuth } from '../../contexts/AuthContext';

export default function VerifyOtpForm() {
  const { verifyOtp, resendOtp } = useAuth();
  const navigate      = useNavigate();
  const [params]      = useSearchParams();
  const location      = useLocation();

  const email         = params.get('email') || '';
  const type          = params.get('type')  || 'register';

  const [digits, setDigits]       = useState(Array(6).fill(''));
  const [error, setError]         = useState('');
  const [loading, setLoading]     = useState(false);
  const [countdown, setCountdown] = useState(0);
  const [resending, setResending] = useState(false);
  const inputRefs = useRef([]);

  // Countdown timer for resend
  useEffect(() => {
    if (countdown <= 0) return;
    const t = setTimeout(() => setCountdown((c) => c - 1), 1000);
    return () => clearTimeout(t);
  }, [countdown]);

  const handleChange = (i, val) => {
    if (!/^\d?$/.test(val)) return;
    const next = [...digits];
    next[i] = val;
    setDigits(next);
    if (val && i < 5) inputRefs.current[i + 1]?.focus();
  };

  const handleKeyDown = (i, e) => {
    if (e.key === 'Backspace' && !digits[i] && i > 0) {
      inputRefs.current[i - 1]?.focus();
    }
  };

  const handlePaste = (e) => {
    const pasted = e.clipboardData.getData('text').replace(/\D/g, '').slice(0, 6);
    if (pasted.length === 6) {
      setDigits(pasted.split(''));
      inputRefs.current[5]?.focus();
    }
  };

  const handleSubmit = async () => {
    const code = digits.join('');
    if (code.length !== 6) { setError('Vui lòng nhập đủ 6 chữ số.'); return; }
    setError(''); setLoading(true);
    try {
      await verifyOtp(email, code);
      navigate('/');
    } catch (err) {
      setError(err.message);
      setDigits(Array(6).fill(''));
      inputRefs.current[0]?.focus();
    } finally { setLoading(false); }
  };

  const handleResend = async () => {
    setResending(true); setError('');
    try {
      await resendOtp(email, type);
      setCountdown(30);
    } catch (err) { setError('Gửi lại mã thất bại. Vui lòng thử lại.'); }
    finally { setResending(false); }
  };

  return (
    <div className="w-full max-w-md space-y-5 rounded-2xl bg-white bg-opacity-95 p-8 shadow-2xl border border-gray-100">
      {/* Header */}
      <div className="text-center space-y-1">
        <div className="w-14 h-14 rounded-2xl bg-indigo-50 flex items-center justify-center mx-auto mb-3">
          <FiShield className="w-7 h-7 text-indigo-600" />
        </div>
        <h2 className="text-3xl font-bold text-gray-900">Nhập mã OTP</h2>
        <p className="text-sm text-gray-500 leading-relaxed">
          Mã OTP đã được gửi đến<br />
          <span className="font-semibold text-gray-700">{email}</span>
        </p>
      </div>


      {/* Error */}
      {error && (
        <div className="flex items-center gap-2 px-4 py-3 rounded-lg bg-red-50 border border-red-200 text-red-700 text-sm">
          <FiAlertCircle className="flex-shrink-0" />
          {error}
        </div>
      )}

      {/* OTP boxes */}
      <div className="flex justify-center gap-2" onPaste={handlePaste}>
        {digits.map((d, i) => (
          <input
            key={i}
            ref={(el) => (inputRefs.current[i] = el)}
            type="text"
            inputMode="numeric"
            maxLength={1}
            value={d}
            onChange={(e) => handleChange(i, e.target.value)}
            onKeyDown={(e) => handleKeyDown(i, e)}
            className="w-11 h-13 text-center text-xl font-bold border-2 rounded-xl focus:outline-none focus:border-blue-500 transition bg-gray-50 focus:bg-white"
            style={{ height: '52px' }}
          />
        ))}
      </div>

      {/* Resend */}
      <div className="flex items-center justify-center gap-1.5 text-sm text-gray-500">
        <span>Không nhận được mã?</span>
        <button
          onClick={handleResend}
          disabled={resending || countdown > 0}
          className="font-semibold text-blue-600 hover:text-blue-700 disabled:text-gray-400 disabled:cursor-not-allowed"
        >
          {resending ? 'Đang gửi...' : countdown > 0 ? `Gửi lại sau ${countdown}s` : 'Gửi lại mã'}
        </button>
      </div>

      {/* Submit */}
      <button
        onClick={handleSubmit}
        disabled={loading || digits.join('').length !== 6}
        className="w-full py-2.5 px-4 bg-gray-900 hover:bg-gray-800 text-white font-semibold rounded-lg transition disabled:opacity-60 disabled:cursor-not-allowed text-sm"
      >
        {loading ? (
          <span className="flex items-center justify-center gap-2">
            <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
            Đang xác thực...
          </span>
        ) : 'Xác thực'}
      </button>
    </div>
  );
}
