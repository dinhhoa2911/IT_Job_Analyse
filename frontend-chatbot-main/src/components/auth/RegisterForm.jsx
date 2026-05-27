import React, { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { FiAlertCircle, FiCheckCircle, FiEye, FiEyeOff, FiLock, FiMail, FiXCircle } from 'react-icons/fi';
import { useAuth } from '../../contexts/AuthContext';
import PasswordStrength from './PasswordStrength';

// ── Disposable email domain blocklist ─────────────────────────────────────────
const DISPOSABLE = new Set([
  'mailinator.com','guerrillamail.com','tempmail.com','throwaway.email',
  'yopmail.com','sharklasers.com','grr.la','spam4.me','trashmail.com',
  'trashmail.me','trashmail.net','10minutemail.com','maildrop.cc',
  'dispostable.com','mailnesia.com','fakeinbox.com','discard.email',
  'trashmail.at','trashmail.io','trashmail.xyz','temp-mail.org',
  'getnada.com','tempmail.net','throwam.com','mailnull.com',
  'spamgourmet.com','tempinbox.com','spamex.com','mintemail.com',
  'spamfree24.org','spam.la','binkmail.com','bobmail.info',
]);

// ── Email validation helpers ───────────────────────────────────────────────────

/** Basic RFC-5322-ish regex. */
const isValidFormat = (email) =>
  /^[^\s@]+@[^\s@]{2,}\.[^\s@]{2,}$/.test(email.trim());

/** Check disposable domain list. */
const isDisposable = (email) => {
  const domain = email.split('@')[1]?.toLowerCase() ?? '';
  return DISPOSABLE.has(domain);
};

/**
 * Check if domain has MX records using Cloudflare DNS-over-HTTPS.
 * Falls back to true (assume valid) on network error to avoid blocking users.
 */
const hasMxRecord = async (email) => {
  const domain = email.split('@')[1];
  if (!domain) return false;
  try {
    const res  = await fetch(
      `https://cloudflare-dns.com/dns-query?name=${encodeURIComponent(domain)}&type=MX`,
      { headers: { Accept: 'application/dns-json' } }
    );
    const data = await res.json();
    if (data.Status === 3) return false;        // NXDOMAIN — domain doesn't exist
    return Array.isArray(data.Answer) && data.Answer.length > 0;
  } catch {
    return true; // network error → don't block
  }
};

// ── Check status helpers ───────────────────────────────────────────────────────

const CHECK_CONFIG = {
  checking:   { border: 'border-blue-300  focus:ring-blue-400',   icon: null,             label: 'Đang kiểm tra…',                       color: 'text-blue-500'   },
  invalid:    { border: 'border-red-400   focus:ring-red-400',    icon: 'alert',          label: 'Email không hợp lệ.',                  color: 'text-red-500'    },
  disposable: { border: 'border-red-400   focus:ring-red-400',    icon: 'x',              label: 'Không chấp nhận email tạm thời.',      color: 'text-red-500'    },
  no_mx:      { border: 'border-red-400   focus:ring-red-400',    icon: 'x',              label: 'Domain email không tồn tại.',          color: 'text-red-500'    },
  taken:      { border: 'border-red-400   focus:ring-red-400',    icon: 'x',              label: 'Email đã được đăng ký trong hệ thống.', color: 'text-red-500'    },
  ok:         { border: 'border-emerald-400 focus:ring-emerald-400', icon: 'check',        label: 'Email hợp lệ và khả dụng.',            color: 'text-emerald-600'},
};

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
    <svg className="h-5 w-5" viewBox="0 0 24 24" fill="white">
      <path d="M24 12.073C24 5.405 18.627 0 12 0S0 5.405 0 12.073C0 18.1 4.388 23.094 10.125 24v-8.437H7.078v-3.49h3.047V9.41c0-3.025 1.792-4.697 4.533-4.697 1.312 0 2.686.235 2.686.235v2.97h-1.513c-1.491 0-1.956.93-1.956 1.884v2.25h3.328l-.532 3.49h-2.796V24C19.612 23.094 24 18.1 24 12.073z"/>
    </svg>
  );
}

// MFA2 check
const isPasswordStrong = (p) =>
  p.length >= 8 && /[A-Z]/.test(p) && /[a-z]/.test(p) &&
  /[0-9]/.test(p) && /[!@#$%^&*()_+\-=[\]{};':"\\|,.<>/?]/.test(p);

// ── Component ──────────────────────────────────────────────────────────────────

export default function RegisterForm() {
  const { register, googleLogin, facebookLogin, checkEmailExists } = useAuth();
  const navigate = useNavigate();

  const [email, setEmail]           = useState('');
  const [password, setPassword]     = useState('');
  const [showPass, setShowPass]     = useState(false);
  const [error, setError]           = useState('');
  const [loading, setLoading]       = useState(false);
  const [googleLoad, setGoogleLoad] = useState(false);
  const [fbLoad,     setFbLoad]     = useState(false);

  // email check state
  const [emailStatus, setEmailStatus]     = useState(null);  // null | checking | invalid | disposable | no_mx | taken | ok
  const [checkSteps, setCheckSteps]       = useState([]);    // [{label, pass}]

  // ── Email validation (3 tiers) ─────────────────────────────────────────────

  const runEmailChecks = async (val) => {
    const v = val.trim();
    if (!v) { setEmailStatus(null); setCheckSteps([]); return; }

    setEmailStatus('checking');
    setCheckSteps([]);

    const steps = [];

    // 1 — Format
    if (!isValidFormat(v)) {
      steps.push({ label: 'Định dạng email', pass: false });
      setCheckSteps(steps);
      setEmailStatus('invalid');
      return;
    }
    steps.push({ label: 'Định dạng email hợp lệ', pass: true });
    setCheckSteps([...steps]);

    // 2 — Disposable
    if (isDisposable(v)) {
      steps.push({ label: 'Email tạm thời bị chặn', pass: false });
      setCheckSteps([...steps]);
      setEmailStatus('disposable');
      return;
    }
    steps.push({ label: 'Không phải email tạm thời', pass: true });
    setCheckSteps([...steps]);

    // 3 — MX record (domain tồn tại?)
    const mx = await hasMxRecord(v);
    if (!mx) {
      steps.push({ label: 'Domain email không tồn tại', pass: false });
      setCheckSteps([...steps]);
      setEmailStatus('no_mx');
      return;
    }
    steps.push({ label: 'Domain email tồn tại (MX record OK)', pass: true });
    setCheckSteps([...steps]);

    // 4 — Firebase duplicate
    try {
      const exists = await checkEmailExists(v);
      if (exists) {
        steps.push({ label: 'Email đã được đăng ký trong hệ thống', pass: false });
        setCheckSteps([...steps]);
        setEmailStatus('taken');
        return;
      }
      steps.push({ label: 'Email chưa đăng ký — khả dụng', pass: true });
      setCheckSteps([...steps]);
      setEmailStatus('ok');
    } catch {
      // Firebase unavailable — allow registration anyway
      steps.push({ label: 'Kiểm tra hệ thống (bỏ qua)', pass: true });
      setCheckSteps([...steps]);
      setEmailStatus('ok');
    }
  };

  const handleEmailChange = (val) => {
    setEmail(val);
    setEmailStatus(null);
    setCheckSteps([]);
  };

  // ── Submit ─────────────────────────────────────────────────────────────────

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!email || !password) { setError('Vui lòng nhập đầy đủ thông tin.'); return; }

    // Force check if user skipped blur
    if (!emailStatus) { await runEmailChecks(email); return; }
    if (['invalid','disposable','no_mx','taken'].includes(emailStatus)) {
      setError(CHECK_CONFIG[emailStatus]?.label ?? 'Email không hợp lệ.');
      return;
    }
    if (emailStatus === 'checking') { setError('Đang kiểm tra email, vui lòng đợi.'); return; }

    if (!isPasswordStrong(password)) {
      setError('Mật khẩu chưa đáp ứng yêu cầu bảo mật.'); return;
    }

    setError(''); setLoading(true);
    try {
      const { otpCode } = await register(email, password);
      navigate(
        `/verify-otp?email=${encodeURIComponent(email)}&type=register`,
        { state: { demoOtp: otpCode } }
      );
    } catch (err) {
      setError(err.message || 'Đăng ký thất bại. Vui lòng thử lại.');
    } finally {
      setLoading(false);
    }
  };

  const handleGoogle = async () => {
    setError(''); setGoogleLoad(true);
    try { await googleLogin(); navigate('/'); }
    catch (err) { if (err.code !== 'auth/popup-closed-by-user') setError('Đăng nhập Google thất bại.'); }
    finally { setGoogleLoad(false); }
  };

  const handleFacebook = async () => {
    setError(''); setFbLoad(true);
    try { await facebookLogin(); navigate('/'); }
    catch (err) { if (err.code !== 'auth/popup-closed-by-user') setError('Đăng nhập Facebook thất bại.'); }
    finally { setFbLoad(false); }
  };

  // ── Border / icon for current email status ─────────────────────────────────
  const cfg = emailStatus ? CHECK_CONFIG[emailStatus] : null;
  const borderClass = cfg
    ? `${cfg.border.split(' ')[0]} focus:ring-2 ${cfg.border.split(' ')[1]}`
    : 'border-gray-300 focus:ring-blue-500';

  const isBlocked = ['invalid','disposable','no_mx','taken'].includes(emailStatus);

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div className="w-full max-w-md space-y-4 rounded-2xl bg-white bg-opacity-95 p-8 shadow-2xl backdrop-blur-sm border border-gray-100">
      <div className="text-center space-y-1">
        <h2 className="text-3xl font-bold text-gray-900">Đăng ký</h2>
        <p className="text-sm text-gray-500">Tạo tài khoản để tìm kiếm việc làm IT thông minh</p>
      </div>

      {error && (
        <div className="flex items-center gap-2 px-4 py-3 rounded-lg bg-red-50 border border-red-200 text-red-700 text-sm">
          <FiAlertCircle className="flex-shrink-0" />
          {error}
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-4">

        {/* Email field */}
        <div>
          <div className="relative">
            <input
              type="email"
              placeholder="Email"
              value={email}
              onChange={(e) => handleEmailChange(e.target.value)}
              onBlur={() => runEmailChecks(email)}
              className={`w-full pl-10 pr-10 py-2.5 border rounded-lg text-sm focus:outline-none focus:ring-2 focus:border-transparent transition ${borderClass}`}
            />
            <FiMail className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-4 h-4" />

            {/* Status icon */}
            <span className="absolute right-3 top-1/2 -translate-y-1/2">
              {emailStatus === 'checking' && (
                <span className="w-4 h-4 border-2 border-blue-400 border-t-transparent rounded-full animate-spin block" />
              )}
              {emailStatus === 'ok' && <FiCheckCircle className="w-4 h-4 text-emerald-500" />}
              {isBlocked           && <FiXCircle      className="w-4 h-4 text-red-500" />}
            </span>
          </div>

          {/* Step-by-step check results */}
          {checkSteps.length > 0 && (
            <ul className="mt-2 space-y-1">
              {checkSteps.map((s, i) => (
                <li key={i} className={`flex items-center gap-1.5 text-xs ${s.pass ? 'text-emerald-600' : 'text-red-500'}`}>
                  {s.pass
                    ? <FiCheckCircle className="w-3 h-3 flex-shrink-0" />
                    : <FiXCircle     className="w-3 h-3 flex-shrink-0" />
                  }
                  {s.label}
                </li>
              ))}
            </ul>
          )}
        </div>

        {/* Password */}
        <div>
          <div className="relative">
            <input
              type={showPass ? 'text' : 'password'}
              placeholder="Mật khẩu"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full pl-10 pr-10 py-2.5 border border-gray-300 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition"
            />
            <FiLock className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400 w-4 h-4" />
            <button
              type="button"
              onClick={() => setShowPass(!showPass)}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
            >
              {showPass ? <FiEyeOff className="w-4 h-4" /> : <FiEye className="w-4 h-4" />}
            </button>
          </div>
          <PasswordStrength password={password} />
        </div>

        <button
          type="submit"
          disabled={loading || isBlocked || emailStatus === 'checking'}
          className="w-full py-2.5 px-4 bg-gray-900 hover:bg-gray-800 text-white font-semibold rounded-lg transition disabled:opacity-50 disabled:cursor-not-allowed text-sm"
        >
          {loading ? (
            <span className="flex items-center justify-center gap-2">
              <span className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" />
              Đang tạo tài khoản…
            </span>
          ) : 'Đăng ký & nhận OTP'}
        </button>
      </form>

      <div className="flex items-center gap-3">
        <div className="flex-1 h-px bg-gray-200" />
        <span className="text-xs text-gray-400">Hoặc đăng ký với</span>
        <div className="flex-1 h-px bg-gray-200" />
      </div>

      <div className="flex flex-col gap-2.5">
        <button
          onClick={handleGoogle}
          disabled={googleLoad || fbLoad}
          className="w-full flex items-center justify-center gap-2 py-2.5 px-4 border border-gray-300 rounded-lg hover:bg-gray-50 transition font-medium text-sm text-gray-700 disabled:opacity-60"
        >
          <GoogleIcon />
          {googleLoad ? 'Đang xử lý...' : 'Đăng ký với Google'}
        </button>

        <button
          onClick={handleFacebook}
          disabled={fbLoad || googleLoad}
          className="w-full flex items-center justify-center gap-2 py-2.5 px-4 border border-[#1877F2] rounded-lg bg-[#1877F2] hover:bg-[#166fe5] transition font-medium text-sm text-white disabled:opacity-60"
        >
          <FacebookIcon />
          {fbLoad ? 'Đang xử lý...' : 'Đăng ký với Facebook'}
        </button>
      </div>

      <p className="text-center text-sm text-gray-500">
        Đã có tài khoản?{' '}
        <Link to="/login" className="font-semibold text-blue-600 hover:text-blue-700 hover:underline">
          Đăng nhập
        </Link>
      </p>
    </div>
  );
}
