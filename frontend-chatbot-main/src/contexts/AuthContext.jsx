import { createContext, useContext, useEffect, useState } from 'react';
import {
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
  signInWithPopup,
  signOut,
  sendPasswordResetEmail,
  onAuthStateChanged,
  fetchSignInMethodsForEmail,
  reload,
} from 'firebase/auth';
import { doc, setDoc, getDoc, deleteDoc, serverTimestamp, Timestamp } from 'firebase/firestore';
import emailjs from '@emailjs/browser';
import { auth, db, googleProvider } from '../firebase';

const AuthContext = createContext(null);

export function useAuth() {
  return useContext(AuthContext);
}

// ── Session storage key for pending OTP ───────────────────────────────────────
// Firebase auto-signs-in after createUserWithEmailAndPassword, so we need to
// gate access to the chat until the user completes OTP verification.
const PENDING_KEY = 'otp_pending_email';
const DEMO_OTP_KEY = 'otp_demo_code'; // stores latest generated code for demo display

// ── OTP helpers ────────────────────────────────────────────────────────────────

function generateOtp() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

async function storeOtp(email, type) {
  const code      = generateOtp();
  const expiresAt = Timestamp.fromDate(new Date(Date.now() + 10 * 60 * 1000));
  await setDoc(doc(db, 'otpCodes', email.toLowerCase()), {
    code, type, expiresAt, createdAt: serverTimestamp(),
  });
  return code;
}

// EmailJS credentials
const EMAILJS_SERVICE_ID  = 'service_m9sz77m';
const EMAILJS_TEMPLATE_ID = 'template_uje7471';
const EMAILJS_PUBLIC_KEY  = 'UqnUdAZqQ-51d5R1Y';

async function sendOtpEmail(email, code) {
  try {
    const result = await emailjs.send(
      EMAILJS_SERVICE_ID,
      EMAILJS_TEMPLATE_ID,
      {
        to_email: email,      // recipient — maps to {{email}} in "To Email" field
        email,                // extra alias in case template uses {{email}}
        passcode: code,       // {{passcode}} — OTP code
        time: '10 phút',      // {{time}} — expiry
      },
      EMAILJS_PUBLIC_KEY
    );
    console.log('OTP email sent:', result.status, result.text);
  } catch (err) {
    console.error('EmailJS error detail:', err?.status, err?.text, err);
  }
}

async function verifyOtpCode(email, inputCode) {
  const ref  = doc(db, 'otpCodes', email.toLowerCase());
  const snap = await getDoc(ref);
  if (!snap.exists()) throw new Error('Mã OTP không hợp lệ hoặc đã hết hạn.');
  const { code, expiresAt } = snap.data();
  if (new Date() > expiresAt.toDate()) {
    await deleteDoc(ref);
    throw new Error('Mã OTP đã hết hạn. Vui lòng yêu cầu mã mới.');
  }
  if (code !== inputCode) throw new Error('Mã OTP không đúng. Vui lòng thử lại.');
  await deleteDoc(ref);
}

// ── Provider ───────────────────────────────────────────────────────────────────

export function AuthProvider({ children }) {
  const [user, setUser]                         = useState(null);
  const [loading, setLoading]                   = useState(true);
  // pendingVerification: email address that still needs OTP confirmation.
  // Backed by sessionStorage so it survives page refresh in the same tab.
  const [pendingVerification, setPendingVerification] = useState(
    () => sessionStorage.getItem(PENDING_KEY) || null
  );

  const setPending = (email) => {
    if (email) sessionStorage.setItem(PENDING_KEY, email);
    else        sessionStorage.removeItem(PENDING_KEY);
    setPendingVerification(email);
  };

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, (u) => {
      setUser(u);
      setLoading(false);
      // If user signed in via Google or a persisted session (not fresh registration),
      // clear any stale pending flag that doesn't belong to this user.
      if (u) {
        const pending = sessionStorage.getItem(PENDING_KEY);
        if (pending && pending !== u.email) {
          setPending(null);
        }
      }
    });
    return unsub;
  }, []);

  const checkEmailExists = async (email) => {
    const methods = await fetchSignInMethodsForEmail(auth, email);
    return methods.length > 0;
  };

  /**
   * Register with email/password.
   * After creation Firebase auto-signs-in, so we set pendingVerification
   * to gate the user on the OTP page until they verify.
   */
  const register = async (email, password) => {
    const exists = await checkEmailExists(email);
    if (exists) throw new Error('Email này đã được đăng ký. Vui lòng dùng email khác.');
    const cred    = await createUserWithEmailAndPassword(auth, email, password);
    const otpCode = await storeOtp(email, 'register');
    // Send real email via Firebase Extension + keep demo banner as fallback
    await sendOtpEmail(email, otpCode, 'register');
    sessionStorage.setItem(DEMO_OTP_KEY, otpCode);
    // Block access to chat until OTP is verified
    setPending(email);
    return { user: cred.user, otpCode };
  };

  const resendOtp = async (email, type = 'register') => {
    const newCode = await storeOtp(email, type);
    await sendOtpEmail(email, newCode);
    return newCode;
  };

  /**
   * Verify the 6-digit OTP from Firestore.
   * Clears the pending gate on success → user can now access the chat.
   */
  const verifyOtp = async (email, inputCode) => {
    await verifyOtpCode(email, inputCode);
    if (auth.currentUser) await reload(auth.currentUser);
    sessionStorage.removeItem(DEMO_OTP_KEY);
    setPending(null); // OTP confirmed → lift the gate
  };

  const login = (email, password) => signInWithEmailAndPassword(auth, email, password);
  const googleLogin = () => signInWithPopup(auth, googleProvider);
  const logout = () => {
    setPending(null);
    return signOut(auth);
  };
  const forgotPassword = (email) => sendPasswordResetEmail(auth, email);

  return (
    <AuthContext.Provider value={{
      user, loading, pendingVerification,
      checkEmailExists, register, resendOtp, verifyOtp,
      login, googleLogin, logout, forgotPassword,
    }}>
      {!loading && children}
    </AuthContext.Provider>
  );
}
