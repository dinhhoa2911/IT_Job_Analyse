import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import LoginPage        from '../../pages/LoginPage';
import RegisterPage     from '../../pages/RegisterPage';
import ForgotPasswordPage from '../../pages/ForgotPasswordPage';
import VerifyOtpPage   from '../../pages/VerifyOtpPage';

/**
 * @component
 * @brief Auth routing shell rendered when no user is authenticated.
 * Handles /login, /register, /forgot-password, /verify-otp.
 * Any unknown path redirects to /login.
 */
export default function AuthPage() {
  return (
    <Routes>
      <Route path="/login"           element={<LoginPage />} />
      <Route path="/register"        element={<RegisterPage />} />
      <Route path="/forgot-password" element={<ForgotPasswordPage />} />
      <Route path="/verify-otp"      element={<VerifyOtpPage />} />
      <Route path="*"                element={<Navigate to="/login" replace />} />
    </Routes>
  );
}
