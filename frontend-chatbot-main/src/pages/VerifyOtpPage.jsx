import React from 'react';
import AuthBackground from '../components/auth/AuthBackground';
import VerifyOtpForm from '../components/auth/VerifyOtpForm';

export default function VerifyOtpPage() {
  return (
    <div className="relative flex min-h-screen bg-[#f5f3ef]">
      <AuthBackground />
      <div className="relative flex w-full items-center justify-center px-4 lg:w-1/2">
        <div className="absolute top-6 left-1/2 -translate-x-1/2 lg:hidden flex items-center gap-2">
          <span className="text-2xl">🤖</span>
          <span className="font-bold text-gray-800 text-lg">IT Job Analyse</span>
        </div>
        <div className="w-full max-w-md mt-16 lg:mt-0">
          <VerifyOtpForm />
        </div>
      </div>
    </div>
  );
}
