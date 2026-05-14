import React from 'react';
import AuthBackground from '../components/auth/AuthBackground';
import LoginForm from '../components/auth/LoginForm';

export default function LoginPage() {
  return (
    <div className="relative flex min-h-screen bg-[#f5f3ef]">
      <AuthBackground />
      {/* Right — form */}
      <div className="relative flex w-full items-center justify-center px-4 lg:w-1/2">
        {/* Mobile top logo */}
        <div className="absolute top-6 left-1/2 -translate-x-1/2 lg:hidden flex items-center gap-2">
          <span className="text-2xl">🤖</span>
          <span className="font-bold text-gray-800 text-lg">IT Job Analyse</span>
        </div>
        <div className="w-full max-w-md mt-16 lg:mt-0">
          <LoginForm />
        </div>
      </div>
    </div>
  );
}
