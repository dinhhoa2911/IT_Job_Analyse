import { initializeApp } from 'firebase/app';
import { getAuth, GoogleAuthProvider, FacebookAuthProvider } from 'firebase/auth';
import { getFirestore } from 'firebase/firestore';
import { getStorage } from 'firebase/storage';

const firebaseConfig = {
  apiKey: "AIzaSyCLRELukeaUeY2kgfEhHwNcsQBkQh03wB4",
  authDomain: "finalproject-96977.firebaseapp.com",
  projectId: "finalproject-96977",
  storageBucket: "finalproject-96977.firebasestorage.app",
  messagingSenderId: "34556140589",
  appId: "1:34556140589:web:a8231f4bbcad375603eca4",
  measurementId: "G-9H3QXTZM0H",
};

const app = initializeApp(firebaseConfig);

export const auth           = getAuth(app);
export const db             = getFirestore(app);
export const storage        = getStorage(app);

export const googleProvider = new GoogleAuthProvider();

// Facebook: App ID đặt tại Firebase Console → Authentication → Sign-in method → Facebook
// App Secret (chuỗi hex 32 ký tự) chỉ đặt trong Firebase Console, KHÔNG đưa vào code này
export const facebookProvider = new FacebookAuthProvider();
facebookProvider.addScope('email');
facebookProvider.addScope('public_profile');

export default app;
