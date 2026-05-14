import { initializeApp } from 'firebase/app';
import { getAuth, GoogleAuthProvider } from 'firebase/auth';
import { getFirestore } from 'firebase/firestore';

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

export const auth = getAuth(app);
export const db   = getFirestore(app);
export const googleProvider = new GoogleAuthProvider();

export default app;
