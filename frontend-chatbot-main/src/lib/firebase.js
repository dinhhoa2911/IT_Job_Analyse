import { initializeApp } from "firebase/app";
import { getAuth, GoogleAuthProvider, FacebookAuthProvider } from "firebase/auth";

const firebaseConfig = {
  apiKey:            "AIzaSyCLRELukeaUeY2kgfEhHwNcsQBkQh03wB4",
  authDomain:        "finalproject-96977.firebaseapp.com",
  projectId:         "finalproject-96977",
  storageBucket:     "finalproject-96977.firebasestorage.app",
  messagingSenderId: "34556140589",
  appId:             "1:34556140589:web:a8231f4bbcad375603eca4",
  measurementId:     "G-9H3QXTZM0H",
};

const app = initializeApp(firebaseConfig);

export const auth = getAuth(app);

export const googleProvider = new GoogleAuthProvider();
googleProvider.addScope("email");
googleProvider.addScope("profile");

// Facebook App ID (số nguyên, lấy tại developers.facebook.com → Settings → Basic)
// App Secret đặt ở Firebase Console → Authentication → Sign-in method → Facebook
export const facebookProvider = new FacebookAuthProvider();
facebookProvider.addScope("email");
facebookProvider.addScope("public_profile");
