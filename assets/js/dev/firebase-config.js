export const firebaseConfig = {
  apiKey: 'AIzaSyC49VFcW1pjHq0sCkdcps_DwUAoo4z5oaw',
  authDomain: 'blacket-65c5b.firebaseapp.com',
  databaseURL: 'https://blacket-65c5b-default-rtdb.firebaseio.com',
  projectId: 'blacket-65c5b',
  storageBucket: 'blacket-65c5b.firebasestorage.app',
  messagingSenderId: '497023905730',
  appId: '1:497023905730:web:a59093052a3f93a476305b',
  measurementId: 'G-XZZR5DH79B'
};

let dbPromise = null;

export async function getFirestoreDb() {
  if (!dbPromise) {
    dbPromise = (async () => {
      const appModule = await import('https://www.gstatic.com/firebasejs/9.22.1/firebase-app.js');
      const fsModule = await import('https://www.gstatic.com/firebasejs/9.22.1/firebase-firestore.js');
      const app = appModule.getApps().length
        ? appModule.getApps()[0]
        : appModule.initializeApp(firebaseConfig);
      return { db: fsModule.getFirestore(app), fs: fsModule };
    })();
  }
  return dbPromise;
}
