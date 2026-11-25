import React from 'react';
import AppRoot from './src/AppRoot';

// Hardware back (Android) and Rangular routing: the compiled root
// module keeps its navigator private, so App.tsx cannot call back()
// on it yet. In-app back navigation is a template concern today (a
// button that navigates where the user came from); the app README's
// "Back behaviour" section states exactly what works and what does
// not. Nothing is wired here that is not real.
export default function App() {
  return <AppRoot />;
}
