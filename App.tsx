import React, { useState } from 'react';
import { Pressable, Text, View } from 'react-native';

// Plain Expo deciding test (2026-11-25): nine core Pressables in
// exactly the positions the Rangular nine-disc page puts them,
// and no Rangular code anywhere in the import path. If disc 3
// still refuses here, the fault is below the framework entirely.
const ROWS = [[1, 2, 3], [4, 5, 6], [7, 8, 9]];

export default function App() {
  const [hit, setHit] = useState('');
  const mark = (n: number) => setHit((h) => h + ' ' + n);
  return (
    <View style={{ flex: 1, paddingTop: 56, paddingHorizontal: 16, backgroundColor: '#ffffff' }}>
      {ROWS.map((row) => (
        <View key={row[0]} style={{ flexDirection: 'row', justifyContent: 'space-between', padding: 12 }}>
          {row.map((n) => (
            <Pressable key={n} onPress={() => mark(n)} style={{ width: 40, height: 40, borderRadius: 20, backgroundColor: '#0b57d0', alignItems: 'center', justifyContent: 'center' }}>
              <Text style={{ color: '#ffffff', fontWeight: '700' }}>{n}</Text>
            </Pressable>
          ))}
        </View>
      ))}
      <Text style={{ fontSize: 16, marginTop: 12 }}>Tapped:{hit}</Text>
    </View>
  );
}
