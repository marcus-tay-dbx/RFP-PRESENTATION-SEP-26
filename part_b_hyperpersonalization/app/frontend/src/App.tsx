import React, { useState } from 'react';
import Customer360ViewA from './pages/Customer360ViewA';
import Customer360ViewB from './pages/Customer360ViewB';
import { theme } from './theme';

export default function App() {
  const [template, setTemplate] = useState<'A' | 'B'>('B');
  return (
    <div>
      {/* Template switcher — demo-only */}
      <div style={{ position:'fixed', bottom:'16px', right:'16px', zIndex:300,
                    background:theme.colors.navy, borderRadius:'24px', padding:'6px',
                    display:'flex', gap:'4px', boxShadow:'0 4px 12px rgba(0,0,0,0.3)' }}>
        {(['A','B'] as const).map(t => (
          <button key={t} onClick={() => setTemplate(t)}
            style={{ padding:'6px 16px', borderRadius:'20px', border:'none',
                     cursor:'pointer', fontWeight:600, fontSize:'12px',
                     background: template === t ? theme.colors.red : 'transparent',
                     color: 'white' }}>
            Template {t}
          </button>
        ))}
      </div>
      {template === 'A' ? <Customer360ViewA /> : <Customer360ViewB />}
    </div>
  );
}
