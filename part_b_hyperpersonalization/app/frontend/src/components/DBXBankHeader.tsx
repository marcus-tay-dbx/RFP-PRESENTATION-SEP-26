import React from 'react';
import { theme } from '../theme';

export const DBXBankHeader: React.FC<{ subtitle?: string }> = ({ subtitle }) => (
  <header style={{ background: theme.colors.navy, color: theme.colors.white,
                   padding: '12px 24px', display: 'flex', alignItems: 'center',
                   gap: '16px', boxShadow: '0 2px 8px rgba(0,0,0,0.3)' }}>
    <img src="/dbx_bank_logo.png" alt="DBX Bank"
         style={{ height: '36px', filter: 'brightness(0) invert(1)' }} />
    <div>
      <div style={{ fontWeight: 700, fontSize: '16px', letterSpacing: '0.5px' }}>
        Customer Intelligence Platform
      </div>
      {subtitle && <div style={{ fontSize: '12px', opacity: 0.8 }}>{subtitle}</div>}
    </div>
    <div style={{ marginLeft: 'auto', background: theme.colors.red, padding: '4px 12px',
                  borderRadius: '4px', fontSize: '11px', fontWeight: 600 }}>
      POWERED BY DATABRICKS
    </div>
  </header>
);
