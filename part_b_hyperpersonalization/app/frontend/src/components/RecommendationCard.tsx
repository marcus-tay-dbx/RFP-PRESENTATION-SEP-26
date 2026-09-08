import React from 'react';
import { theme } from '../theme';

interface Props {
  rank:          1 | 2;
  product:       string;
  confidence:    number;
  onDraftEmail?: () => void;
}

export const RecommendationCard: React.FC<Props> = ({ rank, product, confidence, onDraftEmail }) => {
  const info = theme.products[product as keyof typeof theme.products] || theme.products.NO_ACTION;
  return (
    <div style={{ border: `2px solid ${rank === 1 ? theme.colors.navy : theme.colors.grey200}`,
                  borderRadius: '8px', padding: '16px',
                  background: rank === 1 ? '#EEF2FF' : theme.colors.white }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontSize: '24px' }}>{info.icon}</span>
        <span style={{ background: rank === 1 ? theme.colors.navy : theme.colors.grey600,
                       color: 'white', borderRadius: '12px', padding: '2px 8px', fontSize: '11px' }}>
          #{rank} Pick
        </span>
      </div>
      <div style={{ fontWeight: 700, fontSize: '15px', marginTop: '8px', color: theme.colors.navy }}>
        {info.label}
      </div>
      <div style={{ margin: '8px 0' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between',
                      fontSize: '12px', marginBottom: '4px' }}>
          <span>Model Confidence</span>
          <span style={{ fontWeight: 700, color: info.color }}>{confidence}%</span>
        </div>
        <div style={{ background: theme.colors.grey200, borderRadius: '4px', height: '6px' }}>
          <div style={{ width: `${confidence}%`, background: info.color, borderRadius: '4px',
                        height: '6px', transition: 'width 0.8s ease' }} />
        </div>
      </div>
      {rank === 1 && onDraftEmail && (
        <button onClick={onDraftEmail}
          style={{ width: '100%', marginTop: '12px', padding: '8px',
                   background: theme.colors.navy, color: 'white', border: 'none',
                   borderRadius: '6px', cursor: 'pointer', fontWeight: 600, fontSize: '13px' }}>
          ✉️ Draft Personalised Email
        </button>
      )}
    </div>
  );
};
