import React, { useState, useEffect, useCallback, useMemo } from 'react';

// ── TYPES ───────────────────────────────────────────────────────────────────
type Theme = 'light' | 'dark' | 'system';
type Page = 'overview' | 'detail';
type SortField = 'total_deposit_balance_myr' | 'ctos_score' | 'relationship_tenure_years' | 'legal_name' | 'confidence_1';
interface Customer { [key: string]: any; }

// ── THEME TOKENS ────────────────────────────────────────────────────────────
const LIGHT = {
  bg: '#F1F5F9', card: '#FFFFFF', border: '#E2E8F0',
  text: '#0F172A', muted: '#64748B', subtle: '#94A3B8',
  sidebar: '#1E293B', sidebarText: '#F8FAFC', sidebarMuted: '#94A3B8',
  input: '#F8FAFC', inputBorder: '#CBD5E1',
  hover: '#F8FAFC', activeRow: '#EFF6FF',
  tableHeader: '#F8FAFC', tableBorder: '#E2E8F0',
  badge: '#E2E8F0', badgeText: '#475569',
  red: '#FF3621', redHover: '#E5311E',
  green: '#16A34A', amber: '#D97706', crimson: '#DC2626',
  navy: '#1B3A6B', blue: '#2563EB', blueLight: '#DBEAFE',
};
const DARK = {
  bg: '#0F172A', card: '#1E293B', border: '#334155',
  text: '#F1F5F9', muted: '#94A3B8', subtle: '#64748B',
  sidebar: '#020617', sidebarText: '#F1F5F9', sidebarMuted: '#64748B',
  input: '#1E293B', inputBorder: '#475569',
  hover: '#334155', activeRow: '#1E3A5F',
  tableHeader: '#1E293B', tableBorder: '#334155',
  badge: '#334155', badgeText: '#CBD5E1',
  red: '#FF3621', redHover: '#E5311E',
  green: '#22C55E', amber: '#FBBF24', crimson: '#F87171',
  navy: '#3B82F6', blue: '#60A5FA', blueLight: '#1E3A5F',
};

function useThemeColors(theme: Theme): typeof LIGHT {
  const [dark, setDark] = useState(
    typeof window !== 'undefined'
      ? window.matchMedia('(prefers-color-scheme: dark)').matches
      : false
  );
  useEffect(() => {
    if (theme !== 'system') return;
    const mq = window.matchMedia('(prefers-color-scheme: dark)');
    const handler = (e: MediaQueryListEvent) => setDark(e.matches);
    mq.addEventListener('change', handler);
    return () => mq.removeEventListener('change', handler);
  }, [theme]);
  if (theme === 'dark' || (theme === 'system' && dark)) return DARK;
  return LIGHT;
}

// ── REFERENCE DATA ──────────────────────────────────────────────────────────
const RM_LIST = [
  'Ahmad Fadzillah bin Roslan',
  'Nurul Hana binti Zainal Abidin',
  'Rajan Kumar Subramaniam',
  'Tan Wei Ming',
  'Priya Nair d/o Krishnan',
  'Mohd Izzat bin Ibrahim',
];

const SEGMENTS: Record<string, { label: string; bg: string; text: string }> = {
  mass_market:          { label: 'Mass Market',    bg: '#E2E8F0', text: '#475569' },
  affluent:             { label: 'Affluent',        bg: '#DBEAFE', text: '#1D4ED8' },
  high_net_worth:       { label: 'High Net Worth',  bg: '#FEF9C3', text: '#A16207' },
  private_banking:      { label: 'Private Banking', bg: '#1B3A6B', text: '#FFFFFF' },
  premier:              { label: 'Premier',         bg: '#EDE9FE', text: '#6D28D9' },
};

const PRODUCTS: Record<string, { icon: string; label: string; color: string }> = {
  CREDIT_CARD:   { icon: '💳', label: 'Credit Card',     color: '#2563EB' },
  HOME_LOAN:     { icon: '🏠', label: 'Home Loan',       color: '#16A34A' },
  PERSONAL_LOAN: { icon: '💰', label: 'Personal Loan',   color: '#D97706' },
  INVESTMENT:    { icon: '📈', label: 'Investment',      color: '#7C3AED' },
  INSURANCE:     { icon: '🛡️', label: 'Insurance',       color: '#0891B2' },
  NO_ACTION:     { icon: '✓',  label: 'No Action',       color: '#64748B' },
  PENDING:       { icon: '⏳', label: 'Pending',         color: '#94A3B8' },
};

// ── UTILITIES ────────────────────────────────────────────────────────────────
const fmtMYR = (v: any) => {
  const n = parseFloat(v);
  if (isNaN(n)) return '—';
  if (n >= 1e9) return `MYR ${(n/1e9).toFixed(1)}B`;
  if (n >= 1e6) return `MYR ${(n/1e6).toFixed(1)}M`;
  if (n >= 1e3) return `MYR ${(n/1e3).toFixed(0)}K`;
  return `MYR ${n.toLocaleString('en-MY', { maximumFractionDigits: 0 })}`;
};
const fmtMYRFull = (v: any) => {
  const n = parseFloat(v);
  if (isNaN(n)) return '—';
  return `MYR ${n.toLocaleString('en-MY', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
};
const fmtMYT = (v: any) => {
  if (!v) return '—';
  try {
    return new Date(v).toLocaleString('en-MY', {
      timeZone: 'Asia/Kuala_Lumpur',
      day: '2-digit', month: 'short', year: 'numeric',
    });
  } catch { return String(v); }
};
const nowMYT = () => new Date().toLocaleString('en-MY', {
  timeZone: 'Asia/Kuala_Lumpur',
  hour: '2-digit', minute: '2-digit', hour12: false,
}) + ' MYT';

const initials = (name: string) =>
  name?.split(' ').filter(Boolean).slice(0, 2).map(w => w[0]?.toUpperCase()).join('') || '??';

const ctosColor = (score: any, C: typeof LIGHT) => {
  const n = parseInt(score);
  if (isNaN(n)) return C.muted;
  if (n >= 700) return C.green;
  if (n >= 600) return C.amber;
  return C.crimson;
};

const getProd = (key: string) => PRODUCTS[key] || PRODUCTS.PENDING;
const getSeg  = (key: string) => SEGMENTS[key] || { label: key || '—', bg: '#E2E8F0', text: '#475569' };

// ── SHARED COMPONENTS ────────────────────────────────────────────────────────
const Badge = ({ label, bg, text }: { label: string; bg: string; text: string }) => (
  <span style={{ padding: '2px 8px', borderRadius: '12px', fontSize: '11px',
                 fontWeight: 600, background: bg, color: text, whiteSpace: 'nowrap' }}>
    {label}
  </span>
);

const KpiCard = ({ label, value, sub, C }: { label: string; value: string; sub?: string; C: typeof LIGHT }) => (
  <div style={{ background: C.card, border: `1px solid ${C.border}`, borderRadius: '12px',
                padding: '16px 20px', flex: 1, minWidth: '160px' }}>
    <div style={{ fontSize: '11px', color: C.muted, fontWeight: 600, textTransform: 'uppercase',
                  letterSpacing: '0.06em', marginBottom: '6px' }}>{label}</div>
    <div style={{ fontSize: '22px', fontWeight: 800, color: C.text, lineHeight: 1.1 }}>{value}</div>
    {sub && <div style={{ fontSize: '11px', color: C.muted, marginTop: '4px' }}>{sub}</div>}
  </div>
);

const Tab = ({ label, active, onClick, C }: { label: string; active: boolean; onClick: () => void; C: typeof LIGHT }) => (
  <button onClick={onClick} style={{
    padding: '8px 16px', border: 'none', borderRadius: '8px', cursor: 'pointer',
    fontWeight: active ? 700 : 500, fontSize: '13px',
    background: active ? C.red : 'transparent',
    color: active ? '#FFFFFF' : C.muted,
    transition: 'all 0.15s',
  }}>{label}</button>
);

const InfoRow = ({ label, value, C }: { label: string; value: string; C: typeof LIGHT }) => (
  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '8px 0', borderBottom: `1px solid ${C.border}` }}>
    <span style={{ fontSize: '13px', color: C.muted }}>{label}</span>
    <span style={{ fontSize: '13px', fontWeight: 500, color: C.text }}>{value || '—'}</span>
  </div>
);

// ── MAIN APP ─────────────────────────────────────────────────────────────────
export default function App() {
  // ── Theme & RM ──────────────────────────────────────────────────────────
  const [theme, setTheme] = useState<Theme>('light');
  const C = useThemeColors(theme);
  const [rm, setRm] = useState(RM_LIST[0]);
  const [showRmDrop, setShowRmDrop] = useState(false);
  const [showThemeDrop, setShowThemeDrop] = useState(false);
  const [myt, setMyt] = useState(nowMYT);

  useEffect(() => {
    const t = setInterval(() => setMyt(nowMYT()), 30000);
    return () => clearInterval(t);
  }, []);

  // ── Routing ─────────────────────────────────────────────────────────────
  const [page, setPage] = useState<Page>('overview');
  const [selectedId, setSelectedId] = useState<string | null>(null);

  // ── Overview state ───────────────────────────────────────────────────────
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [loadingList, setLoadingList] = useState(true);
  const [searchQ, setSearchQ] = useState('');
  const [segFilter, setSegFilter] = useState('');
  const [stateFilter, setStateFilter] = useState('');
  const [sortField, setSortField] = useState<SortField>('total_deposit_balance_myr');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [page_, setPage_] = useState(1);
  const PAGE_SIZE = 25;

  // ── Detail state ─────────────────────────────────────────────────────────
  const [customer, setCustomer] = useState<Customer | null>(null);
  const [recs, setRecs] = useState<Customer | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [detailTab, setDetailTab] = useState('Profile');
  const [liveLoading, setLiveLoading] = useState(false);
  const [liveError, setLiveError] = useState('');

  // ── Modals ───────────────────────────────────────────────────────────────
  const [showEmail, setShowEmail] = useState(false);
  const [emailDraft, setEmailDraft] = useState('');
  const [emailLoading, setEmailLoading] = useState(false);
  const [showExplain, setShowExplain] = useState<1|2|null>(null);
  const [explanation, setExplanation] = useState('');
  const [explainLoading, setExplainLoading] = useState(false);

  // ── Fetch customer list ──────────────────────────────────────────────────
  useEffect(() => {
    fetch('/api/customers?limit=200')
      .then(r => r.json())
      .then(d => setCustomers(Array.isArray(d) ? d : []))
      .catch(() => setCustomers([]))
      .finally(() => setLoadingList(false));
  }, []);

  // ── Fetch customer detail ────────────────────────────────────────────────
  const loadDetail = useCallback(async (id: string) => {
    setLoadingDetail(true);
    setCustomer(null);
    setRecs(null);
    setLiveError('');
    setDetailTab('Profile');
    try {
      const [c, r] = await Promise.all([
        fetch(`/api/customer/${id}`).then(r => r.json()),
        fetch(`/api/customer/${id}/recommendations`).then(r => r.json()).catch(() => null),
      ]);
      setCustomer(c);
      setRecs(r);
    } catch {}
    setLoadingDetail(false);
  }, []);

  const openDetail = (id: string) => {
    setSelectedId(id);
    setPage('detail');
    loadDetail(id);
  };

  // ── Live score ───────────────────────────────────────────────────────────
  const liveScore = async () => {
    if (!selectedId) return;
    setLiveLoading(true); setLiveError('');
    try {
      const r = await fetch(`/api/customer/${selectedId}/recommend-live`, { method: 'POST' });
      if (!r.ok) throw new Error(`${r.status}`);
      setRecs(await r.json());
    } catch { setLiveError('Live scoring failed — check endpoint availability'); }
    setLiveLoading(false);
  };

  // ── Draft email ──────────────────────────────────────────────────────────
  const draftEmail = async () => {
    if (!customer) return;
    setShowEmail(true); setEmailDraft(''); setEmailLoading(true);
    try {
      const r = await fetch('/api/draft-email', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          party_id: customer.party_id, product: recs?.recommendation_1,
          confidence: recs?.confidence_1, customer_name: customer.legal_name,
          segment: customer.lifestyle_segment, tenure: customer.relationship_tenure_years,
        }),
      });
      const d = await r.json();
      setEmailDraft(d.email_draft || '(No content)');
    } catch { setEmailDraft('Error generating email.'); }
    setEmailLoading(false);
  };

  // ── Explain ─────────────────────────────────────────────────────────────
  const explainRec = async (rank: 1|2) => {
    if (!customer || !recs) return;
    const product    = rank === 1 ? recs.recommendation_1 : recs.recommendation_2;
    const confidence = rank === 1 ? recs.confidence_1 : recs.confidence_2;
    setShowExplain(rank); setExplanation(''); setExplainLoading(true);
    try {
      const r = await fetch('/api/explain-recommendation', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ party_id: customer.party_id, product, confidence, recommendation_rank: rank }),
      });
      const d = await r.json();
      setExplanation(d.explanation || '(No explanation)');
    } catch { setExplanation('Error generating explanation.'); }
    setExplainLoading(false);
  };

  // ── Derived overview data ────────────────────────────────────────────────
  const states = useMemo(() => Array.from(new Set(customers.map(c => c.primary_state).filter(Boolean))).sort(), [customers]);

  const filtered = useMemo(() => {
    let data = customers.filter(c => {
      const q = searchQ.toLowerCase();
      const nameMatch = !q || c.legal_name?.toLowerCase().includes(q) || c.cif_number?.toLowerCase().includes(q);
      const segMatch  = !segFilter || c.lifestyle_segment === segFilter;
      const stateMatch = !stateFilter || c.primary_state === stateFilter;
      return nameMatch && segMatch && stateMatch;
    });
    data = [...data].sort((a, b) => {
      const av = a[sortField], bv = b[sortField];
      if (sortField === 'legal_name') {
        return sortDir === 'asc' ? String(av||'').localeCompare(String(bv||'')) : String(bv||'').localeCompare(String(av||''));
      }
      return sortDir === 'asc' ? (parseFloat(av)||0) - (parseFloat(bv)||0) : (parseFloat(bv)||0) - (parseFloat(av)||0);
    });
    return data;
  }, [customers, searchQ, segFilter, stateFilter, sortField, sortDir]);

  const totalPages = Math.ceil(filtered.length / PAGE_SIZE);
  const pageData   = filtered.slice((page_-1)*PAGE_SIZE, page_*PAGE_SIZE);

  const kpis = useMemo(() => {
    const active = customers.filter(c => c.lifecycle_status === 'active');
    const totalBal = active.reduce((s, c) => s + (parseFloat(c.total_deposit_balance_myr) || 0), 0);
    const avgCtos  = active.length ? Math.round(active.reduce((s,c) => s + (parseInt(c.ctos_score)||0), 0) / active.length) : 0;
    const withRec  = active.filter(c => c.recommendation_1 && c.recommendation_1 !== 'PENDING' && c.recommendation_1 !== 'NO_ACTION').length;
    return { count: active.length, totalBal, avgCtos, recPct: active.length ? Math.round(100*withRec/active.length) : 0 };
  }, [customers]);

  // ── Sort toggle ──────────────────────────────────────────────────────────
  const toggleSort = (field: SortField) => {
    if (sortField === field) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortField(field); setSortDir('desc'); }
  };
  const sortIcon = (field: SortField) => sortField === field ? (sortDir === 'desc' ? ' ↓' : ' ↑') : '';

  // ── Styles helpers ───────────────────────────────────────────────────────
  const cardStyle: React.CSSProperties = {
    background: C.card, border: `1px solid ${C.border}`,
    borderRadius: '12px', padding: '20px',
    boxShadow: '0 1px 4px rgba(0,0,0,0.06)',
  };

  // ══════════════════════════════════════════════════════════════════════════
  // RENDER
  // ══════════════════════════════════════════════════════════════════════════
  return (
    <div style={{ minHeight: '100vh', background: C.bg, color: C.text,
                  fontFamily: "'Inter', 'system-ui', sans-serif",
                  transition: 'background 0.2s, color 0.2s' }}>

      {/* ════════════════ HEADER ════════════════════════════════════════════ */}
      <header style={{ background: C.sidebar, color: C.sidebarText,
                       padding: '0 24px', height: '56px',
                       display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                       position: 'sticky', top: 0, zIndex: 100,
                       borderBottom: `1px solid rgba(255,255,255,0.06)` }}>
        {/* Logo */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', cursor: 'pointer' }}
             onClick={() => setPage('overview')}>
          <div style={{ width: '32px', height: '32px', borderRadius: '8px',
                        background: '#FF3621', display: 'flex', alignItems: 'center',
                        justifyContent: 'center', fontSize: '18px', fontWeight: 900 }}>
            ◈
          </div>
          <div>
            <div style={{ fontWeight: 800, fontSize: '15px', letterSpacing: '-0.3px',
                          color: '#FFFFFF' }}>DBX Bank</div>
            <div style={{ fontSize: '10px', color: C.sidebarMuted, letterSpacing: '0.04em',
                          fontWeight: 500 }}>RM INTELLIGENCE PLATFORM</div>
          </div>
        </div>

        {/* Center: Page breadcrumb */}
        <div style={{ fontSize: '13px', color: C.sidebarMuted, fontWeight: 500 }}>
          {page === 'overview' ? 'Customer Overview' : customer ? `${customer.legal_name} · Customer 360` : 'Loading…'}
        </div>

        {/* Right controls */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          {/* MYT clock */}
          <div style={{ fontSize: '12px', color: C.sidebarMuted, fontWeight: 500,
                        background: 'rgba(255,255,255,0.07)', padding: '4px 10px', borderRadius: '6px' }}>
            🕐 {myt}
          </div>

          {/* Theme toggle */}
          <div style={{ position: 'relative' }}>
            <button onClick={() => setShowThemeDrop(v => !v)} style={{
              background: 'rgba(255,255,255,0.07)', border: 'none', borderRadius: '6px',
              color: C.sidebarText, padding: '5px 10px', cursor: 'pointer', fontSize: '12px',
              fontWeight: 500, display: 'flex', alignItems: 'center', gap: '4px',
            }}>
              {theme === 'light' ? '☀️' : theme === 'dark' ? '🌙' : '💻'}
              <span>{theme.charAt(0).toUpperCase() + theme.slice(1)}</span>
              <span style={{ fontSize: '10px' }}>▼</span>
            </button>
            {showThemeDrop && (
              <div style={{ position: 'absolute', right: 0, top: '36px', background: C.card,
                            border: `1px solid ${C.border}`, borderRadius: '8px', minWidth: '130px',
                            boxShadow: '0 8px 24px rgba(0,0,0,0.15)', zIndex: 200 }}>
                {(['light','dark','system'] as Theme[]).map(t => (
                  <div key={t} onClick={() => { setTheme(t); setShowThemeDrop(false); }}
                    style={{ padding: '9px 14px', cursor: 'pointer', fontSize: '13px',
                             color: theme === t ? C.red : C.text, fontWeight: theme === t ? 700 : 400,
                             display: 'flex', gap: '8px', alignItems: 'center' }}>
                    {t === 'light' ? '☀️' : t === 'dark' ? '🌙' : '💻'}
                    {t.charAt(0).toUpperCase() + t.slice(1)}
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* RM Dropdown */}
          <div style={{ position: 'relative' }}>
            <button onClick={() => setShowRmDrop(v => !v)} style={{
              background: 'rgba(255,255,255,0.07)', border: 'none', borderRadius: '6px',
              color: C.sidebarText, padding: '5px 12px', cursor: 'pointer',
              fontSize: '12px', fontWeight: 500, display: 'flex', alignItems: 'center', gap: '6px',
            }}>
              <div style={{ width: '22px', height: '22px', borderRadius: '50%', background: C.red,
                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                            fontSize: '10px', fontWeight: 700, color: '#fff' }}>
                {initials(rm)}
              </div>
              <span style={{ maxWidth: '130px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {rm.split(' ').slice(0,2).join(' ')}
              </span>
              <span style={{ fontSize: '10px' }}>▼</span>
            </button>
            {showRmDrop && (
              <div style={{ position: 'absolute', right: 0, top: '36px', background: C.card,
                            border: `1px solid ${C.border}`, borderRadius: '8px', minWidth: '240px',
                            boxShadow: '0 8px 24px rgba(0,0,0,0.15)', zIndex: 200 }}>
                <div style={{ padding: '8px 12px', fontSize: '10px', color: C.muted,
                              fontWeight: 700, letterSpacing: '0.06em', textTransform: 'uppercase',
                              borderBottom: `1px solid ${C.border}` }}>RELATIONSHIP MANAGERS</div>
                {RM_LIST.map(name => (
                  <div key={name} onClick={() => { setRm(name); setShowRmDrop(false); }}
                    style={{ padding: '9px 14px', cursor: 'pointer', fontSize: '13px',
                             color: rm === name ? C.red : C.text, fontWeight: rm === name ? 700 : 400,
                             display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <div style={{ width: '24px', height: '24px', borderRadius: '50%',
                                  background: rm === name ? C.red : C.border, flexShrink: 0,
                                  display: 'flex', alignItems: 'center', justifyContent: 'center',
                                  fontSize: '10px', fontWeight: 700,
                                  color: rm === name ? '#fff' : C.muted }}>
                      {initials(name)}
                    </div>
                    {name}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </header>

      {/* Dismiss dropdowns on outside click */}
      {(showRmDrop || showThemeDrop) && (
        <div style={{ position: 'fixed', inset: 0, zIndex: 99 }}
             onClick={() => { setShowRmDrop(false); setShowThemeDrop(false); }} />
      )}

      {/* ════════════════ PAGE CONTENT ══════════════════════════════════════ */}
      <main style={{ maxWidth: '1440px', margin: '0 auto', padding: '24px' }}>

        {/* ── OVERVIEW PAGE ─────────────────────────────────────────────── */}
        {page === 'overview' && (
          <>
            {/* KPI Row */}
            <div style={{ display: 'flex', gap: '16px', marginBottom: '20px', flexWrap: 'wrap' }}>
              <KpiCard label="Active Customers" value={loadingList ? '…' : kpis.count.toLocaleString()}
                       sub="In portfolio" C={C} />
              <KpiCard label="Total Portfolio Value" value={loadingList ? '…' : fmtMYR(kpis.totalBal)}
                       sub="Deposit balances" C={C} />
              <KpiCard label="Avg CTOS Score" value={loadingList ? '…' : `${kpis.avgCtos}/850`}
                       sub="Portfolio average" C={C} />
              <KpiCard label="With AI Recommendations" value={loadingList ? '…' : `${kpis.recPct}%`}
                       sub="Actionable insights" C={C} />
            </div>

            {/* Filter bar */}
            <div style={{ ...cardStyle, display: 'flex', gap: '12px', marginBottom: '16px',
                          alignItems: 'center', flexWrap: 'wrap' }}>
              <input
                value={searchQ}
                onChange={e => { setSearchQ(e.target.value); setPage_(1); }}
                placeholder="🔍  Search by name or CIF…"
                style={{ flex: '1 1 220px', padding: '8px 12px', borderRadius: '8px',
                         border: `1px solid ${C.inputBorder}`, background: C.input,
                         color: C.text, fontSize: '13px', outline: 'none' }}
              />
              <select value={segFilter} onChange={e => { setSegFilter(e.target.value); setPage_(1); }}
                style={{ padding: '8px 10px', borderRadius: '8px', border: `1px solid ${C.inputBorder}`,
                         background: C.input, color: C.text, fontSize: '13px', cursor: 'pointer' }}>
                <option value="">All Segments</option>
                {['mass_market','affluent','high_net_worth','private_banking','premier'].map(s => (
                  <option key={s} value={s}>{getSeg(s).label}</option>
                ))}
              </select>
              <select value={stateFilter} onChange={e => { setStateFilter(e.target.value); setPage_(1); }}
                style={{ padding: '8px 10px', borderRadius: '8px', border: `1px solid ${C.inputBorder}`,
                         background: C.input, color: C.text, fontSize: '13px', cursor: 'pointer' }}>
                <option value="">All States</option>
                {states.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
              <div style={{ fontSize: '12px', color: C.muted, whiteSpace: 'nowrap' }}>
                {filtered.length.toLocaleString()} customers
              </div>
            </div>

            {/* Table */}
            <div style={{ ...cardStyle, padding: 0, overflow: 'hidden' }}>
              <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
                  <thead>
                    <tr style={{ background: C.tableHeader, borderBottom: `2px solid ${C.tableBorder}` }}>
                      {[
                        ['Customer', null],
                        ['Segment', null],
                        ['State', null],
                        ['CTOS Score', 'ctos_score'],
                        ['Portfolio Balance', 'total_deposit_balance_myr'],
                        ['Tenure', 'relationship_tenure_years'],
                        ['Next Best Product', 'confidence_1'],
                        ['Actions', null],
                      ].map(([label, field]) => (
                        <th key={label as string}
                          onClick={field ? () => toggleSort(field as SortField) : undefined}
                          style={{ padding: '12px 16px', textAlign: 'left', fontWeight: 600,
                                   color: C.muted, fontSize: '11px', textTransform: 'uppercase',
                                   letterSpacing: '0.05em', whiteSpace: 'nowrap',
                                   cursor: field ? 'pointer' : 'default', userSelect: 'none' }}>
                          {label as string}{field ? sortIcon(field as SortField) : ''}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {loadingList && Array.from({ length: 10 }).map((_, i) => (
                      <tr key={i}>
                        {Array.from({ length: 8 }).map((__, j) => (
                          <td key={j} style={{ padding: '12px 16px' }}>
                            <div style={{ height: '14px', borderRadius: '6px',
                                          background: C.border, width: j===0?'160px':'80px',
                                          animation: 'pulse 1.5s infinite' }} />
                          </td>
                        ))}
                      </tr>
                    ))}
                    {!loadingList && pageData.length === 0 && (
                      <tr><td colSpan={8} style={{ padding: '40px', textAlign: 'center', color: C.muted }}>
                        {customers.length === 0 ? 'No customer data available. Ensure Part A pipeline has completed.' : 'No customers match the current filters.'}
                      </td></tr>
                    )}
                    {!loadingList && pageData.map((c, i) => {
                      const seg  = getSeg(c.lifestyle_segment);
                      const prod = getProd(c.recommendation_1);
                      const conf = parseFloat(c.confidence_1) || 0;
                      return (
                        <tr key={c.party_id}
                          style={{ borderBottom: `1px solid ${C.tableBorder}`,
                                   background: i%2===0 ? C.card : C.bg,
                                   transition: 'background 0.1s' }}
                          onMouseEnter={e => (e.currentTarget.style.background = C.hover)}
                          onMouseLeave={e => (e.currentTarget.style.background = i%2===0 ? C.card : C.bg)}>
                          {/* Customer */}
                          <td style={{ padding: '12px 16px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                              <div style={{ width: '36px', height: '36px', borderRadius: '50%',
                                            background: C.red, flexShrink: 0,
                                            display: 'flex', alignItems: 'center', justifyContent: 'center',
                                            fontSize: '12px', fontWeight: 700, color: '#fff' }}>
                                {initials(c.legal_name)}
                              </div>
                              <div>
                                <div style={{ fontWeight: 600, color: C.text }}>{c.legal_name}</div>
                                <div style={{ fontSize: '11px', color: C.muted }}>{c.cif_number}</div>
                              </div>
                            </div>
                          </td>
                          {/* Segment */}
                          <td style={{ padding: '12px 16px' }}>
                            <Badge label={seg.label} bg={seg.bg} text={seg.text} />
                          </td>
                          {/* State */}
                          <td style={{ padding: '12px 16px', color: C.muted, fontSize: '12px' }}>
                            {c.primary_state || '—'}
                          </td>
                          {/* CTOS */}
                          <td style={{ padding: '12px 16px' }}>
                            <span style={{ fontWeight: 700, color: ctosColor(c.ctos_score, C) }}>
                              {c.ctos_score || '—'}
                            </span>
                          </td>
                          {/* Balance */}
                          <td style={{ padding: '12px 16px', fontWeight: 600, color: C.text, whiteSpace: 'nowrap' }}>
                            {fmtMYR(c.total_deposit_balance_myr)}
                          </td>
                          {/* Tenure */}
                          <td style={{ padding: '12px 16px', color: C.muted }}>
                            {c.relationship_tenure_years ? `${parseFloat(c.relationship_tenure_years).toFixed(1)} yrs` : '—'}
                          </td>
                          {/* Recommendation */}
                          <td style={{ padding: '12px 16px' }}>
                            {c.recommendation_1 && c.recommendation_1 !== 'PENDING' ? (
                              <div>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '5px',
                                              fontWeight: 600, color: prod.color, fontSize: '12px' }}>
                                  <span>{prod.icon}</span>
                                  <span>{prod.label}</span>
                                </div>
                                {conf > 0 && (
                                  <div style={{ marginTop: '4px' }}>
                                    <div style={{ background: C.border, borderRadius: '3px', height: '3px', width: '80px' }}>
                                      <div style={{ width: `${Math.min(conf,100)}%`, background: prod.color,
                                                    height: '3px', borderRadius: '3px' }} />
                                    </div>
                                    <div style={{ fontSize: '10px', color: C.muted, marginTop: '1px' }}>{Math.round(conf)}%</div>
                                  </div>
                                )}
                              </div>
                            ) : (
                              <span style={{ color: C.subtle, fontSize: '12px' }}>—</span>
                            )}
                          </td>
                          {/* Action */}
                          <td style={{ padding: '12px 16px' }}>
                            <button onClick={() => openDetail(c.party_id)} style={{
                              padding: '6px 14px', borderRadius: '7px', border: 'none',
                              background: C.red, color: '#fff', fontSize: '12px', fontWeight: 600,
                              cursor: 'pointer', whiteSpace: 'nowrap',
                            }}>View 360 →</button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center',
                              gap: '8px', padding: '16px', borderTop: `1px solid ${C.tableBorder}` }}>
                  <button onClick={() => setPage_(p => Math.max(1, p-1))} disabled={page_===1}
                    style={{ padding: '5px 12px', borderRadius: '6px', border: `1px solid ${C.border}`,
                             background: C.card, color: C.text, cursor: page_===1?'not-allowed':'pointer',
                             opacity: page_===1?0.4:1, fontSize: '13px' }}>← Prev</button>
                  <span style={{ color: C.muted, fontSize: '13px' }}>
                    Page {page_} of {totalPages} · {filtered.length} customers
                  </span>
                  <button onClick={() => setPage_(p => Math.min(totalPages, p+1))} disabled={page_===totalPages}
                    style={{ padding: '5px 12px', borderRadius: '6px', border: `1px solid ${C.border}`,
                             background: C.card, color: C.text, cursor: page_===totalPages?'not-allowed':'pointer',
                             opacity: page_===totalPages?0.4:1, fontSize: '13px' }}>Next →</button>
                </div>
              )}
            </div>
          </>
        )}

        {/* ── DETAIL PAGE ───────────────────────────────────────────────── */}
        {page === 'detail' && (
          <>
            {/* Breadcrumb */}
            <div style={{ marginBottom: '20px', display: 'flex', alignItems: 'center', gap: '8px' }}>
              <button onClick={() => setPage('overview')} style={{
                background: 'none', border: 'none', color: C.red, cursor: 'pointer',
                fontSize: '14px', fontWeight: 600, padding: 0, display: 'flex', alignItems: 'center', gap: '4px',
              }}>← Customer Overview</button>
              {customer && (
                <><span style={{ color: C.muted }}>›</span>
                <span style={{ color: C.muted, fontSize: '14px' }}>{customer.legal_name}</span></>
              )}
            </div>

            {loadingDetail && (
              <div style={{ textAlign: 'center', padding: '80px', color: C.muted }}>
                <div style={{ fontSize: '32px', marginBottom: '12px' }}>⏳</div>
                Loading customer 360…
              </div>
            )}

            {!loadingDetail && customer && (
              <>
                {/* Hero Card */}
                <div style={{ ...cardStyle, marginBottom: '20px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start',
                                gap: '20px', flexWrap: 'wrap' }}>
                    {/* Identity */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
                      <div style={{ width: '64px', height: '64px', borderRadius: '50%', background: C.red,
                                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                                    fontSize: '22px', fontWeight: 800, color: '#fff', flexShrink: 0 }}>
                        {initials(customer.legal_name)}
                      </div>
                      <div>
                        <div style={{ fontSize: '22px', fontWeight: 800, color: C.text, marginBottom: '4px' }}>
                          {customer.legal_name}
                        </div>
                        <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap', alignItems: 'center' }}>
                          <span style={{ fontSize: '12px', color: C.muted }}>{customer.cif_number}</span>
                          <span style={{ fontSize: '12px', color: C.muted }}>·</span>
                          <span style={{ fontSize: '12px', color: C.muted }}>{customer.party_id}</span>
                          <Badge label={customer.lifecycle_status || 'unknown'}
                                 bg={customer.lifecycle_status === 'active' ? '#DCFCE7' : '#FEE2E2'}
                                 text={customer.lifecycle_status === 'active' ? '#16A34A' : '#DC2626'} />
                          <Badge label={`KYC: ${customer.kyc_status || '—'}`}
                                 bg={customer.kyc_status === 'verified' ? '#DBEAFE' : '#FEF9C3'}
                                 text={customer.kyc_status === 'verified' ? '#1D4ED8' : '#A16207'} />
                          {(() => { const s = getSeg(customer.lifestyle_segment); return <Badge label={s.label} bg={s.bg} text={s.text} />; })()}
                        </div>
                      </div>
                    </div>
                    {/* KPI tiles */}
                    <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
                      {[
                        { label: 'Total Balance', value: fmtMYRFull(customer.total_deposit_balance_myr) },
                        { label: 'CTOS Score', value: customer.ctos_score ? `${customer.ctos_score}/850` : '—',
                          color: ctosColor(customer.ctos_score, C) },
                        { label: 'Tenure', value: customer.relationship_tenure_years ? `${parseFloat(customer.relationship_tenure_years).toFixed(1)} yrs` : '—' },
                        { label: 'Digital Score', value: customer.digital_maturity_score ? `${customer.digital_maturity_score}/10` : '—' },
                      ].map(k => (
                        <div key={k.label} style={{ background: C.bg, border: `1px solid ${C.border}`,
                                                     borderRadius: '10px', padding: '12px 16px', minWidth: '130px' }}>
                          <div style={{ fontSize: '10px', color: C.muted, fontWeight: 600, textTransform: 'uppercase',
                                        letterSpacing: '0.06em', marginBottom: '4px' }}>{k.label}</div>
                          <div style={{ fontSize: '18px', fontWeight: 800, color: k.color || C.text }}>{k.value}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>

                {/* Body */}
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 380px', gap: '20px',
                              alignItems: 'start' }}>

                  {/* LEFT: 360 Tabs */}
                  <div style={cardStyle}>
                    {/* Tab bar */}
                    <div style={{ display: 'flex', gap: '4px', marginBottom: '20px',
                                  padding: '4px', background: C.bg, borderRadius: '10px', width: 'fit-content' }}>
                      {['Profile','Financial','Credit','Digital'].map(t => (
                        <Tab key={t} label={t} active={detailTab===t} onClick={() => setDetailTab(t)} C={C} />
                      ))}
                    </div>

                    {/* Profile Tab */}
                    {detailTab === 'Profile' && (
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
                        <div>
                          <div style={{ fontSize: '12px', fontWeight: 700, color: C.muted, textTransform: 'uppercase',
                                        letterSpacing: '0.06em', marginBottom: '12px' }}>Personal</div>
                          <InfoRow label="Age" value={customer.age ? `${customer.age} years` : '—'} C={C} />
                          <InfoRow label="Gender" value={customer.gender} C={C} />
                          <InfoRow label="Marital Status" value={customer.marital_status} C={C} />
                          <InfoRow label="Dependents" value={customer.number_of_dependents} C={C} />
                          <InfoRow label="Education" value={customer.education_level} C={C} />
                          <InfoRow label="Employment" value={customer.employment_status} C={C} />
                          <InfoRow label="Employer" value={customer.employer_name} C={C} />
                          <InfoRow label="Annual Income" value={fmtMYRFull(customer.annual_income_amount)} C={C} />
                          <InfoRow label="Net Worth Band" value={customer.net_worth_band} C={C} />
                          <InfoRow label="Shariah Preferred" value={customer.is_shariah_preferred ? 'Yes ✓' : 'No'} C={C} />
                        </div>
                        <div>
                          <div style={{ fontSize: '12px', fontWeight: 700, color: C.muted, textTransform: 'uppercase',
                                        letterSpacing: '0.06em', marginBottom: '12px' }}>Relationship</div>
                          <InfoRow label="Primary State" value={customer.primary_state} C={C} />
                          <InfoRow label="Language" value={customer.preferred_language_code} C={C} />
                          <InfoRow label="Contact Method" value={customer.preferred_contact_method} C={C} />
                          <InfoRow label="Tenure" value={customer.relationship_tenure_years ? `${parseFloat(customer.relationship_tenure_years).toFixed(1)} years` : '—'} C={C} />
                          <InfoRow label="Lifecycle" value={customer.lifecycle_status} C={C} />
                          <InfoRow label="Risk Rating" value={customer.risk_rating} C={C} />
                          <InfoRow label="KYC Status" value={customer.kyc_status} C={C} />
                          <InfoRow label="Mobile App User" value={customer.mobile_app_user_flag ? 'Yes' : 'No'} C={C} />
                          <InfoRow label="NPS Score" value={customer.nps_score} C={C} />
                          <InfoRow label="Marketing Consent" value={customer.marketing_consent_flag ? 'Yes' : 'No'} C={C} />
                        </div>
                      </div>
                    )}

                    {/* Financial Tab */}
                    {detailTab === 'Financial' && (
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
                        <div>
                          <div style={{ fontSize: '12px', fontWeight: 700, color: C.muted, textTransform: 'uppercase',
                                        letterSpacing: '0.06em', marginBottom: '12px' }}>Deposit Accounts</div>
                          <InfoRow label="Num. Accounts" value={customer.num_accounts} C={C} />
                          <InfoRow label="Total Balance" value={fmtMYRFull(customer.total_deposit_balance_myr)} C={C} />
                          <InfoRow label="Has Current Account" value={customer.has_current_account ? 'Yes' : 'No'} C={C} />
                          <InfoRow label="Has Savings Account" value={customer.has_savings_account ? 'Yes' : 'No'} C={C} />
                          <InfoRow label="Has Fixed Deposit" value={customer.has_fixed_deposit ? 'Yes' : 'No'} C={C} />
                          <InfoRow label="Has Credit Card" value={customer.has_credit_card ? 'Yes' : 'No'} C={C} />
                          <div style={{ fontSize: '12px', fontWeight: 700, color: C.muted, textTransform: 'uppercase',
                                        letterSpacing: '0.06em', margin: '16px 0 12px' }}>Loans & Facilities</div>
                          <InfoRow label="Num. Loan Facilities" value={customer.num_loan_facilities} C={C} />
                          <InfoRow label="Total Loan Outstanding" value={fmtMYRFull(customer.total_loan_outstanding_myr)} C={C} />
                          <InfoRow label="Monthly Commitment" value={fmtMYRFull(customer.monthly_loan_commitment_myr)} C={C} />
                          <InfoRow label="Has Home Loan" value={customer.has_home_loan ? 'Yes' : 'No'} C={C} />
                          <InfoRow label="Has Personal Loan" value={customer.has_personal_loan ? 'Yes' : 'No'} C={C} />
                          <InfoRow label="Has Hire Purchase" value={customer.has_hire_purchase ? 'Yes' : 'No'} C={C} />
                        </div>
                        <div>
                          <div style={{ fontSize: '12px', fontWeight: 700, color: C.muted, textTransform: 'uppercase',
                                        letterSpacing: '0.06em', marginBottom: '12px' }}>Transactions (30 days)</div>
                          <InfoRow label="Transaction Count" value={customer.txn_count_30d} C={C} />
                          <InfoRow label="Avg Transaction" value={fmtMYRFull(customer.avg_txn_amount_myr)} C={C} />
                          <InfoRow label="Total Debit" value={fmtMYRFull(customer.total_debit_30d_myr)} C={C} />
                          <InfoRow label="Total Credit" value={fmtMYRFull(customer.total_credit_30d_myr)} C={C} />
                          <InfoRow label="Card Spend" value={fmtMYRFull(customer.card_spend_30d_myr)} C={C} />
                          <InfoRow label="Overseas Transactions" value={customer.overseas_txn_flag ? 'Yes' : 'No'} C={C} />
                        </div>
                      </div>
                    )}

                    {/* Credit Tab */}
                    {detailTab === 'Credit' && (
                      <div>
                        {/* CTOS Score Gauge */}
                        <div style={{ marginBottom: '20px', padding: '16px', background: C.bg,
                                      borderRadius: '10px', border: `1px solid ${C.border}` }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                            <span style={{ fontWeight: 700, fontSize: '14px', color: C.text }}>CTOS Score</span>
                            <span style={{ fontWeight: 800, fontSize: '22px',
                                           color: ctosColor(customer.ctos_score, C) }}>
                              {customer.ctos_score || '—'}<span style={{ fontSize: '14px', fontWeight: 500, color: C.muted }}>/850</span>
                            </span>
                          </div>
                          <div style={{ background: C.border, borderRadius: '4px', height: '8px', position: 'relative' }}>
                            <div style={{ position: 'absolute', left: 0, height: '100%', borderRadius: '4px',
                                          width: `${Math.min(100, Math.max(0, ((parseInt(customer.ctos_score)||0)-300)/5.5))}%`,
                                          background: ctosColor(customer.ctos_score, C),
                                          transition: 'width 1s ease' }} />
                          </div>
                          <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '4px',
                                        fontSize: '10px', color: C.muted }}>
                            <span>300 Poor</span><span>600 Fair</span><span>700 Good</span><span>850 Excellent</span>
                          </div>
                        </div>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
                          <div>
                            <InfoRow label="CCRIS Status" value={customer.ccris_status} C={C} />
                            <InfoRow label="Payment Conduct 12M" value={customer.payment_conduct_12m} C={C} />
                            <InfoRow label="Total Credit Facilities" value={customer.total_credit_facilities} C={C} />
                            <InfoRow label="Credit Bureau Count" value={customer.credit_card_bureau_count} C={C} />
                            <InfoRow label="Inquiry Count (6M)" value={customer.inquiry_count_last_6m} C={C} />
                          </div>
                          <div>
                            <InfoRow label="Legal Cases" value={customer.legal_cases_count} C={C} />
                            <InfoRow label="Bankruptcy Status" value={customer.bankruptcy_status} C={C} />
                            <InfoRow label="KB Monthly Commitment" value={fmtMYRFull(customer.kb_monthly_commitment_myr)} C={C} />
                            <InfoRow label="Debt-to-Income Ratio"
                              value={customer.annual_income_amount && customer.monthly_loan_commitment_myr
                                ? `${((parseFloat(customer.monthly_loan_commitment_myr)*12/parseFloat(customer.annual_income_amount))*100).toFixed(1)}%`
                                : '—'} C={C} />
                            <InfoRow label="Is PEP" value={customer.is_pep ? 'Yes ⚠️' : 'No'} C={C} />
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Digital Tab */}
                    {detailTab === 'Digital' && (
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '24px' }}>
                        <div>
                          <div style={{ fontSize: '12px', fontWeight: 700, color: C.muted, textTransform: 'uppercase',
                                        letterSpacing: '0.06em', marginBottom: '12px' }}>Digital Banking</div>
                          <InfoRow label="Digital Enrollment" value={customer.digital_banking_enrollment_flag ? 'Yes ✓' : 'No'} C={C} />
                          <InfoRow label="Mobile App User" value={customer.mobile_app_user_flag ? 'Yes ✓' : 'No'} C={C} />
                          <InfoRow label="Mobile Sessions (30d)" value={customer.mobile_sessions_30d} C={C} />
                          <InfoRow label="Product Views (30d)" value={customer.product_views_last_30d} C={C} />
                          <InfoRow label="Last Product Viewed" value={customer.last_product_category_viewed} C={C} />
                          {/* Digital score bar */}
                          <div style={{ marginTop: '16px' }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '6px' }}>
                              <span style={{ fontSize: '12px', color: C.muted }}>Digital Maturity Score</span>
                              <span style={{ fontSize: '13px', fontWeight: 700, color: C.text }}>
                                {customer.digital_maturity_score || '—'}/10
                              </span>
                            </div>
                            <div style={{ background: C.border, borderRadius: '4px', height: '6px' }}>
                              <div style={{ width: `${(parseFloat(customer.digital_maturity_score)||0)*10}%`,
                                            background: C.blue, height: '6px', borderRadius: '4px', transition: 'width 0.8s' }} />
                            </div>
                          </div>
                        </div>
                        <div>
                          <div style={{ fontSize: '12px', fontWeight: 700, color: C.muted, textTransform: 'uppercase',
                                        letterSpacing: '0.06em', marginBottom: '12px' }}>Telco Signals</div>
                          <InfoRow label="Telco ARPU" value={fmtMYRFull(customer.telco_arpu_myr)} C={C} />
                          <InfoRow label="Data Consumption" value={customer.telco_data_consumption_gb_monthly ? `${parseFloat(customer.telco_data_consumption_gb_monthly).toFixed(1)} GB/mo` : '—'} C={C} />
                          <InfoRow label="Digital Activity Score" value={customer.digital_activity_score} C={C} />
                        </div>
                      </div>
                    )}
                  </div>

                  {/* RIGHT: AI Recommendations */}
                  <div style={{ ...cardStyle, position: 'sticky', top: '72px' }}>
                    <div style={{ fontWeight: 700, fontSize: '15px', color: C.text, marginBottom: '2px' }}>
                      🎯 AI Recommendations
                    </div>
                    <div style={{ fontSize: '11px', color: C.muted, marginBottom: '16px' }}>
                      {recs?.live ? '⚡ Live · XGBoost model serving' : '🗄 Batch · pre-scored by XGBoost'}
                    </div>

                    {recs ? (
                      <>
                        {/* Rec cards */}
                        {[
                          { rank: 1, product: recs.recommendation_1, conf: parseFloat(recs.confidence_1)||0 },
                          { rank: 2, product: recs.recommendation_2, conf: parseFloat(recs.confidence_2)||0 },
                        ].map(({ rank, product, conf }) => {
                          const p = getProd(product);
                          return (
                            <div key={rank} style={{
                              border: `2px solid ${rank===1 ? C.red : C.border}`,
                              borderRadius: '12px', padding: '14px', marginBottom: '10px',
                              background: rank===1 ? (C === DARK ? '#1F1515' : '#FFF8F7') : C.bg,
                            }}>
                              <div style={{ display: 'flex', justifyContent: 'space-between',
                                            alignItems: 'flex-start', marginBottom: '8px' }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                                  <span style={{ fontSize: '28px' }}>{p.icon}</span>
                                  <div>
                                    <div style={{ fontWeight: 700, fontSize: '14px', color: C.text }}>{p.label}</div>
                                    <div style={{ fontSize: '10px', color: C.muted }}>#{rank} Recommendation</div>
                                  </div>
                                </div>
                                <div style={{ textAlign: 'right' }}>
                                  <div style={{ fontSize: '24px', fontWeight: 800, color: p.color, lineHeight: 1 }}>
                                    {Math.round(conf)}%
                                  </div>
                                  <div style={{ fontSize: '10px', color: C.muted }}>confidence</div>
                                </div>
                              </div>
                              <div style={{ background: C.border, borderRadius: '4px', height: '4px', marginBottom: '10px' }}>
                                <div style={{ width: `${Math.min(conf,100)}%`, background: p.color,
                                              height: '4px', borderRadius: '4px', transition: 'width 0.8s' }} />
                              </div>
                              <button onClick={() => explainRec(rank as 1|2)} style={{
                                fontSize: '11px', padding: '5px 12px', borderRadius: '6px',
                                border: `1px solid ${C.border}`, background: 'transparent',
                                color: C.text, cursor: 'pointer', fontWeight: 600,
                              }}>🤖 Why this product?</button>
                            </div>
                          );
                        })}

                        {liveError && (
                          <div style={{ fontSize: '12px', color: C.crimson, padding: '8px 12px',
                                        background: '#FEF2F2', borderRadius: '6px', marginBottom: '10px' }}>
                            {liveError}
                          </div>
                        )}

                        {/* Action buttons */}
                        <div style={{ display: 'flex', gap: '8px', marginTop: '4px' }}>
                          <button onClick={liveScore} disabled={liveLoading} style={{
                            flex: 1, padding: '9px', borderRadius: '8px',
                            border: `1px solid ${C.border}`, background: 'transparent',
                            color: C.text, cursor: liveLoading ? 'wait' : 'pointer',
                            fontSize: '13px', fontWeight: 600,
                          }}>{liveLoading ? '⏳ Scoring…' : '⚡ Live Score'}</button>
                          <button onClick={draftEmail} style={{
                            flex: 1, padding: '9px', borderRadius: '8px', border: 'none',
                            background: C.red, color: '#fff', cursor: 'pointer',
                            fontSize: '13px', fontWeight: 600,
                          }}>✉️ Draft Email</button>
                        </div>
                      </>
                    ) : (
                      <div style={{ padding: '24px', textAlign: 'center', color: C.muted, fontSize: '13px' }}>
                        <div style={{ fontSize: '32px', marginBottom: '8px' }}>⏳</div>
                        Recommendations not yet available.<br />
                        <span style={{ fontSize: '12px' }}>Run Part B pipeline to generate batch scores,<br />or click Live Score below.</span>
                        <div style={{ marginTop: '16px' }}>
                          <button onClick={liveScore} disabled={liveLoading} style={{
                            padding: '9px 20px', borderRadius: '8px', border: 'none',
                            background: C.red, color: '#fff', cursor: liveLoading ? 'wait' : 'pointer',
                            fontSize: '13px', fontWeight: 600,
                          }}>{liveLoading ? '⏳ Scoring…' : '⚡ Score Now'}</button>
                        </div>
                        {liveError && <div style={{ color: C.crimson, fontSize: '12px', marginTop: '8px' }}>{liveError}</div>}
                      </div>
                    )}

                    {/* GLM badge */}
                    <div style={{ marginTop: '12px', padding: '8px', borderRadius: '6px',
                                  background: C.bg, border: `1px solid ${C.border}`, fontSize: '11px',
                                  color: C.muted, textAlign: 'center' }}>
                      Powered by Databricks · XGBoost + GLM 5.2 via FMAPI
                    </div>
                  </div>
                </div>
              </>
            )}
          </>
        )}
      </main>

      {/* ════════════════ EMAIL MODAL ════════════════════════════════════════ */}
      {showEmail && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)',
                      display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
          <div style={{ background: C.card, borderRadius: '16px', padding: '28px', width: '540px',
                        maxWidth: '90vw', boxShadow: '0 24px 60px rgba(0,0,0,0.25)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '16px' }}>
              <div>
                <div style={{ fontWeight: 700, fontSize: '16px', color: C.text }}>✉️ Draft Personalised Email</div>
                <div style={{ fontSize: '11px', color: C.muted, marginTop: '2px' }}>GLM 5.2 · Unity AI Gateway</div>
              </div>
              <button onClick={() => { setShowEmail(false); setEmailDraft(''); }}
                style={{ background: 'none', border: 'none', fontSize: '20px', cursor: 'pointer', color: C.muted }}>✕</button>
            </div>
            {customer && recs && (
              <div style={{ display: 'flex', gap: '8px', marginBottom: '14px', flexWrap: 'wrap' }}>
                <Badge label={customer.legal_name} bg={C.bg} text={C.text} />
                <Badge label={getProd(recs.recommendation_1).label} bg={C.blueLight} text={C.navy} />
                <Badge label={`${Math.round(parseFloat(recs.confidence_1)||0)}% confidence`} bg={C.bg} text={C.muted} />
              </div>
            )}
            <textarea
              value={emailLoading ? '⏳ Generating with GLM 5.2…' : emailDraft}
              onChange={e => setEmailDraft(e.target.value)}
              readOnly={emailLoading}
              rows={9}
              style={{ width: '100%', padding: '12px', borderRadius: '8px', border: `1px solid ${C.border}`,
                       background: C.bg, color: C.text, fontSize: '13px', lineHeight: 1.6,
                       resize: 'vertical', outline: 'none', boxSizing: 'border-box', fontFamily: 'inherit' }}
            />
            <div style={{ fontSize: '11px', color: C.muted, marginTop: '6px', fontStyle: 'italic' }}>
              AI-generated via Unity AI Gateway · review before sending
            </div>
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px', marginTop: '16px' }}>
              <button onClick={() => { setShowEmail(false); setEmailDraft(''); }}
                style={{ padding: '8px 16px', border: `1px solid ${C.border}`, borderRadius: '8px',
                         background: 'transparent', color: C.text, cursor: 'pointer', fontSize: '13px' }}>Close</button>
              {emailDraft && !emailLoading && (
                <button onClick={() => navigator.clipboard.writeText(emailDraft)}
                  style={{ padding: '8px 16px', background: C.red, color: '#fff', border: 'none',
                           borderRadius: '8px', cursor: 'pointer', fontWeight: 600, fontSize: '13px' }}>📋 Copy</button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* ════════════════ EXPLAIN MODAL ══════════════════════════════════════ */}
      {showExplain !== null && (
        <div style={{ position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.6)',
                      display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 }}>
          <div style={{ background: C.card, borderRadius: '16px', padding: '28px', width: '500px',
                        maxWidth: '90vw', boxShadow: '0 24px 60px rgba(0,0,0,0.25)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '16px' }}>
              <div>
                <div style={{ fontWeight: 700, fontSize: '16px', color: C.text }}>🤖 Why This Product?</div>
                <div style={{ fontSize: '11px', color: C.muted, marginTop: '2px' }}>FMAPI · GLM 5.2 · Unity AI Gateway</div>
              </div>
              <button onClick={() => { setShowExplain(null); setExplanation(''); }}
                style={{ background: 'none', border: 'none', fontSize: '20px', cursor: 'pointer', color: C.muted }}>✕</button>
            </div>
            {recs && showExplain && (() => {
              const product = showExplain === 1 ? recs.recommendation_1 : recs.recommendation_2;
              const conf    = showExplain === 1 ? recs.confidence_1 : recs.confidence_2;
              const p = getProd(product);
              return (
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px', padding: '12px 16px',
                              borderRadius: '10px', background: C.bg, border: `1px solid ${C.border}`,
                              marginBottom: '16px' }}>
                  <span style={{ fontSize: '32px' }}>{p.icon}</span>
                  <div>
                    <div style={{ fontWeight: 700, color: C.text }}>{p.label}</div>
                    <div style={{ fontSize: '12px', color: C.muted }}>{Math.round(parseFloat(conf)||0)}% confidence · #{showExplain} recommendation</div>
                  </div>
                </div>
              );
            })()}
            <div style={{ minHeight: '80px', padding: '14px', borderRadius: '10px',
                          background: C.bg, border: `1px solid ${C.border}`,
                          fontSize: '14px', lineHeight: 1.7, color: C.text }}>
              {explainLoading ? (
                <span style={{ color: C.muted, fontStyle: 'italic' }}>
                  ⏳ GLM 5.2 is analysing {customer?.legal_name}'s profile…
                </span>
              ) : explanation || <span style={{ color: C.muted, fontStyle: 'italic' }}>Click Why to generate.</span>}
            </div>
            <div style={{ fontSize: '11px', color: C.muted, marginTop: '8px', fontStyle: 'italic' }}>
              ✓ Every explanation logged for compliance audit via Unity AI Gateway
            </div>
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '8px', marginTop: '16px' }}>
              <button onClick={() => { setShowExplain(null); setExplanation(''); }}
                style={{ padding: '8px 16px', border: `1px solid ${C.border}`, borderRadius: '8px',
                         background: 'transparent', color: C.text, cursor: 'pointer', fontSize: '13px' }}>Close</button>
              {explanation && (
                <button onClick={() => navigator.clipboard.writeText(explanation)}
                  style={{ padding: '8px 16px', background: C.red, color: '#fff', border: 'none',
                           borderRadius: '8px', cursor: 'pointer', fontWeight: 600, fontSize: '13px' }}>📋 Copy</button>
              )}
            </div>
          </div>
        </div>
      )}

      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Inter', system-ui, sans-serif; }
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #CBD5E1; border-radius: 3px; }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.5} }
      `}</style>
    </div>
  );
}
