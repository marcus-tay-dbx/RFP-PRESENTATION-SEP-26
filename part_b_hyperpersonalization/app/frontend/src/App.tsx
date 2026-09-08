import React, { useState, useEffect, useRef } from 'react';

// ── Palette & constants ───────────────────────────────────────────────────────
const C = {
  navy:    '#1B3A6B',
  navyD:   '#122848',
  red:     '#C8102E',
  white:   '#FFFFFF',
  bg:      '#F5F7FA',
  bg2:     '#EDF1F7',
  g50:     '#F8FAFC',
  g100:    '#F1F5F9',
  g200:    '#E2E8F0',
  g400:    '#94A3B8',
  g600:    '#475569',
  g900:    '#0F172A',
};

type SegKey = 'mass_market' | 'affluent' | 'high_net_worth' | 'private_banking' | 'premier';
type ProdKey = 'CREDIT_CARD' | 'HOME_LOAN' | 'PERSONAL_LOAN' | 'INVESTMENT' | 'INSURANCE' | 'NO_ACTION';

const SEGMENTS: Record<SegKey, { color: string; label: string; bg: string; textColor: string }> = {
  mass_market:     { color: '#64748B', label: 'Mass Market',     bg: '#F1F5F9', textColor: '#334155' },
  affluent:        { color: '#1E40AF', label: 'Affluent',        bg: '#DBEAFE', textColor: '#1E40AF' },
  high_net_worth:  { color: '#92400E', label: 'High Net Worth',  bg: '#FEF3C7', textColor: '#92400E' },
  private_banking: { color: '#FFFFFF', label: 'Private Banking', bg: '#1B3A6B', textColor: '#FFFFFF' },
  premier:         { color: '#6D28D9', label: 'Premier',         bg: '#EDE9FE', textColor: '#6D28D9' },
};

const PRODUCTS: Record<ProdKey, { icon: string; label: string; color: string }> = {
  CREDIT_CARD:   { icon: '💳', label: 'Credit Card',    color: '#1B3A6B' },
  HOME_LOAN:     { icon: '🏠', label: 'Home Financing', color: '#047857' },
  PERSONAL_LOAN: { icon: '💰', label: 'Personal Loan',  color: '#0369A1' },
  INVESTMENT:    { icon: '📈', label: 'Investment',     color: '#7C3AED' },
  INSURANCE:     { icon: '🛡️', label: 'Takaful',        color: '#B45309' },
  NO_ACTION:     { icon: '✓',  label: 'No Action',      color: '#64748B' },
};

const fmtMYR = (v: number) =>
  `MYR ${v.toLocaleString('en-MY', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}`;
const fmtK = (v: number) =>
  `MYR ${(v / 1000).toLocaleString('en-MY', { minimumFractionDigits: 0, maximumFractionDigits: 0 })}K`;
const ctosColor = (score: number) =>
  score >= 700 ? '#059669' : score >= 600 ? '#D97706' : '#DC2626';

function getSeg(key: string) {
  return SEGMENTS[(key as SegKey)] ?? SEGMENTS.mass_market;
}
function getProd(key: string) {
  return PRODUCTS[(key as ProdKey)] ?? PRODUCTS.NO_ACTION;
}

// ── Tiny shared components ────────────────────────────────────────────────────
const Row = ({ label, value, valueColor }: { label: string; value: React.ReactNode; valueColor?: string }) => (
  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '9px 12px', background: C.g50, borderRadius: '8px' }}>
    <span style={{ fontSize: '12px', color: C.g600 }}>{label}</span>
    <span style={{ fontSize: '13px', fontWeight: 600, color: valueColor || C.g900 }}>{value ?? '—'}</span>
  </div>
);

// ── Main App ──────────────────────────────────────────────────────────────────
export default function App() {
  const [customers, setCustomers]       = useState<any[]>([]);
  const [searchQuery, setSearchQuery]   = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [searching, setSearching]       = useState(false);
  const [selectedId, setSelectedId]     = useState<string | null>(null);
  const [customer, setCustomer]         = useState<any>(null);
  const [recs, setRecs]                 = useState<any>(null);
  const [loading, setLoading]           = useState(false);
  const [activeTab, setActiveTab]       = useState('Profile');
  const [liveLoading, setLiveLoading]   = useState(false);
  const [liveError, setLiveError]       = useState('');
  const [showEmail, setShowEmail]       = useState(false);
  const [emailDraft, setEmailDraft]     = useState('');
  const [emailLoading, setEmailLoading] = useState(false);
  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Load top 20 customers on mount
  useEffect(() => {
    fetch('/api/customers')
      .then(r => r.json())
      .then(setCustomers)
      .catch(() => {});
  }, []);

  // Debounced search
  const handleSearch = (q: string) => {
    setSearchQuery(q);
    if (searchTimer.current) clearTimeout(searchTimer.current);
    if (q.length < 2) { setSearchResults([]); return; }
    setSearching(true);
    searchTimer.current = setTimeout(async () => {
      try {
        const res = await fetch(`/api/customers/search?q=${encodeURIComponent(q)}&limit=10`);
        setSearchResults(await res.json());
      } catch {
        setSearchResults([]);
      } finally {
        setSearching(false);
      }
    }, 300);
  };

  const loadCustomer = async (id: string) => {
    if (id === selectedId) return;
    setSelectedId(id);
    setSearchQuery('');
    setSearchResults([]);
    setCustomer(null);
    setRecs(null);
    setEmailDraft('');
    setLiveError('');
    setActiveTab('Profile');
    setLoading(true);
    try {
      const [c, r] = await Promise.all([
        fetch(`/api/customer/${id}`).then(res => res.json()),
        fetch(`/api/customer/${id}/recommendations`).then(res => res.json()),
      ]);
      setCustomer(c);
      setRecs(r);
    } catch {
      // keep loading state visible — customer may partially load
    } finally {
      setLoading(false);
    }
  };

  const liveScore = async () => {
    if (!selectedId) return;
    setLiveLoading(true);
    setLiveError('');
    try {
      const res = await fetch(`/api/customer/${selectedId}/recommend-live`, { method: 'POST' });
      if (!res.ok) throw new Error(`${res.status}`);
      setRecs(await res.json());
    } catch (e: any) {
      setLiveError('Live scoring failed — check endpoint availability');
    } finally {
      setLiveLoading(false);
    }
  };

  const draftEmail = async () => {
    if (!customer || !recs) return;
    setShowEmail(true);
    setEmailDraft('');
    setEmailLoading(true);
    try {
      const res = await fetch('/api/draft-email', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          party_id:      customer.party_id,
          product:       recs.recommendation_1,
          confidence:    recs.confidence_1,
          customer_name: customer.legal_name,
          segment:       customer.lifestyle_segment,
          tenure:        customer.relationship_tenure_years,
        }),
      });
      const data = await res.json();
      setEmailDraft(data.email_draft || '(No content returned)');
    } catch {
      setEmailDraft('Error generating email. Please try again.');
    } finally {
      setEmailLoading(false);
    }
  };

  const displayList = searchQuery.length >= 2 ? searchResults : customers;
  const seg = customer ? getSeg(customer.lifestyle_segment) : null;

  // ── Render ──────────────────────────────────────────────────────────────────
  return (
    <div style={{ display: 'flex', height: '100vh', fontFamily: "'Inter', 'system-ui', sans-serif",
                  background: C.bg, overflow: 'hidden' }}>

      {/* ════════════════ LEFT SIDEBAR ═══════════════════════════════════════ */}
      <aside style={{ width: '280px', minWidth: '280px', background: C.navy, color: C.white,
                      display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>

        {/* Logo */}
        <div style={{ padding: '18px 16px 14px',
                      borderBottom: '1px solid rgba(255,255,255,0.1)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
            <div style={{ width: '36px', height: '36px', background: C.red, borderRadius: '8px',
                          display: 'flex', alignItems: 'center', justifyContent: 'center',
                          fontWeight: 900, fontSize: '16px', letterSpacing: '-1px',
                          flexShrink: 0 }}>
              Ab
            </div>
            <div>
              <div style={{ fontWeight: 800, fontSize: '15px', letterSpacing: '0.2px',
                            lineHeight: '1.1' }}>
                Alliance Bank
              </div>
              <div style={{ fontSize: '9.5px', opacity: 0.55, letterSpacing: '0.8px',
                            textTransform: 'uppercase', marginTop: '1px' }}>
                Intelligence Hub
              </div>
            </div>
          </div>
        </div>

        {/* Search */}
        <div style={{ padding: '12px 14px 8px' }}>
          <input
            value={searchQuery}
            onChange={e => handleSearch(e.target.value)}
            placeholder="Search name or CIF…"
            style={{ width: '100%', padding: '8px 14px', borderRadius: '20px', border: 'none',
                     background: 'rgba(255,255,255,0.13)', color: C.white, fontSize: '13px',
                     outline: 'none', boxSizing: 'border-box',
                     caretColor: C.white }}
          />
        </div>

        {/* Customer list */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '4px 8px 12px' }}>
          {searching && (
            <div style={{ textAlign: 'center', padding: '16px', opacity: 0.5, fontSize: '12px' }}>
              Searching…
            </div>
          )}
          {!searching && searchQuery.length >= 2 && searchResults.length === 0 && (
            <div style={{ textAlign: 'center', padding: '16px', opacity: 0.5, fontSize: '12px' }}>
              No results found
            </div>
          )}

          {displayList.map(c => {
            const s = getSeg(c.lifestyle_segment);
            const isActive = c.party_id === selectedId;
            return (
              <div
                key={c.party_id}
                onClick={() => loadCustomer(c.party_id)}
                style={{
                  padding: '9px 10px',
                  borderRadius: '8px',
                  cursor: 'pointer',
                  marginBottom: '2px',
                  background: isActive ? 'rgba(255,255,255,0.16)' : 'transparent',
                  borderLeft: isActive ? `3px solid ${C.red}` : '3px solid transparent',
                  transition: 'background 0.15s',
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center',
                              justifyContent: 'space-between', gap: '6px' }}>
                  <div style={{ fontSize: '13px', fontWeight: 600,
                                overflow: 'hidden', textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap', flex: 1 }}>
                    {c.legal_name}
                  </div>
                  <span style={{ fontSize: '10px', padding: '2px 7px', borderRadius: '10px',
                                 background: s.bg, color: s.textColor, whiteSpace: 'nowrap',
                                 flexShrink: 0, fontWeight: 600 }}>
                    {s.label.split(' ')[0]}
                  </span>
                </div>
                <div style={{ fontSize: '11px', opacity: 0.55, marginTop: '2px' }}>
                  {c.cif_number}
                  {c.total_deposit_balance_myr
                    ? ` · ${fmtK(Number(c.total_deposit_balance_myr))}` : ''}
                </div>
              </div>
            );
          })}
        </div>

        {/* Footer */}
        <div style={{ padding: '10px 16px', borderTop: '1px solid rgba(255,255,255,0.08)',
                      fontSize: '10px', opacity: 0.35, textAlign: 'center',
                      letterSpacing: '0.8px' }}>
          POWERED BY DATABRICKS
        </div>
      </aside>

      {/* ════════════════ MAIN CONTENT ═══════════════════════════════════════ */}
      <main style={{ flex: 1, overflowY: 'auto', minWidth: 0 }}>

        {loading ? (
          /* Loading state */
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center',
                        height: '100%', color: C.g600 }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ fontSize: '28px', marginBottom: '12px' }}>⏳</div>
              <div style={{ fontSize: '14px' }}>Loading customer profile…</div>
            </div>
          </div>
        ) : !customer ? (
          /* Empty state */
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center',
                        justifyContent: 'center', height: '100%', color: C.g600 }}>
            <div style={{ fontSize: '56px', marginBottom: '16px' }}>🏦</div>
            <div style={{ fontSize: '21px', fontWeight: 700, color: C.navy, marginBottom: '8px' }}>
              Alliance Bank Intelligence Hub
            </div>
            <div style={{ fontSize: '14px', opacity: 0.65 }}>
              Select a customer from the sidebar to view their 360° profile and AI recommendations
            </div>
          </div>
        ) : (
          <div style={{ padding: '22px 24px', display: 'flex', flexDirection: 'column', gap: '18px' }}>

            {/* ── HERO SECTION ─────────────────────────────────────────────── */}
            <div style={{ background: C.white, borderRadius: '16px', padding: '22px 24px',
                          boxShadow: '0 2px 12px rgba(27,58,107,0.07)',
                          display: 'flex', alignItems: 'flex-start', gap: '20px',
                          flexWrap: 'wrap' }}>
              {/* Identity */}
              <div style={{ flex: 1, minWidth: '220px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '14px',
                              marginBottom: '10px' }}>
                  <div style={{ width: '54px', height: '54px', borderRadius: '50%',
                                background: C.navy, color: C.white, flexShrink: 0,
                                display: 'flex', alignItems: 'center', justifyContent: 'center',
                                fontSize: '20px', fontWeight: 800 }}>
                    {(customer.legal_name || '?')[0]}
                  </div>
                  <div>
                    <div style={{ fontSize: '21px', fontWeight: 800, color: C.navy,
                                  lineHeight: 1.15 }}>
                      {customer.legal_name}
                    </div>
                    <div style={{ fontSize: '13px', color: C.g600, marginTop: '3px' }}>
                      CIF {customer.cif_number}
                      {customer.primary_state ? ` · ${customer.primary_state}` : ''}
                    </div>
                  </div>
                </div>

                <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                  {seg && (
                    <span style={{ padding: '3px 10px', borderRadius: '12px', fontSize: '11px',
                                   fontWeight: 700, background: seg.bg, color: seg.textColor }}>
                      {seg.label.toUpperCase()}
                    </span>
                  )}
                  {customer.lifecycle_status && (
                    <span style={{ padding: '3px 10px', borderRadius: '12px', fontSize: '11px',
                                   fontWeight: 600,
                                   background: customer.lifecycle_status === 'active' ? '#DCFCE7' : '#FEE2E2',
                                   color: customer.lifecycle_status === 'active' ? '#166534' : '#991B1B' }}>
                      {customer.lifecycle_status.toUpperCase()}
                    </span>
                  )}
                  {customer.kyc_status && (
                    <span style={{ padding: '3px 10px', borderRadius: '12px', fontSize: '11px',
                                   fontWeight: 600, background: '#EFF6FF', color: '#1E40AF' }}>
                      KYC: {String(customer.kyc_status).toUpperCase()}
                    </span>
                  )}
                </div>
              </div>

              {/* KPI tiles */}
              <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                {[
                  {
                    label: 'Total Balance',
                    value: fmtK(Number(customer.total_deposit_balance_myr || 0)),
                    sub: 'Deposits',
                    color: C.navy,
                  },
                  {
                    label: 'CTOS Score',
                    value: customer.ctos_score || '—',
                    sub: '/ 850',
                    color: customer.ctos_score ? ctosColor(Number(customer.ctos_score)) : C.g600,
                  },
                  {
                    label: 'Tenure',
                    value: `${customer.relationship_tenure_years || 0}yr`,
                    sub: 'with bank',
                    color: C.navy,
                  },
                  {
                    label: 'Digital Score',
                    value: `${customer.digital_maturity_score ?? customer.digital_activity_score ?? 0}/10`,
                    sub: 'maturity',
                    color: C.navy,
                  },
                ].map(kpi => (
                  <div key={kpi.label}
                    style={{ background: C.g50, borderRadius: '12px', padding: '13px 18px',
                             textAlign: 'center', minWidth: '95px' }}>
                    <div style={{ fontSize: '20px', fontWeight: 800, color: kpi.color,
                                  lineHeight: 1.1 }}>
                      {kpi.value}
                    </div>
                    <div style={{ fontSize: '11px', color: C.g600, marginTop: '3px',
                                  fontWeight: 500 }}>
                      {kpi.label}
                    </div>
                    <div style={{ fontSize: '10px', color: C.g400, marginTop: '1px' }}>
                      {kpi.sub}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* ── TWO COLUMN ───────────────────────────────────────────────── */}
            <div style={{ display: 'grid', gridTemplateColumns: '3fr 2fr', gap: '18px',
                          alignItems: 'start' }}>

              {/* ── LEFT: Customer 360 Tabs ─────────────────────────────────── */}
              <div style={{ background: C.white, borderRadius: '16px',
                            boxShadow: '0 2px 12px rgba(27,58,107,0.06)', overflow: 'hidden' }}>
                {/* Tab bar */}
                <div style={{ display: 'flex', borderBottom: `2px solid ${C.g100}`,
                              overflowX: 'auto' }}>
                  {['Profile', 'Financial Products', 'Credit', 'Digital Activity'].map(tab => (
                    <button
                      key={tab}
                      onClick={() => setActiveTab(tab)}
                      style={{
                        padding: '13px 18px',
                        border: 'none',
                        cursor: 'pointer',
                        fontWeight: activeTab === tab ? 700 : 500,
                        fontSize: '13px',
                        background: 'transparent',
                        color: activeTab === tab ? C.navy : C.g600,
                        borderBottom: `2px solid ${activeTab === tab ? C.navy : 'transparent'}`,
                        marginBottom: '-2px',
                        whiteSpace: 'nowrap',
                        transition: 'color 0.12s',
                      }}
                    >
                      {tab}
                    </button>
                  ))}
                </div>

                {/* Tab content */}
                <div style={{ padding: '20px' }}>

                  {/* Profile */}
                  {activeTab === 'Profile' && (
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                      <Row label="Lifestyle Segment" value={seg?.label || '—'} />
                      <Row label="Age"
                        value={customer.age ? `${customer.age} years` : '—'} />
                      <Row label="Annual Income"
                        value={customer.annual_income_amount
                          ? fmtMYR(Number(customer.annual_income_amount)) : '—'} />
                      <Row label="Employment"
                        value={customer.employment_status?.replace(/_/g, ' ') || '—'} />
                      <Row label="Primary State"
                        value={customer.primary_state || '—'} />
                      <Row label="Marital Status"
                        value={customer.marital_status || '—'} />
                      <Row label="Dependents"
                        value={customer.number_of_dependents ?? '—'} />
                      <Row label="Shariah Preference"
                        value={customer.is_shariah_preferred ? 'Yes' : 'No'} />
                      <Row label="NPS Score"
                        value={customer.nps_score != null
                          ? `${customer.nps_score}/10` : '—'} />
                      <Row label="Risk Rating"
                        value={customer.risk_rating?.toUpperCase() || '—'} />
                    </div>
                  )}

                  {/* Financial Products */}
                  {activeTab === 'Financial Products' && (
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                      <Row label="Deposit Balance"
                        value={customer.total_deposit_balance_myr
                          ? fmtMYR(Number(customer.total_deposit_balance_myr)) : '—'} />
                      <Row label="Loan Balance"
                        value={customer.total_loan_outstanding_myr
                          ? fmtMYR(Number(customer.total_loan_outstanding_myr)) : '—'} />
                      <Row label="Credit Card"
                        value={customer.has_credit_card ? '✅ Yes' : '❌ No'} />
                      <Row label="Home Loan"
                        value={customer.has_home_loan ? '✅ Yes' : '❌ No'} />
                      <Row label="Personal Loan"
                        value={customer.has_personal_loan ? '✅ Yes' : '❌ No'} />
                      <Row label="Monthly Commitment"
                        value={customer.monthly_loan_commitment_myr
                          ? fmtMYR(Number(customer.monthly_loan_commitment_myr)) : '—'} />
                      <Row label="Current Account"
                        value={customer.has_current_account ? '✅ Yes' : '❌ No'} />
                      <Row label="Savings Account"
                        value={customer.has_savings_account ? '✅ Yes' : '❌ No'} />
                      <Row label="Fixed Deposit"
                        value={customer.has_fixed_deposit ? '✅ Yes' : '❌ No'} />
                      <Row label="Total Accounts"
                        value={customer.num_accounts ?? '—'} />
                    </div>
                  )}

                  {/* Credit */}
                  {activeTab === 'Credit' && (
                    <div>
                      {/* CTOS gauge */}
                      <div style={{ marginBottom: '16px', padding: '16px', background: C.g50,
                                    borderRadius: '12px' }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between',
                                      alignItems: 'center', marginBottom: '8px' }}>
                          <span style={{ fontWeight: 700, color: C.navy, fontSize: '14px' }}>
                            CTOS Score
                          </span>
                          <span style={{ fontWeight: 800, fontSize: '22px',
                                         color: ctosColor(Number(customer.ctos_score || 0)) }}>
                            {customer.ctos_score || '—'}
                          </span>
                        </div>
                        <div style={{ background: C.g200, borderRadius: '6px', height: '10px',
                                      overflow: 'hidden' }}>
                          <div style={{
                            width: `${Math.min(Number(customer.ctos_score || 0) / 850 * 100, 100)}%`,
                            background: ctosColor(Number(customer.ctos_score || 0)),
                            height: '100%', borderRadius: '6px', transition: 'width 0.8s ease',
                          }} />
                        </div>
                        <div style={{ display: 'flex', justifyContent: 'space-between',
                                      fontSize: '10px', color: C.g400, marginTop: '4px' }}>
                          <span>0</span>
                          <span style={{ color: '#DC2626' }}>600</span>
                          <span style={{ color: '#D97706' }}>700</span>
                          <span>850</span>
                        </div>
                      </div>
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                        <Row label="CCRIS Status"
                          value={customer.ccris_status
                            ? String(customer.ccris_status).toUpperCase() : '—'} />
                        <Row label="Payment Conduct"
                          value={customer.payment_conduct_12m ?? '—'} />
                        <Row label="Debt-to-Income"
                          value={customer.debt_to_income_ratio != null
                            ? `${(Number(customer.debt_to_income_ratio) * 100).toFixed(1)}%` : '—'} />
                        <Row label="Legal Cases"
                          value={customer.legal_cases_count ?? '—'}
                          valueColor={Number(customer.legal_cases_count) > 0 ? '#DC2626' : undefined} />
                        <Row label="Bankruptcy"
                          value={customer.bankruptcy_status || '—'} />
                        <Row label="Bureau Inquiries (6m)"
                          value={customer.inquiry_count_last_6m ?? '—'} />
                      </div>
                    </div>
                  )}

                  {/* Digital Activity */}
                  {activeTab === 'Digital Activity' && (
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                      <Row label="Mobile Sessions (30d)"
                        value={customer.mobile_sessions_30d ?? '—'} />
                      <Row label="Product Views (30d)"
                        value={customer.product_views_last_30d ?? '—'} />
                      <Row label="Last Viewed Product"
                        value={customer.last_product_category_viewed
                          ? String(customer.last_product_category_viewed).replace(/_/g, ' ')
                          : '—'} />
                      <Row label="Telco Carrier"
                        value={customer.telco_carrier || '—'} />
                      <Row label="Telco ARPU"
                        value={customer.telco_arpu_myr
                          ? `MYR ${customer.telco_arpu_myr}` : '—'} />
                      <Row label="Data Consumption"
                        value={customer.telco_data_consumption_gb_monthly
                          ? `${customer.telco_data_consumption_gb_monthly} GB/mo` : '—'} />
                      <Row label="Digital Enrolled"
                        value={customer.digital_banking_enrollment_flag ? '✅ Yes' : '❌ No'} />
                      <Row label="Mobile App"
                        value={customer.mobile_app_user_flag ? '✅ Yes' : '❌ No'} />
                      <Row label="Digital Maturity"
                        value={customer.digital_maturity_score != null
                          ? `${customer.digital_maturity_score}/10` : '—'} />
                      <Row label="Digital Activity"
                        value={customer.digital_activity_score != null
                          ? `${customer.digital_activity_score}/10` : '—'} />
                    </div>
                  )}

                </div>
              </div>

              {/* ── RIGHT: AI Recommendations ──────────────────────────────── */}
              <div style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
                <div style={{ background: C.white, borderRadius: '16px', padding: '20px',
                              boxShadow: '0 2px 12px rgba(27,58,107,0.06)' }}>
                  <div style={{ fontWeight: 700, fontSize: '15px', color: C.navy,
                                marginBottom: '2px' }}>
                    🎯 Next Best Product
                  </div>
                  <div style={{ fontSize: '11px', color: C.g400, marginBottom: '16px' }}>
                    {recs?.live ? 'Live · XGBoost model serving' : 'Batch · XGBoost pre-scored'}
                  </div>

                  {recs ? (
                    <>
                      {/* Recommendation cards */}
                      {[
                        { rank: 1, product: recs.recommendation_1, conf: recs.confidence_1 },
                        { rank: 2, product: recs.recommendation_2, conf: recs.confidence_2 },
                      ].map(({ rank, product, conf }) => {
                        const p = getProd(product);
                        return (
                          <div
                            key={rank}
                            style={{
                              border: `1.5px solid ${rank === 1 ? C.navy : C.g200}`,
                              borderRadius: '12px',
                              padding: '14px',
                              marginBottom: '10px',
                              background: rank === 1 ? '#EEF2FF' : C.white,
                            }}
                          >
                            <div style={{ display: 'flex', justifyContent: 'space-between',
                                          alignItems: 'flex-start', marginBottom: '10px' }}>
                              <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                                <span style={{ fontSize: '24px', lineHeight: 1 }}>{p.icon}</span>
                                <div>
                                  <div style={{ fontWeight: 700, fontSize: '14px',
                                                color: C.navy }}>
                                    {p.label}
                                  </div>
                                  <div style={{ fontSize: '10px', color: C.g400,
                                                marginTop: '1px' }}>
                                    #{rank} Recommendation
                                  </div>
                                </div>
                              </div>
                              <div style={{ textAlign: 'right' }}>
                                <div style={{ fontSize: '22px', fontWeight: 800,
                                              color: p.color, lineHeight: 1 }}>
                                  {conf}%
                                </div>
                                <div style={{ fontSize: '10px', color: C.g400 }}>confidence</div>
                              </div>
                            </div>
                            {/* Confidence bar */}
                            <div style={{ background: C.g200, borderRadius: '4px', height: '5px' }}>
                              <div style={{ width: `${conf}%`, background: p.color,
                                            height: '5px', borderRadius: '4px',
                                            transition: 'width 0.8s ease' }} />
                            </div>
                          </div>
                        );
                      })}

                      {liveError && (
                        <div style={{ fontSize: '12px', color: '#DC2626', marginBottom: '8px',
                                      padding: '8px', background: '#FEF2F2', borderRadius: '6px' }}>
                          {liveError}
                        </div>
                      )}

                      {/* Live Score button */}
                      <button
                        onClick={liveScore}
                        disabled={liveLoading}
                        style={{
                          width: '100%', padding: '10px',
                          background: liveLoading ? C.g400 : C.navy,
                          color: C.white, border: 'none', borderRadius: '8px',
                          cursor: liveLoading ? 'not-allowed' : 'pointer',
                          fontWeight: 600, fontSize: '13px', marginBottom: '8px',
                          transition: 'background 0.15s',
                        }}
                      >
                        {liveLoading ? '⏳ Scoring…' : '⚡ Live Score'}
                      </button>

                      <div style={{ height: '1px', background: C.g100, margin: '8px 0' }} />

                      {/* Draft Email button */}
                      <button
                        onClick={draftEmail}
                        style={{
                          width: '100%', padding: '10px',
                          background: C.red, color: C.white,
                          border: 'none', borderRadius: '8px',
                          cursor: 'pointer', fontWeight: 600, fontSize: '13px',
                          transition: 'opacity 0.15s',
                        }}
                      >
                        ✉️ Draft Email
                      </button>
                    </>
                  ) : (
                    <div style={{ textAlign: 'center', padding: '24px', color: C.g600,
                                  fontSize: '13px' }}>
                      Loading recommendations…
                    </div>
                  )}
                </div>
              </div>
            </div>

          </div>
        )}
      </main>

      {/* ════════════════ EMAIL MODAL ════════════════════════════════════════ */}
      {showEmail && (
        <div
          style={{
            position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.52)',
            display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 300,
          }}
          onClick={e => { if (e.target === e.currentTarget) setShowEmail(false); }}
        >
          <div style={{ background: C.white, borderRadius: '20px', padding: '28px',
                        width: '580px', maxWidth: '92vw',
                        boxShadow: '0 24px 64px rgba(0,0,0,0.35)' }}>

            {/* Modal header */}
            <div style={{ display: 'flex', justifyContent: 'space-between',
                          alignItems: 'flex-start', marginBottom: '18px' }}>
              <div>
                <div style={{ fontWeight: 800, fontSize: '17px', color: C.navy }}>
                  ✉️ AI-Drafted Email
                </div>
                <div style={{ fontSize: '12px', color: C.g600, marginTop: '3px' }}>
                  {customer?.legal_name}
                  {recs ? ` · ${getProd(recs.recommendation_1).label}` : ''}
                </div>
              </div>
              <button
                onClick={() => setShowEmail(false)}
                style={{ background: 'none', border: 'none', cursor: 'pointer',
                         fontSize: '20px', color: C.g400, lineHeight: 1, padding: 0 }}
              >
                ✕
              </button>
            </div>

            {/* Context chips */}
            {recs && (
              <div style={{ display: 'flex', gap: '8px', marginBottom: '16px',
                            flexWrap: 'wrap' }}>
                <span style={{ padding: '4px 10px', borderRadius: '8px',
                               background: '#EEF2FF', color: C.navy,
                               fontSize: '12px', fontWeight: 600 }}>
                  {getProd(recs.recommendation_1).icon} {getProd(recs.recommendation_1).label}
                </span>
                <span style={{ padding: '4px 10px', borderRadius: '8px',
                               background: '#DCFCE7', color: '#166534',
                               fontSize: '12px', fontWeight: 600 }}>
                  {recs.confidence_1}% confidence
                </span>
                {seg && (
                  <span style={{ padding: '4px 10px', borderRadius: '8px',
                                 background: seg.bg, color: seg.textColor,
                                 fontSize: '12px', fontWeight: 600 }}>
                    {seg.label}
                  </span>
                )}
              </div>
            )}

            {/* Email body */}
            {emailLoading ? (
              <div style={{ textAlign: 'center', padding: '44px', color: C.g600 }}>
                <div style={{ fontSize: '26px', marginBottom: '14px' }}>✨</div>
                <div style={{ fontSize: '14px', fontWeight: 500 }}>
                  GLM 5.2 is drafting your personalised email…
                </div>
                <div style={{ fontSize: '12px', marginTop: '5px', opacity: 0.6 }}>
                  via Unity AI Gateway
                </div>
              </div>
            ) : (
              <textarea
                value={emailDraft}
                onChange={e => setEmailDraft(e.target.value)}
                style={{
                  width: '100%', height: '220px', padding: '14px', fontSize: '13px',
                  lineHeight: 1.65, border: `1.5px solid ${C.g200}`, borderRadius: '10px',
                  fontFamily: 'inherit', resize: 'vertical', outline: 'none',
                  boxSizing: 'border-box', color: C.g900,
                }}
              />
            )}

            {/* Footer row */}
            <div style={{ display: 'flex', justifyContent: 'space-between',
                          alignItems: 'center', marginTop: '14px' }}>
              <div style={{ fontSize: '11px', color: C.g400, maxWidth: '280px' }}>
                ⚠️ AI-generated via Unity AI Gateway — reviewed before sending
              </div>
              <div style={{ display: 'flex', gap: '8px' }}>
                <button
                  onClick={() => setShowEmail(false)}
                  style={{ padding: '8px 16px', border: `1.5px solid ${C.g200}`,
                           borderRadius: '8px', background: C.white, cursor: 'pointer',
                           fontSize: '13px', color: C.g600 }}
                >
                  Close
                </button>
                <button
                  onClick={() => navigator.clipboard.writeText(emailDraft)}
                  disabled={emailLoading}
                  style={{ padding: '8px 16px', background: C.navy, color: C.white,
                           border: 'none', borderRadius: '8px', cursor: 'pointer',
                           fontWeight: 600, fontSize: '13px' }}
                >
                  📋 Copy
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
