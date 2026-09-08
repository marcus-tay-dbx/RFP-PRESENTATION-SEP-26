import React, { useState } from 'react';
import { DBXBankHeader } from '../components/DBXBankHeader';
import { RecommendationCard }  from '../components/RecommendationCard';
import { theme } from '../theme';

const API  = '';
const TABS = ['Profile', 'Accounts', 'Transactions', 'Credit', 'Digital'];

export default function Customer360ViewA() {
  const [partyId, setPartyId]           = useState('');
  const [customer, setCustomer]         = useState<any>(null);
  const [recs, setRecs]                 = useState<any>(null);
  const [activeTab, setActiveTab]       = useState('Profile');
  const [emailDraft, setEmailDraft]     = useState('');
  const [draftLoading, setDraftLoading] = useState(false);
  const [searchResults, setSearchResults] = useState<any[]>([]);

  const search = async (q: string) => {
    if (!q) return;
    const res = await fetch(`${API}/api/customers/search?q=${q}&limit=8`);
    setSearchResults(await res.json());
  };

  const load = async (pid: string) => {
    setPartyId(pid); setSearchResults([]);
    const [c, r] = await Promise.all([
      fetch(`${API}/api/customer/${pid}`).then(r => r.json()),
      fetch(`${API}/api/recommend/${pid}`).then(r => r.json())
    ]);
    setCustomer(c); setRecs(r);
  };

  const draftEmail = async () => {
    if (!recs || !customer) return;
    setDraftLoading(true);
    const res = await fetch(`${API}/api/draft-email`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        party_id:       partyId,
        recommendation: recs.recommendation_1,
        confidence:     recs.confidence_1,
        language:       customer.preferred_language_code || 'en'
      })
    });
    const data = await res.json();
    setEmailDraft(data.email_draft);
    setDraftLoading(false);
  };

  const segInfo = customer
    ? theme.segments[customer.lifestyle_segment as keyof typeof theme.segments]
    : null;

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', minHeight: '100vh', background: theme.colors.offWhite }}>
      <DBXBankHeader subtitle="Banker's Workstation" />
      <div style={{ display: 'flex', height: 'calc(100vh - 60px)' }}>

        {/* LEFT SIDEBAR */}
        <div style={{ width: '280px', background: theme.colors.navy, color: 'white',
                      padding: '16px', display: 'flex', flexDirection: 'column',
                      gap: '12px', overflow: 'auto' }}>
          <div style={{ fontWeight: 600, fontSize: '13px', opacity: 0.7, textTransform: 'uppercase' }}>
            Search Customer
          </div>
          <input placeholder="Name or CIF..."
            onChange={e => search(e.target.value)}
            style={{ width: '100%', padding: '8px 12px', borderRadius: '6px', border: 'none',
                     background: 'rgba(255,255,255,0.15)', color: 'white',
                     outline: 'none', fontSize: '14px' }} />
          {searchResults.map(c => (
            <div key={c.party_id} onClick={() => load(c.party_id)}
              style={{ padding: '10px', borderRadius: '6px', cursor: 'pointer',
                       background: partyId === c.party_id
                         ? 'rgba(255,255,255,0.2)' : 'rgba(255,255,255,0.08)',
                       transition: 'background 0.2s' }}>
              <div style={{ fontWeight: 600, fontSize: '13px' }}>{c.legal_name}</div>
              <div style={{ fontSize: '11px', opacity: 0.7 }}>
                {c.cif_number} · {c.lifestyle_segment?.replace('_', ' ')}
              </div>
            </div>
          ))}
        </div>

        {/* MAIN PANEL */}
        <div style={{ flex: 1, overflow: 'auto', padding: '24px' }}>
          {!customer ? (
            <div style={{ textAlign: 'center', marginTop: '80px', color: theme.colors.grey600 }}>
              <div style={{ fontSize: '48px' }}>🔍</div>
              <div style={{ fontSize: '18px', marginTop: '16px' }}>Search for a customer to begin</div>
            </div>
          ) : (
            <>
              {/* Customer header */}
              <div style={{ background: 'white', borderRadius: '12px', padding: '20px',
                            marginBottom: '16px', display: 'flex', gap: '20px', alignItems: 'center',
                            boxShadow: '0 1px 4px rgba(0,0,0,0.1)' }}>
                <div style={{ width: '56px', height: '56px', borderRadius: '50%',
                               background: theme.colors.navy, color: 'white', display: 'flex',
                               alignItems: 'center', justifyContent: 'center',
                               fontSize: '22px', fontWeight: 700 }}>
                  {customer.legal_name?.[0]}
                </div>
                <div>
                  <div style={{ fontWeight: 700, fontSize: '20px' }}>{customer.legal_name}</div>
                  <div style={{ fontSize: '13px', color: theme.colors.grey600 }}>
                    {customer.cif_number} · {customer.primary_state} · {customer.relationship_tenure_years}yr tenure
                  </div>
                  {segInfo && (
                    <span style={{ background: segInfo.color, color: 'white', padding: '2px 8px',
                                   borderRadius: '12px', fontSize: '11px', fontWeight: 600 }}>
                      {segInfo.label}
                    </span>
                  )}
                </div>
                <div style={{ marginLeft: 'auto', display: 'flex', gap: '16px' }}>
                  {[
                    { label: 'Balance', value: `RM ${((customer.total_deposit_balance_myr||0)/1000).toFixed(0)}K` },
                    { label: 'CTOS',    value: customer.ctos_score || 'N/A' },
                    { label: 'NPS',     value: customer.nps_score  || 'N/A' },
                  ].map(kpi => (
                    <div key={kpi.label}
                      style={{ textAlign: 'center', padding: '8px 16px',
                               background: theme.colors.grey50, borderRadius: '8px' }}>
                      <div style={{ fontWeight: 700, fontSize: '18px', color: theme.colors.navy }}>
                        {kpi.value}
                      </div>
                      <div style={{ fontSize: '11px', color: theme.colors.grey600 }}>{kpi.label}</div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Tabs */}
              <div style={{ display: 'flex', gap: '4px', marginBottom: '16px' }}>
                {TABS.map(t => (
                  <button key={t} onClick={() => setActiveTab(t)}
                    style={{ padding: '8px 16px', borderRadius: '6px', border: 'none',
                             cursor: 'pointer', fontWeight: activeTab === t ? 700 : 400,
                             fontSize: '13px',
                             background: activeTab === t ? theme.colors.navy : theme.colors.grey50,
                             color:      activeTab === t ? 'white' : theme.colors.grey600 }}>
                    {t}
                  </button>
                ))}
              </div>

              {/* Tab content */}
              <div style={{ background: 'white', borderRadius: '12px', padding: '20px',
                            boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
                {activeTab === 'Profile' && (
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                    {[
                      ['Employment',      customer.employment_status?.replace(/_/g,' ')],
                      ['Annual Income',   `RM ${((customer.annual_income_amount||0)/1000).toFixed(0)}K`],
                      ['Marital Status',  customer.marital_status],
                      ['Dependents',      customer.number_of_dependents],
                      ['Net Worth Band',  customer.net_worth_band?.replace(/_/g,' ')],
                      ['Risk Rating',     customer.risk_rating?.toUpperCase()],
                      ['KYC Status',      customer.kyc_status],
                      ['Shariah Preferred', customer.is_shariah_preferred ? 'Yes' : 'No'],
                      ['Digital Enrolled',  customer.digital_banking_enrollment_flag ? 'Yes' : 'No'],
                      ['Mobile App User',   customer.mobile_app_user_flag ? 'Yes' : 'No'],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', background:theme.colors.grey50, borderRadius:'6px' }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px' }}>{v ?? '—'}</span>
                      </div>
                    ))}
                  </div>
                )}
                {activeTab === 'Accounts' && (
                  <div>
                    {[
                      ['Accounts',             customer.num_accounts],
                      ['Total Balance',        `RM ${((customer.total_deposit_balance_myr||0)/1000).toFixed(1)}K`],
                      ['Has Current Account',  customer.has_current_account ? 'Yes' : 'No'],
                      ['Has Savings Account',  customer.has_savings_account  ? 'Yes' : 'No'],
                      ['Has Fixed Deposit',    customer.has_fixed_deposit    ? 'Yes' : 'No'],
                      ['Loan Facilities',      customer.num_loan_facilities],
                      ['Total Loan Outstanding', `RM ${((customer.total_loan_outstanding_myr||0)/1000).toFixed(1)}K`],
                      ['Monthly Loan Commitment', `RM ${((customer.monthly_loan_commitment_myr||0)).toFixed(0)}`],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', borderBottom:`1px solid ${theme.colors.grey50}` }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px' }}>{v ?? '—'}</span>
                      </div>
                    ))}
                  </div>
                )}
                {activeTab === 'Credit' && (
                  <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:'12px' }}>
                    {[
                      ['CTOS Score',           customer.ctos_score],
                      ['CCRIS Status',         customer.ccris_status?.toUpperCase()],
                      ['Payment Conduct (12m)', customer.payment_conduct_12m],
                      ['Credit Facilities',    customer.total_credit_facilities],
                      ['Monthly KB Commitment', `RM ${((customer.kb_monthly_commitment_myr||0)).toFixed(0)}`],
                      ['Bureau Inquiries (6m)', customer.inquiry_count_last_6m],
                      ['Legal Cases',          customer.legal_cases_count],
                      ['Bankruptcy',           customer.bankruptcy_status],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', background:theme.colors.grey50, borderRadius:'6px' }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px',
                                       color: k === 'CTOS Score'
                                         ? (Number(v) > 700 ? '#059669' : Number(v) > 600 ? '#D97706' : '#DC2626')
                                         : 'inherit' }}>
                          {v ?? '—'}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
                {activeTab === 'Digital' && (
                  <div>
                    {[
                      ['Digital Activity Score', `${customer.digital_activity_score}/10`],
                      ['Digital Maturity',       `${customer.digital_maturity_score}/10`],
                      ['Mobile Sessions (30d)',  customer.mobile_sessions_30d],
                      ['Product Views (30d)',    customer.product_views_last_30d],
                      ['Last Product Viewed',    customer.last_product_category_viewed?.replace(/_/g,' ') || '—'],
                      ['Telco ARPU',             `RM ${customer.telco_arpu_myr || 0}`],
                      ['Data Consumption',       `${customer.telco_data_consumption_gb_monthly || 0} GB/mo`],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', borderBottom:`1px solid ${theme.colors.grey50}` }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px' }}>{v}</span>
                      </div>
                    ))}
                  </div>
                )}
                {activeTab === 'Transactions' && (
                  <div>
                    {[
                      ['Transactions (30d)',  customer.txn_count_30d],
                      ['Transactions (90d)',  customer.txn_count_90d],
                      ['Transactions (12m)',  customer.txn_count_12m],
                      ['Debit (30d)',         `RM ${((customer.total_debit_30d_myr||0)/1000).toFixed(1)}K`],
                      ['Credit (30d)',        `RM ${((customer.total_credit_30d_myr||0)/1000).toFixed(1)}K`],
                      ['Avg Transaction',    `RM ${((customer.avg_txn_amount_myr||0)).toFixed(0)}`],
                      ['Card Spend (30d)',    `RM ${((customer.card_spend_30d_myr||0)).toFixed(0)}`],
                      ['Overseas Transactions', customer.overseas_txn_flag ? 'Yes' : 'No'],
                    ].map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'8px', borderBottom:`1px solid ${theme.colors.grey50}` }}>
                        <span style={{ color: theme.colors.grey600, fontSize: '13px' }}>{k}</span>
                        <span style={{ fontWeight: 600, fontSize: '13px' }}>{v}</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </>
          )}
        </div>

        {/* RIGHT RAIL — Recommendations */}
        {customer && recs && (
          <div style={{ width: '280px', padding: '24px 16px', display:'flex',
                        flexDirection:'column', gap:'12px',
                        borderLeft:`1px solid ${theme.colors.grey200}`,
                        background:'white', overflowY:'auto' }}>
            <div style={{ fontWeight: 700, fontSize: '14px', color: theme.colors.navy }}>
              🎯 Next Best Product
            </div>
            <RecommendationCard rank={1} product={recs.recommendation_1}
              confidence={recs.confidence_1} onDraftEmail={draftEmail} />
            <RecommendationCard rank={2} product={recs.recommendation_2}
              confidence={recs.confidence_2} />
            {emailDraft && (
              <div style={{ background:'#EEF2FF', borderRadius:'8px', padding:'12px', marginTop:'8px' }}>
                <div style={{ fontWeight:600, fontSize:'12px', color:theme.colors.navy, marginBottom:'8px' }}>
                  ✉️ AI-Drafted Email (GLM 5.2)
                </div>
                <textarea value={emailDraft} onChange={e => setEmailDraft(e.target.value)}
                  style={{ width:'100%', height:'180px', fontSize:'12px',
                           border:`1px solid ${theme.colors.grey200}`, borderRadius:'6px',
                           padding:'8px', fontFamily:'inherit', resize:'vertical' }} />
              </div>
            )}
            {draftLoading && (
              <div style={{ textAlign:'center', color:theme.colors.grey600, fontSize:'13px' }}>
                ✨ Drafting email...
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
