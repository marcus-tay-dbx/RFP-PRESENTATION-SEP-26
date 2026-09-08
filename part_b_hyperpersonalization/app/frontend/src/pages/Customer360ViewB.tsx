import React, { useState } from 'react';
import { AllianceBankHeader } from '../components/AllianceBankHeader';
import { RecommendationCard }  from '../components/RecommendationCard';
import { theme } from '../theme';

const API = '';

export default function Customer360ViewB() {
  const [query, setQuery]               = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const [customer, setCustomer]         = useState<any>(null);
  const [recs, setRecs]                 = useState<any>(null);
  const [emailDraft, setEmailDraft]     = useState('');
  const [showEmailModal, setShowEmailModal] = useState(false);
  const [draftLoading, setDraftLoading] = useState(false);

  const search = async (q: string) => {
    setQuery(q);
    if (q.length < 2) { setSearchResults([]); return; }
    const res = await fetch(`${API}/api/customers/search?q=${encodeURIComponent(q)}&limit=5`);
    setSearchResults(await res.json());
  };

  const load = async (pid: string) => {
    setSearchResults([]); setQuery('');
    const [c, r] = await Promise.all([
      fetch(`${API}/api/customer/${pid}`).then(r => r.json()),
      fetch(`${API}/api/recommend/${pid}`).then(r => r.json())
    ]);
    setCustomer(c); setRecs(r);
  };

  const draftEmail = async () => {
    if (!recs || !customer) return;
    setDraftLoading(true); setShowEmailModal(true);
    const res = await fetch(`${API}/api/draft-email`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        party_id:       customer.party_id,
        recommendation: recs.recommendation_1,
        confidence:     recs.confidence_1,
        language:       customer.preferred_language_code
      })
    });
    const data = await res.json();
    setEmailDraft(data.email_draft); setDraftLoading(false);
  };

  const segInfo = customer
    ? (theme.segments[customer.lifestyle_segment as keyof typeof theme.segments] || theme.segments.mass_market)
    : null;

  const KPITile = ({ label, value, sub }: any) => (
    <div style={{ background:'white', borderRadius:'12px', padding:'16px 20px',
                  boxShadow:'0 2px 8px rgba(27,58,107,0.08)', flex:1, minWidth:'120px' }}>
      <div style={{ fontSize:'22px', fontWeight:800, color:theme.colors.navy }}>{value}</div>
      <div style={{ fontSize:'12px', color:theme.colors.grey600, marginTop:'2px' }}>{label}</div>
      {sub && <div style={{ fontSize:'11px', color:theme.colors.grey600, opacity:0.7 }}>{sub}</div>}
    </div>
  );

  return (
    <div style={{ fontFamily:'system-ui,sans-serif', minHeight:'100vh', background:'#F0F4FA' }}>
      <AllianceBankHeader subtitle="Intelligence Hub" />

      {/* Search bar */}
      <div style={{ background:theme.colors.navy, padding:'16px 32px', position:'relative' }}>
        <input value={query} onChange={e => search(e.target.value)}
          placeholder="Search customer by name or CIF..."
          style={{ width:'100%', maxWidth:'480px', padding:'10px 16px', borderRadius:'24px',
                   border:'none', fontSize:'14px', outline:'none',
                   boxShadow:'0 2px 8px rgba(0,0,0,0.2)' }} />
        {searchResults.length > 0 && (
          <div style={{ position:'absolute', top:'52px', left:'32px', width:'480px',
                        background:'white', borderRadius:'8px',
                        boxShadow:'0 8px 24px rgba(0,0,0,0.15)', zIndex:100, overflow:'hidden' }}>
            {searchResults.map(c => (
              <div key={c.party_id} onClick={() => load(c.party_id)}
                style={{ padding:'12px 16px', cursor:'pointer',
                         borderBottom:`1px solid ${theme.colors.grey50}`,
                         display:'flex', justifyContent:'space-between', alignItems:'center' }}
                onMouseEnter={e => (e.currentTarget.style.background = theme.colors.grey50)}
                onMouseLeave={e => (e.currentTarget.style.background = 'white')}>
                <div>
                  <div style={{ fontWeight:600 }}>{c.legal_name}</div>
                  <div style={{ fontSize:'12px', color:theme.colors.grey600 }}>{c.cif_number}</div>
                </div>
                <span style={{ fontSize:'11px', padding:'2px 8px', borderRadius:'12px',
                               background:theme.colors.grey50, color:theme.colors.grey600 }}>
                  {c.lifestyle_segment?.replace(/_/g,' ')}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {!customer ? (
        <div style={{ textAlign:'center', marginTop:'100px', color:theme.colors.grey600 }}>
          <div style={{ fontSize:'64px' }}>🏦</div>
          <div style={{ fontSize:'20px', fontWeight:600, marginTop:'16px' }}>
            DBX Bank Customer Intelligence
          </div>
          <div style={{ fontSize:'14px', marginTop:'8px' }}>
            Search for a customer to view their 360 profile and recommendations
          </div>
        </div>
      ) : (
        <div style={{ padding:'24px 32px', display:'flex', flexDirection:'column', gap:'20px' }}>

          {/* Hero card */}
          <div style={{ background:`linear-gradient(135deg, ${theme.colors.navy} 0%, ${theme.colors.navyLight} 100%)`,
                        borderRadius:'16px', padding:'24px', color:'white',
                        display:'flex', alignItems:'center', gap:'24px',
                        boxShadow:'0 4px 16px rgba(27,58,107,0.3)' }}>
            <div style={{ width:'64px', height:'64px', borderRadius:'50%',
                           background:'rgba(255,255,255,0.2)', display:'flex',
                           alignItems:'center', justifyContent:'center',
                           fontSize:'28px', fontWeight:800 }}>
              {customer.legal_name?.[0]}
            </div>
            <div style={{ flex:1 }}>
              <div style={{ fontSize:'24px', fontWeight:800 }}>{customer.legal_name}</div>
              <div style={{ opacity:0.8, fontSize:'14px', marginTop:'4px' }}>
                {customer.cif_number} · {customer.primary_state} · {customer.preferred_language_code === 'ms' ? 'BM' : 'EN'}
              </div>
              <span style={{ background:theme.colors.red, padding:'3px 10px', borderRadius:'12px',
                             fontSize:'11px', fontWeight:700, marginTop:'8px', display:'inline-block' }}>
                {segInfo?.label?.toUpperCase()}
              </span>
            </div>
            <div style={{ display:'flex', gap:'12px', flexWrap:'wrap' }}>
              {[
                { label:'Tenure', value:`${customer.relationship_tenure_years}yr` },
                { label:'NPS',    value: customer.nps_score || '—' },
                { label:'Risk',   value: customer.risk_rating?.toUpperCase() },
              ].map(k => (
                <div key={k.label}
                  style={{ textAlign:'center', background:'rgba(255,255,255,0.15)',
                           padding:'10px 16px', borderRadius:'10px' }}>
                  <div style={{ fontWeight:800, fontSize:'18px' }}>{k.value}</div>
                  <div style={{ fontSize:'11px', opacity:0.7 }}>{k.label}</div>
                </div>
              ))}
            </div>
          </div>

          {/* KPI row */}
          <div style={{ display:'flex', gap:'12px', flexWrap:'wrap' }}>
            <KPITile label="Total Balance"    value={`RM ${((customer.total_deposit_balance_myr||0)/1000).toFixed(0)}K`} sub="Deposit" />
            <KPITile label="CTOS Score"       value={customer.ctos_score || '—'} sub={customer.ccris_status?.toUpperCase()} />
            <KPITile label="Txn (30d)"        value={customer.txn_count_30d || 0} sub="transactions" />
            <KPITile label="Digital Score"    value={`${customer.digital_activity_score || 5}/10`} sub="activity" />
            <KPITile label="Products Held"    value={(customer.num_accounts||0) + (customer.num_loan_facilities||0)} sub="total" />
            <KPITile label="Loan Outstanding" value={`RM ${((customer.total_loan_outstanding_myr||0)/1000).toFixed(0)}K`} sub="active" />
          </div>

          {/* Split: 360 summary | Recommendations */}
          <div style={{ display:'grid', gridTemplateColumns:'1fr 340px', gap:'20px' }}>

            {/* Customer summary */}
            <div style={{ background:'white', borderRadius:'12px', padding:'20px',
                          boxShadow:'0 2px 8px rgba(0,0,0,0.06)' }}>
              <div style={{ fontWeight:700, fontSize:'15px', color:theme.colors.navy, marginBottom:'16px' }}>
                Customer 360 Summary
              </div>
              <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr 1fr', gap:'12px' }}>
                {[
                  { section:'Identity', items:[
                    ['Employment',    customer.employment_status?.replace(/_/g,' ')],
                    ['Education',     customer.education_level],
                    ['Marital Status', customer.marital_status],
                    ['Dependents',    customer.number_of_dependents],
                  ]},
                  { section:'Financial', items:[
                    ['Annual Income', `RM ${((customer.annual_income_amount||0)/1000).toFixed(0)}K`],
                    ['Net Worth',     customer.net_worth_band?.replace(/_/g,' ')],
                    ['Monthly Commit', `RM ${((customer.monthly_loan_commitment_myr||0)).toFixed(0)}`],
                    ['Avg Txn',       `RM ${((customer.avg_txn_amount_myr||0)).toFixed(0)}`],
                  ]},
                  { section:'Digital', items:[
                    ['Enrolled',    customer.digital_banking_enrollment_flag ? '✅' : '❌'],
                    ['Mobile App',  customer.mobile_app_user_flag  ? '✅' : '❌'],
                    ['Biometric',   customer.biometric_auth_enabled ? '✅' : '❌'],
                    ['Shariah Pref', customer.is_shariah_preferred  ? 'Yes' : 'No'],
                  ]},
                ].map(group => (
                  <div key={group.section}>
                    <div style={{ fontWeight:600, fontSize:'12px', color:theme.colors.red,
                                  textTransform:'uppercase', marginBottom:'8px' }}>
                      {group.section}
                    </div>
                    {group.items.map(([k,v]) => (
                      <div key={k} style={{ display:'flex', justifyContent:'space-between',
                                            padding:'5px 0',
                                            borderBottom:`1px solid ${theme.colors.grey50}`,
                                            fontSize:'13px' }}>
                        <span style={{ color:theme.colors.grey600 }}>{k}</span>
                        <span style={{ fontWeight:500 }}>{v ?? '—'}</span>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            </div>

            {/* Recommendations + AI */}
            {recs && (
              <div style={{ display:'flex', flexDirection:'column', gap:'12px' }}>
                <div style={{ background:'white', borderRadius:'12px', padding:'20px',
                              boxShadow:'0 2px 8px rgba(0,0,0,0.06)' }}>
                  <div style={{ fontWeight:700, fontSize:'15px', color:theme.colors.navy, marginBottom:'12px' }}>
                    🎯 Product Recommendations
                  </div>
                  <RecommendationCard rank={1} product={recs.recommendation_1}
                    confidence={recs.confidence_1} onDraftEmail={draftEmail} />
                  <div style={{ marginTop:'10px' }}>
                    <RecommendationCard rank={2} product={recs.recommendation_2}
                      confidence={recs.confidence_2} />
                  </div>
                </div>
                <div style={{ background:`linear-gradient(135deg, #EEF2FF, #E0E7FF)`,
                              borderRadius:'12px', padding:'16px', cursor:'pointer' }}
                  onClick={draftEmail}>
                  <div style={{ fontWeight:700, color:theme.colors.navy }}>✨ AI-Powered Actions</div>
                  <div style={{ fontSize:'13px', color:theme.colors.grey600, marginTop:'4px' }}>
                    Powered by GLM 5.2 via Unity AI Gateway
                  </div>
                  <button style={{ marginTop:'12px', width:'100%', padding:'10px',
                                   background:theme.colors.navy, color:'white', border:'none',
                                   borderRadius:'8px', cursor:'pointer', fontWeight:600, fontSize:'13px' }}>
                    ✉️ Draft Personalised Email
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Email modal */}
      {showEmailModal && (
        <div style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.5)',
                      display:'flex', alignItems:'center', justifyContent:'center', zIndex:200 }}>
          <div style={{ background:'white', borderRadius:'16px', padding:'24px',
                        width:'560px', maxWidth:'90vw',
                        boxShadow:'0 20px 60px rgba(0,0,0,0.3)' }}>
            <div style={{ display:'flex', justifyContent:'space-between',
                          alignItems:'center', marginBottom:'16px' }}>
              <div style={{ fontWeight:700, fontSize:'16px', color:theme.colors.navy }}>
                ✉️ AI-Drafted Email — {theme.products[recs?.recommendation_1 as keyof typeof theme.products]?.label}
              </div>
              <button onClick={() => setShowEmailModal(false)}
                style={{ background:'none', border:'none', cursor:'pointer', fontSize:'18px' }}>✕</button>
            </div>
            {draftLoading ? (
              <div style={{ textAlign:'center', padding:'40px', color:theme.colors.grey600 }}>
                ✨ GLM 5.2 is drafting your email...
              </div>
            ) : (
              <>
                <textarea value={emailDraft} onChange={e => setEmailDraft(e.target.value)}
                  style={{ width:'100%', height:'220px', padding:'12px', fontSize:'13px',
                           border:`1px solid ${theme.colors.grey200}`, borderRadius:'8px',
                           fontFamily:'inherit', resize:'vertical', lineHeight:1.6 }} />
                <div style={{ display:'flex', gap:'8px', marginTop:'12px', justifyContent:'flex-end' }}>
                  <button onClick={() => setShowEmailModal(false)}
                    style={{ padding:'8px 16px', border:`1px solid ${theme.colors.grey200}`,
                             borderRadius:'6px', background:'white', cursor:'pointer' }}>Cancel</button>
                  <button onClick={() => { navigator.clipboard.writeText(emailDraft); }}
                    style={{ padding:'8px 16px', background:theme.colors.navy, color:'white',
                             border:'none', borderRadius:'6px', cursor:'pointer', fontWeight:600 }}>
                    📋 Copy Email
                  </button>
                </div>
                <div style={{ marginTop:'8px', fontSize:'11px', color:theme.colors.grey600, textAlign:'right' }}>
                  Generated by system.ai.databricks-glm-5-2 via Unity AI Gateway
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
