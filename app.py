import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Risk Adjustment Intelligence", page_icon="🎯", layout="wide", initial_sidebar_state="expanded")

# ============================================================
# CSS
# ============================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700;800&display=swap');
    .stApp { font-family: 'DM Sans', sans-serif; }
    .hero-header {
        background: linear-gradient(135deg, #0c0f1a 0%, #1a1f3a 40%, #2d1b4e 70%, #1a1f3a 100%);
        padding: 2rem 2.5rem; border-radius: 20px; margin-bottom: 1.5rem;
        position: relative; overflow: hidden; border: 1px solid rgba(139,92,246,0.15);
    }
    .hero-header::before { content: ''; position: absolute; top: -50%; right: -20%; width: 400px; height: 400px; background: radial-gradient(circle, rgba(139,92,246,0.15) 0%, transparent 70%); border-radius: 50%; }
    .hero-header h1 { color: #fff; font-size: 2.2rem; font-weight: 800; margin: 0; }
    .hero-header p { color: #94a3b8; font-size: 1rem; margin-top: 0.3rem; }
    .hero-header .author { color: #8b5cf6; font-size: 0.8rem; font-weight: 600; margin-top: 0.5rem; letter-spacing: 1px; text-transform: uppercase; }
    .kpi-card { background: linear-gradient(145deg, #fff 0%, #f8fafc 100%); border: 1px solid #e2e8f0; border-radius: 16px; padding: 1.2rem; text-align: center; box-shadow: 0 2px 4px rgba(0,0,0,0.04); }
    .kpi-value { font-size: 2rem; font-weight: 800; background: linear-gradient(135deg, #8b5cf6, #6d28d9); -webkit-background-clip: text; -webkit-text-fill-color: transparent; line-height: 1.2; }
    .kpi-value-green { font-size: 2rem; font-weight: 800; background: linear-gradient(135deg, #10b981, #059669); -webkit-background-clip: text; -webkit-text-fill-color: transparent; line-height: 1.2; }
    .kpi-value-amber { font-size: 2rem; font-weight: 800; background: linear-gradient(135deg, #f59e0b, #d97706); -webkit-background-clip: text; -webkit-text-fill-color: transparent; line-height: 1.2; }
    .kpi-value-red { font-size: 2rem; font-weight: 800; background: linear-gradient(135deg, #ef4444, #dc2626); -webkit-background-clip: text; -webkit-text-fill-color: transparent; line-height: 1.2; }
    .kpi-label { font-size: 0.75rem; font-weight: 600; color: #64748b; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 0.3rem; }
    .section-header { font-size: 1.15rem; font-weight: 700; color: #0f172a; margin: 1.2rem 0 0.8rem 0; padding-bottom: 0.4rem; border-bottom: 3px solid #8b5cf6; display: inline-block; }
    [data-testid="stSidebar"] { background: linear-gradient(180deg, #0c0f1a 0%, #1a1f3a 100%); }
    [data-testid="stSidebar"] * { color: #e2e8f0 !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 4px; }
    .stTabs [data-baseweb="tab"] { border-radius: 10px; padding: 8px 16px; font-weight: 600; font-size: 0.85rem; }
    .footer { text-align: center; padding: 1rem; color: #94a3b8; font-size: 0.8rem; border-top: 1px solid #e2e8f0; margin-top: 2rem; }
    .footer a { color: #8b5cf6; text-decoration: none; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# CONNECTION
# ============================================================
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"],
    )

def run_query(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()

def run_query_df(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)

def sn(v, fmt=",.0f"):
    try:
        return f"{float(v):{fmt}}" if v is not None else "0"
    except:
        return str(v) if v is not None else "0"

def sni(v):
    try:
        return f"{int(v):,}" if v is not None else "0"
    except:
        return str(v) if v is not None else "0"

def kpi(label, value, style="kpi-value"):
    return f'<div class="kpi-card"><div class="{style}">{value}</div><div class="kpi-label">{label}</div></div>'

DB = "HEDIS_QUALITY_DB.CLAIMS_DATA"

# ============================================================
# HEADER + SIDEBAR
# ============================================================
st.markdown("""
<div class="hero-header">
    <h1>🎯 Risk Adjustment Intelligence</h1>
    <p>HCC Coding · RAF Optimization · Revenue Recovery</p>
    <div class="author">Designed & Built by Sadaf Pasha</div>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### 🏥 Horizon Health Advantage")
    st.caption("Contract H1234 · HMO-POS · IL/IN/WI")
    st.divider()
    try:
        plan = run_query(f"""
            SELECT
                (SELECT COUNT(DISTINCT MEMBER_ID) FROM {DB}.MMR_ENROLLMENT),
                (SELECT ROUND(AVG(CALCULATED_RAF),3) FROM {DB}.V_MEMBER_RAF_SCORE),
                (SELECT COUNT(*) FROM {DB}.V_RECAPTURE_OPPORTUNITIES),
                (SELECT COUNT(*) FROM {DB}.V_SUSPECT_WORKLIST)
        """)
        st.metric("Members", sni(plan[0][0]))
        st.metric("Avg RAF", f"{plan[0][1]}")
        st.metric("Recapture Opps", sni(plan[0][2]))
        st.metric("Suspect Conditions", sni(plan[0][3]))
    except:
        st.info("Connecting...")
    st.divider()
    st.markdown("**$11,826** per member/yr at RAF 1.0")
    st.divider()
    st.markdown("""<div style="background:rgba(139,92,246,0.1); border:1px solid rgba(139,92,246,0.3); border-radius:12px; padding:0.8rem;">
        <div style="font-size:0.7rem; color:#8b5cf6 !important; font-weight:700; letter-spacing:1px; text-transform:uppercase;">Built by</div>
        <div style="font-size:0.9rem; color:#e2e8f0 !important; font-weight:600;">Sadaf Pasha</div>
    </div>""", unsafe_allow_html=True)

# ============================================================
# TABS
# ============================================================
tab_dash, tab_yoy, tab_pay, tab_hcc, tab_chat, tab_recap, tab_suspect, tab_prov = st.tabs([
    "📊 RAF Overview", "📈 Year-over-Year", "💰 Payment Summary",
    "🧬 HCC Distribution", "💬 AI Chat",
    "🔄 Recapture", "🔍 Suspects", "🏥 Providers"
])


# ============================================================
# TAB 1: RAF OVERVIEW
# ============================================================
with tab_dash:
    try:
        s = run_query(f"SELECT * FROM {DB}.V_RAF_PLAN_SUMMARY")[0]

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1: st.markdown(kpi("Total Members", sni(s[0])), unsafe_allow_html=True)
        with c2: st.markdown(kpi("Avg RAF Score", sn(s[1],".3f")), unsafe_allow_html=True)
        with c3: st.markdown(kpi("Est. Annual Revenue", f"${sn(s[2])}", "kpi-value-green"), unsafe_allow_html=True)
        with c4: st.markdown(kpi("Recapture at Risk", f"${sn(s[6])}", "kpi-value-red"), unsafe_allow_html=True)
        with c5: st.markdown(kpi("Suspect Opportunity", f"${sn(s[8])}", "kpi-value-amber"), unsafe_allow_html=True)

        st.markdown("")
        col_l, col_r = st.columns([3, 2])

        with col_l:
            st.markdown('<div class="section-header">RAF Score Distribution</div>', unsafe_allow_html=True)
            df_raf = run_query_df(f"""
                SELECT
                    CASE WHEN CALCULATED_RAF<0.5 THEN '< 0.50' WHEN CALCULATED_RAF<1.0 THEN '0.50–0.99'
                         WHEN CALCULATED_RAF<1.5 THEN '1.00–1.49' WHEN CALCULATED_RAF<2.0 THEN '1.50–1.99'
                         WHEN CALCULATED_RAF<2.5 THEN '2.00–2.49' ELSE '2.50+' END AS RAF_BUCKET,
                    CASE WHEN CALCULATED_RAF<0.5 THEN 1 WHEN CALCULATED_RAF<1.0 THEN 2
                         WHEN CALCULATED_RAF<1.5 THEN 3 WHEN CALCULATED_RAF<2.0 THEN 4
                         WHEN CALCULATED_RAF<2.5 THEN 5 ELSE 6 END AS SORT_ORDER,
                    COUNT(*) AS MEMBERS, ROUND(AVG(ESTIMATED_ANNUAL_REVENUE),0) AS AVG_REVENUE
                FROM {DB}.V_MEMBER_RAF_SCORE GROUP BY RAF_BUCKET, SORT_ORDER ORDER BY SORT_ORDER
            """)
            colors_raf = ['#ede9fe','#c4b5fd','#a78bfa','#8b5cf6','#7c3aed','#6d28d9']
            fig = go.Figure(go.Bar(
                x=df_raf["RAF_BUCKET"], y=df_raf["MEMBERS"],
                marker=dict(color=colors_raf[:len(df_raf)]),
                text=[f"<b>{m}</b><br><span style='font-size:9px'>${r:,.0f}/yr</span>" for m,r in zip(df_raf["MEMBERS"], df_raf["AVG_REVENUE"])],
                textposition="outside", textfont=dict(size=10),
            ))
            fig.update_layout(height=350, showlegend=False, margin=dict(l=0,r=0,t=10,b=30),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="DM Sans"), yaxis=dict(gridcolor="#f1f5f9",zeroline=False), bargap=0.25)
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            st.markdown('<div class="section-header">Revenue Opportunity</div>', unsafe_allow_html=True)
            try:
                cur_rev = float(s[2] or 0); recap_v = float(s[6] or 0); susp_v = float(s[8] or 0)
                fig_w = go.Figure(go.Waterfall(
                    orientation="v", measure=["absolute","relative","relative","total"],
                    x=["Current<br>Revenue","Recapture","Suspect","Full<br>Potential"],
                    y=[cur_rev, recap_v, susp_v, cur_rev+recap_v+susp_v],
                    text=[f"${v/1000000:.1f}M" for v in [cur_rev, recap_v, susp_v, cur_rev+recap_v+susp_v]],
                    textposition="outside", textfont=dict(size=10),
                    connector=dict(line=dict(color="#e2e8f0",width=1)),
                    increasing=dict(marker=dict(color="#10b981")),
                    totals=dict(marker=dict(color="#8b5cf6")),
                ))
                fig_w.update_layout(height=350, showlegend=False, margin=dict(l=0,r=0,t=10,b=30),
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="DM Sans"),
                    yaxis=dict(showticklabels=False,showgrid=False,zeroline=False))
                st.plotly_chart(fig_w, use_container_width=True)
            except: st.info("Revenue data not available")

        m1, m2, m3, m4 = st.columns(4)
        with m1: st.metric("Total HCCs Coded", sni(s[3]))
        with m2: st.metric("Avg HCCs/Member", sn(s[4],".1f"))
        with m3: st.metric("Members w/ 0 HCCs", sni(s[9]))
        with m4: st.metric("Members w/ 3+ HCCs", sni(s[10]))

    except Exception as e:
        st.error(f"Error: {e}")


# ============================================================
# TAB 2: YEAR-OVER-YEAR
# ============================================================
with tab_yoy:
    st.markdown('<div class="section-header">📈 Year-over-Year Comparison</div>', unsafe_allow_html=True)
    st.caption("2024 vs 2023 — RAF scores, condition prevalence, and member demographics.")

    try:
        # RAF KPIs
        df_yoy = run_query_df(f"""
            WITH cur AS (
                SELECT COUNT(DISTINCT MEMBER_ID) AS MEMBERS, ROUND(AVG(CALCULATED_RAF),3) AS AVG_RAF,
                       ROUND(SUM(ESTIMATED_ANNUAL_REVENUE),0) AS REVENUE, ROUND(AVG(HCC_COUNT),1) AS AVG_HCCS
                FROM {DB}.V_MEMBER_RAF_SCORE
            ),
            pri AS (
                SELECT COUNT(DISTINCT MEMBER_ID) AS MEMBERS, ROUND(AVG(HCC_RISK_SCORE),3) AS AVG_RAF,
                       ROUND(AVG(HCC_RISK_SCORE)*11826*COUNT(DISTINCT MEMBER_ID),0) AS REVENUE, NULL AS AVG_HCCS
                FROM (SELECT MEMBER_ID, HCC_RISK_SCORE, ROW_NUMBER() OVER (PARTITION BY MEMBER_ID ORDER BY MEMBER_ID) AS RN
                      FROM {DB}.MEMBER_ENROLLMENT) WHERE RN=1
            )
            SELECT 'CY 2023' AS YR, p.MEMBERS, p.AVG_RAF, p.REVENUE, p.AVG_HCCS FROM pri p
            UNION ALL SELECT 'CY 2024', c.MEMBERS, c.AVG_RAF, c.REVENUE, c.AVG_HCCS FROM cur c
        """)

        if len(df_yoy) >= 2:
            p23 = df_yoy.iloc[0]; p24 = df_yoy.iloc[1]
            y1,y2,y3,y4 = st.columns(4)
            with y1:
                d = int(p24["MEMBERS"] or 0) - int(p23["MEMBERS"] or 0)
                st.metric("Members", sni(p24["MEMBERS"]), delta=f"{d:+,}")
            with y2:
                d = float(p24["AVG_RAF"] or 0) - float(p23["AVG_RAF"] or 0)
                st.metric("Avg RAF", sn(p24["AVG_RAF"],".3f"), delta=f"{d:+.3f}")
            with y3:
                d = float(p24["REVENUE"] or 0) - float(p23["REVENUE"] or 0)
                st.metric("Est. Revenue", f"${sn(p24['REVENUE'])}", delta=f"${d/1000000:+.1f}M")
            with y4:
                st.metric("Avg HCCs/Member", sn(p24["AVG_HCCS"],".1f"))

        st.markdown("")

        # RAF distribution comparison
        st.markdown('<div class="section-header">RAF Distribution: 2024 vs 2023</div>', unsafe_allow_html=True)
        df_cmp = run_query_df(f"""
            WITH cur AS (
                SELECT CASE WHEN CALCULATED_RAF<0.5 THEN '< 0.50' WHEN CALCULATED_RAF<1.0 THEN '0.50–0.99'
                            WHEN CALCULATED_RAF<1.5 THEN '1.00–1.49' WHEN CALCULATED_RAF<2.0 THEN '1.50–1.99' ELSE '2.00+' END AS BUCKET,
                       CASE WHEN CALCULATED_RAF<0.5 THEN 1 WHEN CALCULATED_RAF<1.0 THEN 2 WHEN CALCULATED_RAF<1.5 THEN 3 WHEN CALCULATED_RAF<2.0 THEN 4 ELSE 5 END AS S,
                       COUNT(*) AS MEMBERS_2024
                FROM {DB}.V_MEMBER_RAF_SCORE GROUP BY BUCKET, S
            ),
            pri AS (
                SELECT CASE WHEN HCC_RISK_SCORE<0.5 THEN '< 0.50' WHEN HCC_RISK_SCORE<1.0 THEN '0.50–0.99'
                            WHEN HCC_RISK_SCORE<1.5 THEN '1.00–1.49' WHEN HCC_RISK_SCORE<2.0 THEN '1.50–1.99' ELSE '2.00+' END AS BUCKET,
                       CASE WHEN HCC_RISK_SCORE<0.5 THEN 1 WHEN HCC_RISK_SCORE<1.0 THEN 2 WHEN HCC_RISK_SCORE<1.5 THEN 3 WHEN HCC_RISK_SCORE<2.0 THEN 4 ELSE 5 END AS S,
                       COUNT(*) AS MEMBERS_2023
                FROM (SELECT MEMBER_ID, HCC_RISK_SCORE, ROW_NUMBER() OVER (PARTITION BY MEMBER_ID ORDER BY MEMBER_ID) AS RN FROM {DB}.MEMBER_ENROLLMENT) WHERE RN=1
                GROUP BY BUCKET, S
            )
            SELECT COALESCE(c.BUCKET,p.BUCKET) AS BUCKET, COALESCE(c.S,p.S) AS S,
                   COALESCE(p.MEMBERS_2023,0) AS MEMBERS_2023, COALESCE(c.MEMBERS_2024,0) AS MEMBERS_2024
            FROM cur c FULL OUTER JOIN pri p ON c.BUCKET=p.BUCKET ORDER BY COALESCE(c.S,p.S)
        """)
        if not df_cmp.empty:
            fig_c = go.Figure()
            fig_c.add_trace(go.Bar(name="2023", x=df_cmp["BUCKET"], y=df_cmp["MEMBERS_2023"],
                marker_color="#94a3b8", text=df_cmp["MEMBERS_2023"], textposition="outside", textfont=dict(size=10)))
            fig_c.add_trace(go.Bar(name="2024", x=df_cmp["BUCKET"], y=df_cmp["MEMBERS_2024"],
                marker_color="#8b5cf6", text=df_cmp["MEMBERS_2024"], textposition="outside", textfont=dict(size=10)))
            fig_c.update_layout(barmode="group", height=350, margin=dict(l=0,r=0,t=10,b=30),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(family="DM Sans"),
                yaxis=dict(gridcolor="#f1f5f9",zeroline=False),
                legend=dict(orientation="h",y=-0.12,x=0.5,xanchor="center"), bargap=0.3, bargroupgap=0.1)
            st.plotly_chart(fig_c, use_container_width=True)

        st.divider()

        # Prevalence comparison
        st.markdown('<div class="section-header">Condition Prevalence: 2024 vs 2023</div>', unsafe_allow_html=True)
        df_prev = run_query_df(f"""
            WITH m24 AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY MEMBER_ID ORDER BY MEMBER_ID) AS RN FROM {DB}.MEMBER_ENROLLMENT),
                 cur AS (SELECT * FROM m24 WHERE RN=1), tot24 AS (SELECT COUNT(*) AS N FROM cur),
                 m23 AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY MEMBER_ID ORDER BY MEMBER_ID) AS RN FROM {DB}.MEMBER_ENROLLMENT_2023),
                 pri AS (SELECT * FROM m23 WHERE RN=1), tot23 AS (SELECT COUNT(*) AS N FROM pri)
            SELECT 'Diabetes' AS CONDITION,
                   ROUND(SUM(cur.DX_DIABETES)*100.0/(SELECT N FROM tot24),1) AS RATE_2024,
                   ROUND((SELECT SUM(DX_DIABETES) FROM pri)*100.0/(SELECT N FROM tot23),1) AS RATE_2023
            FROM cur
            UNION ALL SELECT 'Hypertension', ROUND(SUM(cur.DX_HYPERTENSION)*100.0/(SELECT N FROM tot24),1),
                   ROUND((SELECT SUM(DX_HYPERTENSION) FROM pri)*100.0/(SELECT N FROM tot23),1) FROM cur
            UNION ALL SELECT 'CHF', ROUND(SUM(cur.DX_CHF)*100.0/(SELECT N FROM tot24),1),
                   ROUND((SELECT SUM(DX_CHF) FROM pri)*100.0/(SELECT N FROM tot23),1) FROM cur
            UNION ALL SELECT 'COPD', ROUND(SUM(cur.DX_COPD)*100.0/(SELECT N FROM tot24),1),
                   ROUND((SELECT SUM(DX_COPD) FROM pri)*100.0/(SELECT N FROM tot23),1) FROM cur
            UNION ALL SELECT 'CKD', ROUND(SUM(cur.DX_CKD)*100.0/(SELECT N FROM tot24),1),
                   ROUND((SELECT SUM(DX_CKD) FROM pri)*100.0/(SELECT N FROM tot23),1) FROM cur
            UNION ALL SELECT 'CAD', ROUND(SUM(cur.DX_CAD)*100.0/(SELECT N FROM tot24),1),
                   ROUND((SELECT SUM(DX_CAD) FROM pri)*100.0/(SELECT N FROM tot23),1) FROM cur
            UNION ALL SELECT 'Depression', ROUND(SUM(cur.DX_DEPRESSION)*100.0/(SELECT N FROM tot24),1),
                   ROUND((SELECT SUM(DX_DEPRESSION) FROM pri)*100.0/(SELECT N FROM tot23),1) FROM cur
            UNION ALL SELECT 'Obesity', ROUND(SUM(cur.DX_OBESITY)*100.0/(SELECT N FROM tot24),1),
                   ROUND((SELECT SUM(DX_OBESITY) FROM pri)*100.0/(SELECT N FROM tot23),1) FROM cur
            ORDER BY RATE_2024 DESC
        """)
        if not df_prev.empty:
            df_prev["CHANGE"] = df_prev["RATE_2024"] - df_prev["RATE_2023"]
            fig_p = go.Figure()
            fig_p.add_trace(go.Bar(name="2023", x=df_prev["CONDITION"], y=df_prev["RATE_2023"],
                marker_color="#94a3b8", text=df_prev["RATE_2023"].apply(lambda x: f"{x}%"), textposition="outside", textfont=dict(size=10)))
            fig_p.add_trace(go.Bar(name="2024", x=df_prev["CONDITION"], y=df_prev["RATE_2024"],
                marker_color="#8b5cf6", text=df_prev["RATE_2024"].apply(lambda x: f"{x}%"), textposition="outside", textfont=dict(size=10)))
            fig_p.update_layout(barmode="group", height=380, margin=dict(l=0,r=0,t=10,b=30),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(family="DM Sans"),
                yaxis=dict(title="Prevalence %",gridcolor="#f1f5f9",zeroline=False),
                legend=dict(orientation="h",y=-0.12,x=0.5,xanchor="center"), bargap=0.3, bargroupgap=0.1)
            st.plotly_chart(fig_p, use_container_width=True)

            st.dataframe(df_prev, use_container_width=True, hide_index=True,
                column_config={
                    "RATE_2023": st.column_config.NumberColumn("2023 %", format="%.1f%%"),
                    "RATE_2024": st.column_config.NumberColumn("2024 %", format="%.1f%%"),
                    "CHANGE": st.column_config.NumberColumn("Change (pp)", format="%+.1f"),
                })

    except Exception as e:
        st.error(f"Error: {e}")


# ============================================================
# TAB 3: PAYMENT SUMMARY
# ============================================================
with tab_pay:
    st.markdown('<div class="section-header">💰 CMS Payment Summary</div>', unsafe_allow_html=True)
    try:
        # Annual scenarios
        st.markdown("**Annual Revenue by Scenario**")
        df_a = run_query_df(f"SELECT * FROM {DB}.V_ANNUAL_PAYMENT_SUMMARY ORDER BY SORT_ORDER")
        if not df_a.empty:
            acols = st.columns(len(df_a))
            styles = ["kpi-value","kpi-value-green","kpi-value-amber","kpi-value-green"]
            for i, (col, row) in enumerate(zip(acols, df_a.itertuples())):
                with col: st.markdown(kpi(row.SCENARIO, f"${sn(row.ANNUAL_REVENUE)}", styles[min(i,3)]), unsafe_allow_html=True)

        st.divider()

        # Monthly trend
        st.markdown("**Monthly Payment Trend**")
        st.caption("Purple = CMS Accepted · Green = Claims Forecast · Amber = Full Potential")
        df_m = run_query_df(f"SELECT * FROM {DB}.V_MONTHLY_PAYMENT_SCHEDULE ORDER BY PAYMENT_MONTH")
        if not df_m.empty:
            fig_m = go.Figure()
            fig_m.add_trace(go.Scatter(x=df_m["PAYMENT_MONTH"], y=df_m["ACCEPTED_MONTHLY_REVENUE"],
                name="CMS Accepted", mode="lines+markers", line=dict(color="#8b5cf6",width=3), marker=dict(size=6)))
            fig_m.add_trace(go.Scatter(x=df_m["PAYMENT_MONTH"], y=df_m["CLAIMS_MONTHLY_REVENUE"],
                name="Claims Forecast", mode="lines+markers", line=dict(color="#10b981",width=2,dash="dash"), marker=dict(size=5)))
            fig_m.add_trace(go.Scatter(x=df_m["PAYMENT_MONTH"], y=df_m["TOTAL_POTENTIAL_MONTHLY_REVENUE"],
                name="Full Potential", mode="lines+markers", line=dict(color="#f59e0b",width=2,dash="dot"), marker=dict(size=4)))
            fig_m.update_layout(height=350, margin=dict(l=0,r=0,t=10,b=30),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(family="DM Sans"),
                yaxis=dict(title="Monthly Revenue ($)", gridcolor="#f1f5f9", zeroline=False),
                legend=dict(orientation="h",y=-0.15,x=0.5,xanchor="center"))
            st.plotly_chart(fig_m, use_container_width=True)

            st.dataframe(df_m, use_container_width=True, hide_index=True,
                column_config={
                    "ACCEPTED_MONTHLY_REVENUE": st.column_config.NumberColumn("Accepted $", format="$%,.0f"),
                    "CLAIMS_MONTHLY_REVENUE": st.column_config.NumberColumn("Claims $", format="$%,.0f"),
                    "MONTHLY_CODING_GAP": st.column_config.NumberColumn("Gap $", format="$%,.0f"),
                    "TOTAL_POTENTIAL_MONTHLY_REVENUE": st.column_config.NumberColumn("Potential $", format="$%,.0f"),
                })

        st.divider()
        st.markdown("**Sweep Payment Timeline**")
        st.caption("Initial → Jan-Jun prospective · Final → Jul-Dec retroactive adjustment")
        df_tl = run_query_df(f"SELECT * FROM {DB}.V_PAYMENT_TIMELINE ORDER BY PAYMENT_YEAR, SUBMISSION_DEADLINE")
        if not df_tl.empty:
            st.dataframe(df_tl, use_container_width=True, hide_index=True,
                column_config={
                    "REVENUE_CAPTURED": st.column_config.NumberColumn("Captured $", format="$%,.0f"),
                    "REVENUE_REJECTED": st.column_config.NumberColumn("Rejected $", format="$%,.0f"),
                    "DAYS_REMAINING": st.column_config.NumberColumn("Days Left", format="%d"),
                })

    except Exception as e:
        st.error(f"Error: {e}")


# ============================================================
# TAB 4: HCC DISTRIBUTION
# ============================================================
with tab_hcc:
    st.markdown('<div class="section-header">🧬 HCC Distribution</div>', unsafe_allow_html=True)
    try:
        df_hcc = run_query_df(f"""
            SELECT HCC_CATEGORY, HCC_CODE, HCC_DESCRIPTION, RAF_WEIGHT,
                   MEMBERS_CODED_2024, MEMBERS_NEED_RECAPTURE, MEMBERS_SUSPECT
            FROM {DB}.V_HCC_DISTRIBUTION ORDER BY RAF_WEIGHT DESC
        """)
        st.dataframe(df_hcc, use_container_width=True, hide_index=True,
            column_config={
                "RAF_WEIGHT": st.column_config.NumberColumn("RAF", format="%.3f"),
                "MEMBERS_CODED_2024": st.column_config.NumberColumn("Coded", format="%d"),
                "MEMBERS_NEED_RECAPTURE": st.column_config.NumberColumn("Recapture", format="%d"),
                "MEMBERS_SUSPECT": st.column_config.NumberColumn("Suspect", format="%d"),
            })

        df_cat = run_query_df(f"""
            SELECT HCC_CATEGORY, SUM(MEMBERS_CODED_2024) AS CODED,
                   SUM(MEMBERS_NEED_RECAPTURE) AS RECAPTURE, SUM(MEMBERS_SUSPECT) AS SUSPECT
            FROM {DB}.V_HCC_DISTRIBUTION GROUP BY HCC_CATEGORY ORDER BY CODED DESC
        """)
        fig_cat = go.Figure()
        fig_cat.add_trace(go.Bar(name="Coded", x=df_cat["HCC_CATEGORY"], y=df_cat["CODED"], marker_color="#8b5cf6"))
        fig_cat.add_trace(go.Bar(name="Recapture", x=df_cat["HCC_CATEGORY"], y=df_cat["RECAPTURE"], marker_color="#ef4444"))
        fig_cat.add_trace(go.Bar(name="Suspect", x=df_cat["HCC_CATEGORY"], y=df_cat["SUSPECT"], marker_color="#f59e0b"))
        fig_cat.update_layout(barmode="group", height=350, margin=dict(l=0,r=0,t=10,b=30),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(family="DM Sans"),
            yaxis=dict(gridcolor="#f1f5f9",zeroline=False),
            legend=dict(orientation="h",y=-0.15,x=0.5,xanchor="center"))
        st.plotly_chart(fig_cat, use_container_width=True)
    except Exception as e:
        st.error(f"Error: {e}")


# ============================================================
# TAB 5: AI CHAT
# ============================================================
with tab_chat:
    st.markdown('<div class="section-header">💬 Ask About Risk Adjustment</div>', unsafe_allow_html=True)

    qcols = st.columns(3)
    questions = [
        ("📊","What is our plan-level RAF and total revenue?"),
        ("🔄","Which HCCs have the most recapture opportunities?"),
        ("🔍","How many members have suspect conditions?"),
        ("🏥","Which provider groups have the most revenue at risk?"),
        ("💰","If we recapture 70% of dropped codes, how much revenue?"),
        ("📈","How does our 2024 RAF compare to 2023?"),
    ]
    def set_question(q): st.session_state["ra_pending"] = q
    for i, (icon, q) in enumerate(questions):
        with qcols[i % 3]:
            st.button(f"{icon} {q}", key=f"q_{i}", use_container_width=True, on_click=set_question, args=(q,))

    st.divider()
    if "ra_messages" not in st.session_state: st.session_state.ra_messages = []
    for msg in st.session_state.ra_messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])
            if "dataframe" in msg: st.dataframe(msg["dataframe"], use_container_width=True, hide_index=True)

    typed_prompt = st.chat_input("Ask about HCC coding, RAF, payments, recapture, suspects...")
    prompt = None
    if "ra_pending" in st.session_state: prompt = st.session_state.pop("ra_pending")
    elif typed_prompt: prompt = typed_prompt

    if prompt:
        st.session_state.ra_messages.append({"role":"user","content":prompt})
        with st.chat_message("user"): st.write(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                try:
                    p = run_query(f"SELECT * FROM {DB}.V_RAF_PLAN_SUMMARY")[0]
                    ctx = (f"Plan: {sn(p[0])} members, avg RAF {sn(p[1],'.3f')}, revenue ${sn(p[2])}. "
                        f"HCCs: {sn(p[3])}, avg/member: {sn(p[4],'.1f')}. "
                        f"Recapture: {sn(p[5])} opps, ${sn(p[6])} at risk. Suspects: {sn(p[7])}, ${sn(p[8])} opp.")
                    fp = f"""You are an MA risk adjustment expert. {ctx}
CMS pays ~$11,826/member/year at RAF 1.0. Sweeps: PY2025 H1 due 2025-01-31, H2 2025-03-31, H3 2025-08-31.
Question: {prompt}
Be concise. Use specific numbers."""
                    resp = run_query(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet','{fp.replace(chr(39),chr(39)+chr(39))}')")
                    answer = resp[0][0] if resp else "Could not generate response."
                    st.write(answer)
                    msg = {"role":"assistant","content":answer}
                    kws = ["show","list","member","who","which","top","how many","count","provider","hcc","recapture","suspect","payment","sweep"]
                    if any(k in prompt.lower() for k in kws):
                        try:
                            sq = f"""Write Snowflake SQL for: "{prompt}"
Tables ({DB}): V_MEMBER_RAF_SCORE, V_RECAPTURE_OPPORTUNITIES, V_SUSPECT_WORKLIST, V_PROVIDER_CODING_SCORECARD, V_HCC_DISTRIBUTION, V_MONTHLY_PAYMENT_SCHEDULE, V_ANNUAL_PAYMENT_SUMMARY, V_PAYMENT_TIMELINE, MEMBER_ENROLLMENT, MEMBER_ENROLLMENT_2023. Fully qualified names. LIMIT 25. ONLY SQL."""
                            sr = run_query(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet','{sq.replace(chr(39),chr(39)+chr(39))}')")
                            sql = sr[0][0].strip().replace("```sql","").replace("```SQL","").replace("```","").strip()
                            if sql.upper().startswith("SELECT"):
                                df = run_query_df(sql)
                                if not df.empty:
                                    st.divider(); st.caption("📋 Supporting Data")
                                    st.dataframe(df, use_container_width=True, hide_index=True)
                                    msg["dataframe"] = df
                        except: pass
                    st.session_state.ra_messages.append(msg)
                except Exception as e: st.error(f"Error: {e}")


# ============================================================
# TAB 6: RECAPTURE
# ============================================================
with tab_recap:
    st.markdown('<div class="section-header">🔄 Recapture Worklist</div>', unsafe_allow_html=True)
    st.caption("Conditions documented previously but not yet coded in current year EDS.")
    try:
        rs = run_query(f"""SELECT COUNT(*), COUNT(DISTINCT MEMBER_ID),
            ROUND(SUM(REVENUE_AT_RISK),0), COUNT(CASE WHEN RECAPTURE_PRIORITY='CRITICAL' THEN 1 END)
            FROM {DB}.V_RECAPTURE_OPPORTUNITIES""")
        r1,r2,r3,r4 = st.columns(4)
        with r1: st.markdown(kpi("Opps", sni(rs[0][0])), unsafe_allow_html=True)
        with r2: st.markdown(kpi("Members", sni(rs[0][1])), unsafe_allow_html=True)
        with r3: st.markdown(kpi("Revenue at Risk", f"${sni(rs[0][2])}", "kpi-value-red"), unsafe_allow_html=True)
        with r4: st.markdown(kpi("Critical", sni(rs[0][3]), "kpi-value-red"), unsafe_allow_html=True)

        st.markdown("")
        rf1, rf2 = st.columns(2)
        with rf1: rpri = st.selectbox("Priority:", ["All","CRITICAL","HIGH","MEDIUM","LOW"], key="rpri")
        with rf2: rhcc = st.selectbox("HCC:", ["All"]+[r[0] for r in run_query(f"SELECT DISTINCT HCC_CODE FROM {DB}.V_RECAPTURE_OPPORTUNITIES ORDER BY 1")], key="rhcc")
        rw = []
        if rpri != "All": rw.append(f"RECAPTURE_PRIORITY='{rpri}'")
        if rhcc != "All": rw.append(f"HCC_CODE='{rhcc}'")
        rwhere = " AND "+" AND ".join(rw) if rw else ""
        df_r = run_query_df(f"""SELECT MEMBER_ID, LAST_NAME, FIRST_NAME, AGE, PCP_GROUP,
            HCC_CODE, HCC_DESCRIPTION, RAF_WEIGHT, REVENUE_AT_RISK, RECAPTURE_PRIORITY,
            TARGET_SWEEP, DAYS_UNTIL_DEADLINE, SWEEP_URGENCY
            FROM {DB}.V_RECAPTURE_OPPORTUNITIES WHERE 1=1 {rwhere} ORDER BY RAF_WEIGHT DESC LIMIT 50""")
        st.dataframe(df_r, use_container_width=True, hide_index=True,
            column_config={"RAF_WEIGHT":st.column_config.NumberColumn("RAF",format="%.3f"),
                "REVENUE_AT_RISK":st.column_config.NumberColumn("Revenue $",format="$%,.0f")})
    except Exception as e: st.error(f"Error: {e}")


# ============================================================
# TAB 7: SUSPECTS
# ============================================================
with tab_suspect:
    st.markdown('<div class="section-header">🔍 Suspect Conditions</div>', unsafe_allow_html=True)
    st.caption("Rx/lab/claims evidence suggesting an undiagnosed or undercoded condition.")
    try:
        ss = run_query(f"""SELECT COUNT(*), COUNT(DISTINCT MEMBER_ID),
            ROUND(SUM(EST_REVENUE_IMPACT),0), COUNT(CASE WHEN PRIORITY='HIGH' THEN 1 END)
            FROM {DB}.V_SUSPECT_WORKLIST""")
        s1,s2,s3,s4 = st.columns(4)
        with s1: st.markdown(kpi("Suspects", sni(ss[0][0])), unsafe_allow_html=True)
        with s2: st.markdown(kpi("Members", sni(ss[0][1])), unsafe_allow_html=True)
        with s3: st.markdown(kpi("Est. Revenue", f"${sni(ss[0][2])}", "kpi-value-amber"), unsafe_allow_html=True)
        with s4: st.markdown(kpi("High Priority", sni(ss[0][3]), "kpi-value-amber"), unsafe_allow_html=True)

        st.markdown("")
        sf1, sf2 = st.columns(2)
        with sf1: spri = st.selectbox("Priority:", ["All","HIGH","MEDIUM"], key="spri")
        with sf2: ssrc = st.selectbox("Evidence:", ["All","PHARMACY","LAB","CLAIMS"], key="ssrc")
        sw = []
        if spri != "All": sw.append(f"PRIORITY='{spri}'")
        if ssrc != "All": sw.append(f"EVIDENCE_SOURCE='{ssrc}'")
        swhere = " AND "+" AND ".join(sw) if sw else ""
        df_s = run_query_df(f"""SELECT MEMBER_ID, LAST_NAME, FIRST_NAME, AGE, PCP_GROUP,
            SUSPECT_HCC, HCC_DESCRIPTION, EVIDENCE_SOURCE, EVIDENCE_DETAIL,
            PRIORITY, CONFIDENCE_SCORE, EST_REVENUE_IMPACT,
            TARGET_SWEEP, DAYS_UNTIL_DEADLINE, SWEEP_URGENCY
            FROM {DB}.V_SUSPECT_WORKLIST WHERE 1=1 {swhere} ORDER BY CONFIDENCE_SCORE DESC LIMIT 50""")
        st.dataframe(df_s, use_container_width=True, hide_index=True,
            column_config={"CONFIDENCE_SCORE":st.column_config.ProgressColumn("Confidence",min_value=0,max_value=1,format="%.0f%%"),
                "EST_REVENUE_IMPACT":st.column_config.NumberColumn("Revenue $",format="$%,.0f")})
    except Exception as e: st.error(f"Error: {e}")


# ============================================================
# TAB 8: PROVIDERS
# ============================================================
with tab_prov:
    st.markdown('<div class="section-header">🏥 Provider Coding Scorecard</div>', unsafe_allow_html=True)
    try:
        df_p = run_query_df(f"SELECT * FROM {DB}.V_PROVIDER_CODING_SCORECARD ORDER BY TOTAL_REVENUE_AT_RISK DESC")
        st.dataframe(df_p, use_container_width=True, hide_index=True,
            column_config={
                "PANEL_SIZE":st.column_config.NumberColumn("Panel",format="%d"),
                "AVG_RAF":st.column_config.NumberColumn("Avg RAF",format="%.3f"),
                "AVG_HCCS_PER_MEMBER":st.column_config.NumberColumn("Avg HCCs",format="%.1f"),
                "TOTAL_REVENUE_AT_RISK":st.column_config.NumberColumn("Recapture $",format="$%,.0f"),
                "TOTAL_SUSPECT_REVENUE":st.column_config.NumberColumn("Suspect $",format="$%,.0f"),
            })
        fig_pv = px.scatter(df_p, x="AVG_RAF", y="TOTAL_REVENUE_AT_RISK",
            size="PANEL_SIZE", color="AVG_HCCS_PER_MEMBER", hover_name="PCP_GROUP",
            color_continuous_scale=["#c4b5fd","#8b5cf6","#4c1d95"], size_max=35)
        fig_pv.update_layout(height=400, margin=dict(l=0,r=0,t=10,b=30),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)", font=dict(family="DM Sans"),
            xaxis=dict(title="Avg RAF",gridcolor="#f1f5f9"),
            yaxis=dict(title="Revenue at Risk ($)",gridcolor="#f1f5f9"))
        st.plotly_chart(fig_pv, use_container_width=True)
    except Exception as e: st.error(f"Error: {e}")


# ============================================================
# FOOTER
# ============================================================
st.markdown("""<div class="footer">
    🎯 Risk Adjustment Intelligence · <a href="https://www.anthropic.com">Claude AI</a> +
    <a href="https://www.snowflake.com">Snowflake</a> +
    <a href="https://streamlit.io">Streamlit</a> · Designed & Built by Sadaf Pasha
</div>""", unsafe_allow_html=True)
