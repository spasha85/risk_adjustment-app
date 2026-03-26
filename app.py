import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Risk Adjustment Intelligence",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# CUSTOM CSS — Dark clinical intelligence aesthetic
# ============================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700;800&family=DM+Mono:wght@400;500&display=swap');
    
    .stApp { font-family: 'DM Sans', sans-serif; }
    
    .hero-header {
        background: linear-gradient(135deg, #0c0f1a 0%, #1a1f3a 40%, #2d1b4e 70%, #1a1f3a 100%);
        padding: 2rem 2.5rem;
        border-radius: 20px;
        margin-bottom: 1.5rem;
        position: relative;
        overflow: hidden;
        border: 1px solid rgba(139,92,246,0.15);
    }
    .hero-header::before {
        content: '';
        position: absolute;
        top: -50%; right: -20%;
        width: 400px; height: 400px;
        background: radial-gradient(circle, rgba(139,92,246,0.15) 0%, transparent 70%);
        border-radius: 50%;
    }
    .hero-header h1 {
        color: #ffffff;
        font-size: 2.2rem;
        font-weight: 800;
        margin: 0;
        letter-spacing: -0.5px;
    }
    .hero-header p {
        color: #94a3b8;
        font-size: 1rem;
        margin-top: 0.3rem;
    }
    .hero-header .author {
        color: #8b5cf6;
        font-size: 0.8rem;
        font-weight: 600;
        margin-top: 0.5rem;
        letter-spacing: 1px;
        text-transform: uppercase;
    }
    
    .kpi-card {
        background: linear-gradient(145deg, #ffffff 0%, #f8fafc 100%);
        border: 1px solid #e2e8f0;
        border-radius: 16px;
        padding: 1.5rem;
        text-align: center;
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
        transition: transform 0.2s, box-shadow 0.2s;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 25px -5px rgba(0,0,0,0.1);
    }
    .kpi-value {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(135deg, #8b5cf6, #6d28d9);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .kpi-value-green {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(135deg, #10b981, #059669);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .kpi-value-amber {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(135deg, #f59e0b, #d97706);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .kpi-value-red {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(135deg, #ef4444, #dc2626);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        line-height: 1.2;
    }
    .kpi-label {
        font-size: 0.8rem;
        font-weight: 600;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-top: 0.5rem;
    }
    
    .section-header {
        font-size: 1.3rem;
        font-weight: 700;
        color: #0f172a;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 3px solid #8b5cf6;
        display: inline-block;
    }
    
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0c0f1a 0%, #1a1f3a 100%);
    }
    [data-testid="stSidebar"] * { color: #e2e8f0 !important; }
    
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { border-radius: 12px; padding: 10px 20px; font-weight: 600; }
    .stDataFrame { border-radius: 12px; overflow: hidden; }
    
    .footer {
        text-align: center; padding: 1.5rem; color: #94a3b8;
        font-size: 0.8rem; border-top: 1px solid #e2e8f0; margin-top: 2rem;
    }
    .footer a { color: #8b5cf6; text-decoration: none; }
</style>
""", unsafe_allow_html=True)


# ============================================================
# CONNECTION — Snowflake Native Streamlit (no secrets needed)
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
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def run_query_df(query):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=columns)

CHART_COLORS = ["#8b5cf6","#6d28d9","#10b981","#f59e0b","#ef4444","#06b6d4","#ec4899","#84cc16","#f97316"]

# Null-safe number formatter
def sn(v, fmt=",.0f"):
    """Safe number format — handles None, NaN, and non-numeric values"""
    try:
        if v is None: return "0"
        return f"{float(v):{fmt}}"
    except:
        return str(v) if v is not None else "0"

def sni(v):
    """Safe integer format with commas"""
    try:
        if v is None: return "0"
        return f"{int(v):,}"
    except:
        return str(v) if v is not None else "0"


# ============================================================
# HERO HEADER
# ============================================================
st.markdown("""
<div class="hero-header">
    <h1>🎯 Risk Adjustment Intelligence</h1>
    <p>HCC Coding Analytics · RAF Optimization · Revenue Recovery — Powered by Snowflake + Claude AI</p>
    <div class="author">Designed & Built by Sadaf Pasha</div>
</div>
""", unsafe_allow_html=True)


# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("### 🏥 Horizon Health Advantage")
    st.caption("Contract H1234 · HMO-POS · IL, IN, WI")
    st.divider()
    
    try:
        plan = run_query("""
            SELECT
                (SELECT COUNT(*) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.MEMBER_ENROLLMENT),
                (SELECT ROUND(AVG(CALCULATED_RAF),3) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_MEMBER_RAF_SCORE),
                (SELECT COUNT(*) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_RECAPTURE_OPPORTUNITIES),
                (SELECT COUNT(*) FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_SUSPECT_WORKLIST)
        """)
        st.metric("👥 Total Members", sni(plan[0][0]))
        st.metric("📊 Plan Avg RAF", f"{plan[0][1]}")
        st.metric("🔄 Recapture Opps", sni(plan[0][2]))
        st.metric("🔍 Suspect Conditions", sni(plan[0][3]))
        
        st.divider()
        st.markdown("### 💰 Revenue Context")
        st.markdown("**$11,826** avg per-member CMS payment at 1.0 RAF")
    except:
        st.info("Connecting to database...")
    
    st.divider()
    st.markdown("""
    <div style="background:rgba(139,92,246,0.1); border:1px solid rgba(139,92,246,0.3); border-radius:12px; padding:1rem; margin-top:0.5rem;">
        <div style="font-size:0.75rem; color:#8b5cf6 !important; font-weight:700; letter-spacing:1px; text-transform:uppercase;">Built by</div>
        <div style="font-size:0.95rem; color:#e2e8f0 !important; font-weight:600; margin-top:2px;">Sadaf Pasha</div>
    </div>
    """, unsafe_allow_html=True)


# ============================================================
# TABS
# ============================================================
tab_dash, tab_chat, tab_sweeps, tab_recapture, tab_suspect, tab_providers, tab_simulator, tab_hcc = st.tabs([
    "📊 RAF Overview", "💬 AI Chat", "📅 Sweep Tracker", "🔄 Recapture", "🔍 Suspects", "🏥 Providers", "💰 Simulator", "🧬 HCC Distribution"
])


# ============================================================
# TAB 1: RAF OVERVIEW DASHBOARD
# ============================================================
with tab_dash:
    try:
        summary = run_query("SELECT * FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_RAF_PLAN_SUMMARY")
        s = summary[0]
        # s: TOTAL_MEMBERS, AVG_RAF, TOTAL_ESTIMATED_REVENUE, TOTAL_HCCS_CODED,
        #    AVG_HCCS_PER_MEMBER, TOTAL_RECAPTURE_OPPS, RECAPTURE_REVENUE_AT_RISK,
        #    TOTAL_SUSPECT_OPPS, SUSPECT_REVENUE_OPPORTUNITY, MEMBERS_NO_HCCS, MEMBERS_3_PLUS_HCCS

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value">{sni(s[0])}</div><div class="kpi-label">Total Members</div></div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value">{s[1]}</div><div class="kpi-label">Avg RAF Score</div></div>', unsafe_allow_html=True)
        with c3:
            rev = f"${s[2]/1000000:.1f}M" if s[2] else "$0"
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-green">{rev}</div><div class="kpi-label">Est. Annual Revenue</div></div>', unsafe_allow_html=True)
        with c4:
            recap_rev = f"${s[6]/1000000:.1f}M" if s[6] else "$0"
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-red">{recap_rev}</div><div class="kpi-label">Recapture Revenue at Risk</div></div>', unsafe_allow_html=True)
        with c5:
            suspect_rev = f"${s[8]/1000000:.1f}M" if s[8] else "$0"
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-amber">{suspect_rev}</div><div class="kpi-label">Suspect Revenue Opportunity</div></div>', unsafe_allow_html=True)

        st.markdown("")
        col_left, col_right = st.columns([3, 2])

        with col_left:
            st.markdown('<div class="section-header">📊 RAF Score Distribution</div>', unsafe_allow_html=True)
            df_raf = run_query_df("""
                SELECT
                    CASE
                        WHEN CALCULATED_RAF < 0.5 THEN 'Under 0.5'
                        WHEN CALCULATED_RAF < 1.0 THEN '0.5 - 0.99'
                        WHEN CALCULATED_RAF < 1.5 THEN '1.0 - 1.49'
                        WHEN CALCULATED_RAF < 2.0 THEN '1.5 - 1.99'
                        ELSE '2.0+'
                    END AS RAF_BUCKET,
                    COUNT(*) AS MEMBERS,
                    ROUND(AVG(ESTIMATED_ANNUAL_REVENUE), 0) AS AVG_REVENUE
                FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_MEMBER_RAF_SCORE
                GROUP BY RAF_BUCKET
                ORDER BY RAF_BUCKET
            """)
            fig_raf = px.bar(df_raf, x="RAF_BUCKET", y="MEMBERS", color="MEMBERS",
                             color_continuous_scale=["#c4b5fd","#8b5cf6","#6d28d9","#4c1d95"],
                             text="MEMBERS")
            fig_raf.update_layout(height=400, showlegend=False,
                margin=dict(l=10,r=10,t=10,b=10),
                plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font=dict(family="DM Sans"),
                xaxis_title="RAF Score Range", yaxis_title="Members",
                yaxis=dict(gridcolor="#f1f5f9"),
                coloraxis_showscale=False)
            fig_raf.update_traces(textposition="outside")
            st.plotly_chart(fig_raf, use_container_width=True)

        with col_right:
            st.markdown('<div class="section-header">🔄 Recapture by Priority</div>', unsafe_allow_html=True)
            df_rev = run_query_df("SELECT * FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_REVENUE_SIMULATOR")
            fig_rev = px.pie(df_rev, values="TOTAL_REVENUE_AT_RISK", names="RECAPTURE_PRIORITY",
                             hole=0.55, color_discrete_sequence=["#ef4444","#f59e0b","#8b5cf6","#94a3b8"])
            fig_rev.update_layout(height=400, margin=dict(l=10,r=10,t=30,b=10),
                                 font=dict(family="DM Sans"), paper_bgcolor="rgba(0,0,0,0)")
            fig_rev.update_traces(textposition='inside', textinfo='percent+label', textfont_size=11)
            st.plotly_chart(fig_rev, use_container_width=True)

        # Quick stats row
        st.markdown('<div class="section-header">📋 Key Metrics</div>', unsafe_allow_html=True)
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.metric("Total HCCs Coded", f"{sni(s[3])}")
        with m2:
            st.metric("Avg HCCs/Member", f"{s[4]}")
        with m3:
            st.metric("Members with 0 HCCs", f"{sni(s[9])}")
        with m4:
            st.metric("Members with 3+ HCCs", f"{sni(s[10])}")

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 3: SWEEP TRACKER
# ============================================================
with tab_sweeps:
    st.markdown('<div class="section-header">📅 CMS Payment Sweep Tracker</div>', unsafe_allow_html=True)
    st.caption("Track submissions, revenue, and deadlines across CMS risk adjustment payment sweeps.")

    try:
        # Sweep deadlines reference
        st.markdown("**📋 CMS Sweep Schedule**")
        df_sweeps = run_query_df("""
            SELECT PAYMENT_YEAR, SWEEP_NUMBER, SWEEP_NAME, SUBMISSION_DEADLINE,
                   DOS_START, DOS_END, DESCRIPTION
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.CMS_SWEEP_DATES
            ORDER BY PAYMENT_YEAR, SUBMISSION_DEADLINE
        """)
        st.dataframe(df_sweeps, use_container_width=True, hide_index=True)

        st.divider()

        # Revenue by sweep
        st.markdown('<div class="section-header">💰 Revenue Captured by Sweep</div>', unsafe_allow_html=True)
        try:
            df_sweep_rev = run_query_df("""
                SELECT PAYMENT_YEAR, SWEEP_NAME, SUBMISSION_DEADLINE,
                       TOTAL_SUBMISSIONS, ACCEPTED, REJECTED, PENDING,
                       UNIQUE_MEMBERS, UNIQUE_HCCS,
                       TOTAL_ACCEPTED_REVENUE, REJECTED_REVENUE_LOST, PENDING_REVENUE
                FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_SWEEP_REVENUE_SUMMARY
                ORDER BY PAYMENT_YEAR, SUBMISSION_DEADLINE
            """)
            if not df_sweep_rev.empty:
                total_accepted = df_sweep_rev["TOTAL_ACCEPTED_REVENUE"].sum()
                total_rejected = df_sweep_rev["REJECTED_REVENUE_LOST"].sum()
                total_pending = df_sweep_rev["PENDING_REVENUE"].sum()

                sk1, sk2, sk3, sk4 = st.columns(4)
                with sk1:
                    st.markdown(f'<div class="kpi-card"><div class="kpi-value-green">${sn(total_accepted)}</div><div class="kpi-label">Accepted Revenue</div></div>', unsafe_allow_html=True)
                with sk2:
                    st.markdown(f'<div class="kpi-card"><div class="kpi-value-red">${sn(total_rejected)}</div><div class="kpi-label">Rejected Revenue Lost</div></div>', unsafe_allow_html=True)
                with sk3:
                    st.markdown(f'<div class="kpi-card"><div class="kpi-value-amber">${sn(total_pending)}</div><div class="kpi-label">Pending Revenue</div></div>', unsafe_allow_html=True)
                with sk4:
                    reject_rate = round(df_sweep_rev["REJECTED"].sum() / max(df_sweep_rev["TOTAL_SUBMISSIONS"].sum(),1) * 100, 1)
                    st.markdown(f'<div class="kpi-card"><div class="kpi-value-red">{reject_rate}%</div><div class="kpi-label">Rejection Rate</div></div>', unsafe_allow_html=True)

                st.markdown("")
                st.dataframe(df_sweep_rev, use_container_width=True, hide_index=True,
                    column_config={
                        "TOTAL_ACCEPTED_REVENUE": st.column_config.NumberColumn("Accepted $", format="$%,.0f"),
                        "REJECTED_REVENUE_LOST": st.column_config.NumberColumn("Rejected $", format="$%,.0f"),
                        "PENDING_REVENUE": st.column_config.NumberColumn("Pending $", format="$%,.0f"),
                    })

                fig_sweep = go.Figure()
                fig_sweep.add_trace(go.Bar(name="Accepted", x=df_sweep_rev["SWEEP_NAME"],
                    y=df_sweep_rev["TOTAL_ACCEPTED_REVENUE"], marker_color="#10b981"))
                fig_sweep.add_trace(go.Bar(name="Rejected", x=df_sweep_rev["SWEEP_NAME"],
                    y=df_sweep_rev["REJECTED_REVENUE_LOST"], marker_color="#ef4444"))
                fig_sweep.add_trace(go.Bar(name="Pending", x=df_sweep_rev["SWEEP_NAME"],
                    y=df_sweep_rev["PENDING_REVENUE"], marker_color="#f59e0b"))
                fig_sweep.update_layout(barmode="group", height=400,
                    margin=dict(l=10,r=10,t=30,b=10),
                    plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="DM Sans"),
                    yaxis=dict(title="Revenue ($)", gridcolor="#f1f5f9"),
                    legend=dict(orientation="h", y=-0.15))
                st.plotly_chart(fig_sweep, use_container_width=True)
            else:
                st.info("No sweep revenue data available yet.")
        except Exception as e:
            st.info(f"Sweep revenue view not available: {str(e)}")

        st.divider()

        # EDS submission status
        st.markdown('<div class="section-header">📊 EDS Submission Status</div>', unsafe_allow_html=True)
        try:
            df_eds = run_query_df("""
                SELECT STATUS, COUNT(*) AS SUBMISSION_COUNT,
                       COUNT(DISTINCT MEMBER_ID) AS UNIQUE_MEMBERS,
                       COUNT(DISTINCT HCC_CODE) AS UNIQUE_HCCS
                FROM HEDIS_QUALITY_DB.CLAIMS_DATA.EDS_SUBMISSIONS
                GROUP BY STATUS ORDER BY SUBMISSION_COUNT DESC
            """)
            col_eds1, col_eds2 = st.columns([2, 3])
            with col_eds1:
                st.dataframe(df_eds, use_container_width=True, hide_index=True)
            with col_eds2:
                fig_eds = px.pie(df_eds, values="SUBMISSION_COUNT", names="STATUS",
                    color_discrete_map={"Accepted":"#10b981","Rejected":"#ef4444","Pending":"#f59e0b"},
                    hole=0.55)
                fig_eds.update_layout(height=300, margin=dict(l=10,r=10,t=10,b=10),
                    font=dict(family="DM Sans"), paper_bgcolor="rgba(0,0,0,0)")
                fig_eds.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_eds, use_container_width=True)
        except Exception as e:
            st.info(f"EDS summary not available: {str(e)}")

        # Chart review
        st.divider()
        st.markdown('<div class="section-header">📝 Chart Review / RADV Results</div>', unsafe_allow_html=True)
        try:
            df_review = run_query_df("""
                SELECT REVIEW_OUTCOME, REVIEW_COUNT, MEMBERS_REVIEWED,
                       AVG_ORIGINAL_RAF, AVG_RAF_IMPACT, TOTAL_REVENUE_IMPACT
                FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_CHART_REVIEW_SUMMARY
            """)
            st.dataframe(df_review, use_container_width=True, hide_index=True,
                column_config={
                    "AVG_ORIGINAL_RAF": st.column_config.NumberColumn("Avg RAF", format="%.3f"),
                    "AVG_RAF_IMPACT": st.column_config.NumberColumn("Avg Impact", format="%.3f"),
                    "TOTAL_REVENUE_IMPACT": st.column_config.NumberColumn("Revenue Impact", format="$%,.0f"),
                })
        except Exception as e:
            st.info(f"Chart review data not available: {str(e)}")

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 2: RECAPTURE WORKLIST
# ============================================================
with tab_recapture:
    st.markdown('<div class="section-header">🔄 Diagnosis Recapture Worklist</div>', unsafe_allow_html=True)
    st.caption("Conditions coded in 2023 but NOT yet documented in 2024 — revenue at risk until recaptured.")

    try:
        # KPIs
        recap_stats = run_query("""
            SELECT COUNT(*), COUNT(DISTINCT MEMBER_ID),
                   ROUND(SUM(REVENUE_AT_RISK), 0),
                   COUNT(CASE WHEN RECAPTURE_PRIORITY = 'CRITICAL' THEN 1 END)
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_RECAPTURE_OPPORTUNITIES
        """)
        rc1, rc2, rc3, rc4 = st.columns(4)
        with rc1:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value">{sni(recap_stats[0][0])}</div><div class="kpi-label">Total Recapture Opps</div></div>', unsafe_allow_html=True)
        with rc2:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value">{sni(recap_stats[0][1])}</div><div class="kpi-label">Members Affected</div></div>', unsafe_allow_html=True)
        with rc3:
            rev = f"${sni(recap_stats[0][2])}"
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-red">{rev}</div><div class="kpi-label">Revenue at Risk</div></div>', unsafe_allow_html=True)
        with rc4:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-red">{sni(recap_stats[0][3])}</div><div class="kpi-label">Critical Priority</div></div>', unsafe_allow_html=True)

        st.markdown("")

        # Filters
        fc1, fc2 = st.columns(2)
        with fc1:
            priority_filter = st.selectbox("Priority:", ["All","CRITICAL","HIGH","MEDIUM","LOW"], key="recap_pri")
        with fc2:
            category_filter = st.selectbox("HCC Category:", ["All","Cardiovascular","Endocrine","Pulmonary","Renal","Behavioral","Neurological","Neoplasm"], key="recap_cat")

        where_clauses = []
        if priority_filter != "All":
            where_clauses.append(f"RECAPTURE_PRIORITY = '{priority_filter}'")
        if category_filter != "All":
            where_clauses.append(f"HCC_CATEGORY = '{category_filter}'")
        where_sql = " AND " + " AND ".join(where_clauses) if where_clauses else ""

        df_recap = run_query_df(f"""
            SELECT MEMBER_ID, LAST_NAME, FIRST_NAME, AGE, PCP_GROUP,
                   HCC_CODE, HCC_DESCRIPTION, HCC_CATEGORY, RAF_WEIGHT,
                   REVENUE_AT_RISK, RECAPTURE_PRIORITY,
                   TARGET_SWEEP, DAYS_UNTIL_DEADLINE, SWEEP_URGENCY
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_RECAPTURE_OPPORTUNITIES
            WHERE 1=1 {where_sql}
            ORDER BY RAF_WEIGHT DESC
            LIMIT 50
        """)
        st.dataframe(df_recap, use_container_width=True, hide_index=True,
            column_config={
                "RAF_WEIGHT": st.column_config.NumberColumn("RAF Weight", format="%.3f"),
                "REVENUE_AT_RISK": st.column_config.NumberColumn("Revenue at Risk", format="$%,.0f"),
                "DAYS_UNTIL_DEADLINE": st.column_config.NumberColumn("Days to Deadline", format="%d"),
            })

        # Recapture by HCC
        st.markdown('<div class="section-header">📊 Recapture Opportunities by HCC</div>', unsafe_allow_html=True)
        df_by_hcc = run_query_df("""
            SELECT HCC_CODE, HCC_DESCRIPTION, COUNT(*) AS OPPORTUNITIES,
                   ROUND(SUM(REVENUE_AT_RISK), 0) AS TOTAL_REVENUE,
                   RAF_WEIGHT
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_RECAPTURE_OPPORTUNITIES
            GROUP BY HCC_CODE, HCC_DESCRIPTION, RAF_WEIGHT
            ORDER BY TOTAL_REVENUE DESC
            LIMIT 15
        """)
        fig_hcc = go.Figure()
        fig_hcc.add_trace(go.Bar(
            y=df_by_hcc["HCC_DESCRIPTION"], x=df_by_hcc["TOTAL_REVENUE"],
            orientation='h',
            marker=dict(color=df_by_hcc["RAF_WEIGHT"],
                        colorscale=[[0,"#c4b5fd"],[1,"#6d28d9"]]),
            text=df_by_hcc["TOTAL_REVENUE"].apply(lambda x: f"${x:,.0f}"),
            textposition="outside",
        ))
        fig_hcc.update_layout(height=500,
            margin=dict(l=10,r=80,t=10,b=10),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="DM Sans"),
            xaxis=dict(title="Revenue at Risk ($)", gridcolor="#f1f5f9"),
            yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig_hcc, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 3: SUSPECT CONDITIONS
# ============================================================
with tab_suspect:
    st.markdown('<div class="section-header">🔍 Suspect Condition Worklist</div>', unsafe_allow_html=True)
    st.caption("Members with pharmacy or lab evidence suggesting an HCC that hasn't been coded.")

    try:
        sus_stats = run_query("""
            SELECT COUNT(*), COUNT(DISTINCT MEMBER_ID),
                   ROUND(SUM(EST_REVENUE_IMPACT), 0),
                   COUNT(CASE WHEN PRIORITY = 'HIGH' THEN 1 END)
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_SUSPECT_WORKLIST
        """)
        sc1, sc2, sc3, sc4 = st.columns(4)
        with sc1:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value">{sni(sus_stats[0][0])}</div><div class="kpi-label">Suspect Conditions</div></div>', unsafe_allow_html=True)
        with sc2:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value">{sni(sus_stats[0][1])}</div><div class="kpi-label">Members Flagged</div></div>', unsafe_allow_html=True)
        with sc3:
            rev = f"${sni(sus_stats[0][2])}"
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-amber">{rev}</div><div class="kpi-label">Est. Revenue Opportunity</div></div>', unsafe_allow_html=True)
        with sc4:
            st.markdown(f'<div class="kpi-card"><div class="kpi-value-amber">{sni(sus_stats[0][3])}</div><div class="kpi-label">High Priority</div></div>', unsafe_allow_html=True)

        st.markdown("")

        # Filters
        sf1, sf2 = st.columns(2)
        with sf1:
            sus_priority = st.selectbox("Priority:", ["All","HIGH","MEDIUM"], key="sus_pri")
        with sf2:
            sus_source = st.selectbox("Evidence Source:", ["All","PHARMACY","LAB"], key="sus_src")

        swhere = []
        if sus_priority != "All":
            swhere.append(f"PRIORITY = '{sus_priority}'")
        if sus_source != "All":
            swhere.append(f"EVIDENCE_SOURCE = '{sus_source}'")
        swhere_sql = " AND " + " AND ".join(swhere) if swhere else ""

        df_sus = run_query_df(f"""
            SELECT MEMBER_ID, LAST_NAME, FIRST_NAME, AGE, PCP_GROUP,
                   SUSPECT_HCC, HCC_DESCRIPTION, EVIDENCE_SOURCE, EVIDENCE_DETAIL,
                   PRIORITY, CONFIDENCE_SCORE, EST_REVENUE_IMPACT,
                   EARLIEST_DOS, LATEST_DOS, TARGET_SWEEP, DAYS_UNTIL_DEADLINE, SWEEP_URGENCY
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_SUSPECT_WORKLIST
            WHERE 1=1 {swhere_sql}
            ORDER BY CONFIDENCE_SCORE DESC
            LIMIT 50
        """)
        st.dataframe(df_sus, use_container_width=True, hide_index=True,
            column_config={
                "CONFIDENCE_SCORE": st.column_config.ProgressColumn("Confidence", min_value=0, max_value=1, format="%.0f%%"),
                "EST_REVENUE_IMPACT": st.column_config.NumberColumn("Est Revenue", format="$%,.0f"),
                "DAYS_UNTIL_DEADLINE": st.column_config.NumberColumn("Days to Deadline", format="%d"),
            })

        # By HCC
        st.markdown('<div class="section-header">📊 Suspects by HCC Type</div>', unsafe_allow_html=True)
        df_sus_hcc = run_query_df("""
            SELECT SUSPECT_HCC, HCC_DESCRIPTION, COUNT(*) AS CNT,
                   ROUND(SUM(EST_REVENUE_IMPACT), 0) AS TOTAL_REV,
                   ROUND(AVG(CONFIDENCE_SCORE)*100, 0) AS AVG_CONFIDENCE
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_SUSPECT_WORKLIST
            GROUP BY SUSPECT_HCC, HCC_DESCRIPTION
            ORDER BY TOTAL_REV DESC
        """)
        fig_sus = px.bar(df_sus_hcc, x="HCC_DESCRIPTION", y="TOTAL_REV", color="AVG_CONFIDENCE",
                         color_continuous_scale=["#fde68a","#f59e0b","#d97706"],
                         text="CNT")
        fig_sus.update_layout(height=400, showlegend=False,
            margin=dict(l=10,r=10,t=10,b=10),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="DM Sans"),
            xaxis_title="", yaxis_title="Revenue Opportunity ($)",
            yaxis=dict(gridcolor="#f1f5f9"))
        fig_sus.update_traces(textposition="outside", texttemplate="%{text} members")
        st.plotly_chart(fig_sus, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 4: PROVIDER CODING SCORECARD
# ============================================================
with tab_providers:
    st.markdown('<div class="section-header">🏥 Provider Group Coding Scorecard</div>', unsafe_allow_html=True)
    st.caption("Compare coding completeness and revenue opportunity across provider groups.")

    try:
        df_prov = run_query_df("""
            SELECT * FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_PROVIDER_CODING_SCORECARD
            ORDER BY TOTAL_REVENUE_AT_RISK DESC
        """)
        st.dataframe(df_prov, use_container_width=True, hide_index=True,
            column_config={
                "PANEL_SIZE": st.column_config.NumberColumn("Panel", format="%d"),
                "AVG_RAF": st.column_config.NumberColumn("Avg RAF", format="%.3f"),
                "AVG_HCCS_PER_MEMBER": st.column_config.NumberColumn("Avg HCCs", format="%.1f"),
                "TOTAL_REVENUE_AT_RISK": st.column_config.NumberColumn("Recapture $", format="$%,.0f"),
                "TOTAL_SUSPECT_REVENUE": st.column_config.NumberColumn("Suspect $", format="$%,.0f"),
            })

        # Heatmap: Avg RAF + Recapture Opps
        st.markdown('<div class="section-header">📊 Provider RAF vs Revenue at Risk</div>', unsafe_allow_html=True)
        fig_prov = px.scatter(df_prov, x="AVG_RAF", y="TOTAL_REVENUE_AT_RISK",
                              size="PANEL_SIZE", color="AVG_HCCS_PER_MEMBER",
                              hover_name="PCP_GROUP",
                              color_continuous_scale=["#c4b5fd","#8b5cf6","#4c1d95"],
                              size_max=40)
        fig_prov.update_layout(height=450,
            margin=dict(l=10,r=10,t=10,b=10),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="DM Sans"),
            xaxis=dict(title="Average RAF Score", gridcolor="#f1f5f9"),
            yaxis=dict(title="Revenue at Risk ($)", gridcolor="#f1f5f9"))
        st.plotly_chart(fig_prov, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 5: REVENUE IMPACT SIMULATOR
# ============================================================
with tab_simulator:
    st.markdown('<div class="section-header">💰 Revenue Impact Simulator</div>', unsafe_allow_html=True)
    st.caption("Model the financial impact of recapturing dropped diagnoses and validating suspects.")

    try:
        # Revenue simulator table
        df_sim = run_query_df("SELECT * FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_REVENUE_SIMULATOR")
        st.dataframe(df_sim, use_container_width=True, hide_index=True,
            column_config={
                "TOTAL_REVENUE_AT_RISK": st.column_config.NumberColumn("Total at Risk", format="$%,.0f"),
                "IF_50_PCT_RECAPTURED": st.column_config.NumberColumn("50% Recapture", format="$%,.0f"),
                "IF_75_PCT_RECAPTURED": st.column_config.NumberColumn("75% Recapture", format="$%,.0f"),
                "IF_100_PCT_RECAPTURED": st.column_config.NumberColumn("100% Recapture", format="$%,.0f"),
            })

        st.divider()
        st.markdown("**🔮 Custom Scenario**")
        st.caption("Adjust recapture rate to see projected revenue recovery.")

        recapture_pct = st.slider("Recapture Rate:", 0, 100, 60, step=5, format="%d%%")

        total_at_risk = run_query("""
            SELECT ROUND(SUM(REVENUE_AT_RISK), 0)
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_RECAPTURE_OPPORTUNITIES
        """)[0][0] or 0

        suspect_rev = run_query("""
            SELECT ROUND(SUM(EST_REVENUE_IMPACT), 0)
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_SUSPECT_WORKLIST
        """)[0][0] or 0

        recovered = round(total_at_risk * recapture_pct / 100)
        suspect_captured = round(suspect_rev * recapture_pct / 100 * 0.6)  # 60% validation rate

        sr1, sr2, sr3, sr4 = st.columns(4)
        with sr1:
            st.metric("Recapture Revenue at Risk", f"${sni(total_at_risk)}")
        with sr2:
            st.metric("Projected Recovery", f"${sni(recovered)}", delta=f"+${sni(recovered)}")
        with sr3:
            st.metric("Suspect Validation Revenue", f"${sni(suspect_captured)}", delta=f"+${sni(suspect_captured)}")
        with sr4:
            total_gain = recovered + suspect_captured
            st.metric("Total Revenue Gain", f"${sni(total_gain)}", delta=f"+${sni(total_gain)}")

        # Gauge
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=total_gain,
            number={"prefix": "$", "valueformat": ",.0f"},
            title={"text": "Projected Revenue Recovery"},
            gauge={
                "axis": {"range": [0, total_at_risk + suspect_rev]},
                "bar": {"color": "#8b5cf6"},
                "steps": [
                    {"range": [0, total_at_risk], "color": "#fef2f2"},
                    {"range": [total_at_risk, total_at_risk + suspect_rev], "color": "#fffbeb"},
                ],
                "threshold": {
                    "line": {"color": "#10b981", "width": 4},
                    "thickness": 0.75,
                    "value": total_gain,
                },
            },
        ))
        fig_gauge.update_layout(height=300, margin=dict(l=30,r=30,t=60,b=10),
                               font=dict(family="DM Sans"), paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_gauge, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 6: HCC DISTRIBUTION
# ============================================================
with tab_hcc:
    st.markdown('<div class="section-header">🧬 HCC Distribution Analysis</div>', unsafe_allow_html=True)
    st.caption("Overview of coded, recapturable, and suspect HCCs across the member population.")

    try:
        df_hcc_dist = run_query_df("""
            SELECT HCC_CATEGORY, HCC_CODE, HCC_DESCRIPTION, RAF_WEIGHT,
                   MEMBERS_CODED_2024, MEMBERS_NEED_RECAPTURE, MEMBERS_SUSPECT
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_HCC_DISTRIBUTION
            ORDER BY RAF_WEIGHT DESC
        """)
        st.dataframe(df_hcc_dist, use_container_width=True, hide_index=True,
            column_config={
                "RAF_WEIGHT": st.column_config.NumberColumn("RAF Weight", format="%.3f"),
                "MEMBERS_CODED_2024": st.column_config.NumberColumn("Coded 2024", format="%d"),
                "MEMBERS_NEED_RECAPTURE": st.column_config.NumberColumn("Need Recapture", format="%d"),
                "MEMBERS_SUSPECT": st.column_config.NumberColumn("Suspect", format="%d"),
            })

        # Stacked bar by category
        st.markdown('<div class="section-header">📊 HCC Coding Status by Category</div>', unsafe_allow_html=True)
        df_cat = run_query_df("""
            SELECT HCC_CATEGORY,
                   SUM(MEMBERS_CODED_2024) AS CODED,
                   SUM(MEMBERS_NEED_RECAPTURE) AS RECAPTURE,
                   SUM(MEMBERS_SUSPECT) AS SUSPECT
            FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_HCC_DISTRIBUTION
            GROUP BY HCC_CATEGORY
            ORDER BY CODED DESC
        """)
        fig_cat = go.Figure()
        fig_cat.add_trace(go.Bar(name="Coded 2024", x=df_cat["HCC_CATEGORY"], y=df_cat["CODED"], marker_color="#8b5cf6"))
        fig_cat.add_trace(go.Bar(name="Need Recapture", x=df_cat["HCC_CATEGORY"], y=df_cat["RECAPTURE"], marker_color="#ef4444"))
        fig_cat.add_trace(go.Bar(name="Suspect", x=df_cat["HCC_CATEGORY"], y=df_cat["SUSPECT"], marker_color="#f59e0b"))
        fig_cat.update_layout(barmode="group", height=400,
            margin=dict(l=10,r=10,t=30,b=10),
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            font=dict(family="DM Sans"),
            yaxis=dict(title="Members", gridcolor="#f1f5f9"),
            legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig_cat, use_container_width=True)

    except Exception as e:
        st.error(f"Error: {str(e)}")


# ============================================================
# TAB 7: AI CHAT
# ============================================================
with tab_chat:
    st.markdown('<div class="section-header">💬 Ask About Risk Adjustment</div>', unsafe_allow_html=True)
    st.caption("Claude AI analyzes your HCC coding data, RAF scores, recapture opportunities, and suspect conditions.")

    st.markdown("**Quick questions:**")
    qcols = st.columns(3)
    questions = [
        ("📊", "What is our plan-level RAF and total revenue?"),
        ("🔄", "Which HCCs have the most recapture opportunities?"),
        ("🔍", "How many members have suspect conditions?"),
        ("🏥", "Which provider groups have the most revenue at risk?"),
        ("💰", "If we recapture 70% of dropped codes, how much revenue?"),
        ("🧬", "What are the highest-value HCCs we should focus on?"),
    ]
    def set_question(q):
        st.session_state["ra_pending"] = q
    for i, (icon, q) in enumerate(questions):
        with qcols[i % 3]:
            st.button(f"{icon} {q}", key=f"ra_q_{i}", use_container_width=True,
                      on_click=set_question, args=(q,))

    st.divider()

    if "ra_messages" not in st.session_state:
        st.session_state.ra_messages = []

    for msg in st.session_state.ra_messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])
            if "dataframe" in msg:
                st.dataframe(msg["dataframe"], use_container_width=True, hide_index=True)

    if "ra_pending" in st.session_state:
        prompt = st.session_state.pop("ra_pending")
    else:
        prompt = st.chat_input("Ask about HCC coding, RAF scores, recapture, suspects, revenue...")

    if prompt:
        st.session_state.ra_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Analyzing risk adjustment data..."):
                try:
                    # Gather context
                    plan_ctx = run_query("SELECT * FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_RAF_PLAN_SUMMARY")
                    p = plan_ctx[0]
                    # Null-safe helper
                    def safe_num(v, fmt=",.0f"):
                        try:
                            return f"{float(v):{fmt}}" if v is not None else "0"
                        except:
                            return str(v) if v is not None else "0"
                    plan_str = (
                        f"Plan: {safe_num(p[0])} members, avg RAF {safe_num(p[1],'.3f')}, est revenue ${safe_num(p[2])}. "
                        f"Total HCCs coded: {safe_num(p[3])}, avg HCCs/member: {safe_num(p[4],'.1f')}. "
                        f"Recapture opportunities: {safe_num(p[5])}, recapture revenue at risk: ${safe_num(p[6])}. "
                        f"Suspect conditions: {safe_num(p[7])}, suspect revenue opportunity: ${safe_num(p[8])}. "
                        f"Members with 0 HCCs: {safe_num(p[9])}, members with 3+ HCCs: {safe_num(p[10])}."
                    )

                    rev_ctx = run_query_df("SELECT * FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_REVENUE_SIMULATOR")
                    try:
                        rev_str = "Revenue scenarios by priority: " + "; ".join([
                            f"{r['RECAPTURE_PRIORITY']}: {r['OPPORTUNITIES']} opps, ${safe_num(r['TOTAL_REVENUE_AT_RISK'])} at risk"
                            for _, r in rev_ctx.iterrows()
                        ])
                    except:
                        rev_str = "Revenue simulator data not available."

                    hcc_ctx = run_query_df("""
                        SELECT HCC_CODE, HCC_DESCRIPTION, RAF_WEIGHT, MEMBERS_CODED_2024, MEMBERS_NEED_RECAPTURE, MEMBERS_SUSPECT
                        FROM HEDIS_QUALITY_DB.CLAIMS_DATA.V_HCC_DISTRIBUTION
                        ORDER BY RAF_WEIGHT DESC LIMIT 15
                    """)
                    hcc_str = "Top HCCs: " + "; ".join([
                        f"{r['HCC_CODE']} ({r['HCC_DESCRIPTION']}): RAF={r['RAF_WEIGHT']}, coded={r['MEMBERS_CODED_2024']}, recapture={r['MEMBERS_NEED_RECAPTURE']}, suspect={r['MEMBERS_SUSPECT']}"
                        for _, r in hcc_ctx.iterrows()
                    ])

                    fp = f"""You are a Medicare Advantage risk adjustment analytics expert.
{plan_str}
{rev_str}
{hcc_str}
CMS pays ~$11,826 per member per year at RAF 1.0. Each 0.1 RAF increase = ~$1,183/member/year.
CMS sweep deadlines: PY2025 Final-H1 due 2025-01-31, Final-H2 due 2025-03-31, Final-H3 due 2025-08-31. DOS window for PY2025 is CY2024 (2024-01-01 to 2024-12-31). Submissions must be in RAPS or EDS format before the sweep deadline to count for that payment year.
Question: {prompt}
Use specific numbers from the data. Be concise and actionable."""

                    safe_fp = fp.replace("'", "''")
                    resp = run_query(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet','{safe_fp}')")
                    answer = resp[0][0] if resp else "Could not generate response."
                    st.write(answer)
                    msg = {"role": "assistant", "content": answer}

                    # Auto SQL
                    kws = ["show","list","member","who","which","top","how many","count","find","provider","hcc","recapture","suspect"]
                    if any(k in prompt.lower() for k in kws):
                        try:
                            sq = f"""Write a Snowflake SELECT query for: "{prompt}"
Tables (all HEDIS_QUALITY_DB.CLAIMS_DATA):
- MEMBER_ENROLLMENT (MEMBER_ID, FIRST_NAME, LAST_NAME, AGE, GENDER, PCP_NAME, PCP_GROUP, DUAL_ELIGIBLE_FLAG, HCC_RISK_SCORE, DX_DIABETES, DX_HYPERTENSION, DX_CHF, DX_COPD, DX_CKD, DX_CAD, DX_DEPRESSION, DX_OBESITY)
- V_MEMBER_RAF_SCORE (MEMBER_ID, FIRST_NAME, LAST_NAME, AGE, GENDER, PCP_NPI, PCP_GROUP, HCC_COUNT, CALCULATED_RAF, ESTIMATED_ANNUAL_REVENUE, DEMOGRAPHIC_RAF, HCC_RAF_INCREMENT)
- V_RECAPTURE_OPPORTUNITIES (MEMBER_ID, LAST_NAME, FIRST_NAME, AGE, PCP_NPI, PCP_GROUP, HCC_CODE, HCC_DESCRIPTION, HCC_CATEGORY, RAF_WEIGHT, REVENUE_AT_RISK, RECAPTURE_PRIORITY, TARGET_SWEEP, DAYS_UNTIL_DEADLINE, SWEEP_URGENCY, PAYMENT_YEAR_IMPACTED)
- V_SUSPECT_WORKLIST (SUSPECT_ID, MEMBER_ID, LAST_NAME, FIRST_NAME, AGE, PCP_NPI, PCP_GROUP, SUSPECT_HCC, HCC_DESCRIPTION, EVIDENCE_SOURCE, EVIDENCE_DETAIL, PRIORITY, CONFIDENCE_SCORE, EST_REVENUE_IMPACT, EARLIEST_DOS, LATEST_DOS, TARGET_SWEEP, DAYS_UNTIL_DEADLINE, SWEEP_URGENCY, PAYMENT_YEAR_IMPACTED)
- V_PROVIDER_CODING_SCORECARD (PCP_GROUP, PANEL_SIZE, AVG_RAF, TOTAL_HCCS_CODED, AVG_HCCS_PER_MEMBER, TOTAL_RECAPTURE_OPPS, TOTAL_REVENUE_AT_RISK, TOTAL_SUSPECT_REVENUE)
- V_HCC_DISTRIBUTION (HCC_CATEGORY, HCC_CODE, HCC_DESCRIPTION, RAF_WEIGHT, MEMBERS_CODED_2024, MEMBERS_NEED_RECAPTURE, MEMBERS_SUSPECT)
- V_SWEEP_REVENUE_SUMMARY (PAYMENT_YEAR, SWEEP_NAME, SUBMISSION_DEADLINE, TOTAL_SUBMISSIONS, ACCEPTED, REJECTED, PENDING, TOTAL_ACCEPTED_REVENUE, REJECTED_REVENUE_LOST, PENDING_REVENUE)
- CMS_SWEEP_DATES (PAYMENT_YEAR, SWEEP_NUMBER, SWEEP_NAME, SUBMISSION_DEADLINE, DOS_START, DOS_END, DESCRIPTION)
- EDS_SUBMISSIONS (SUBMISSION_ID, MEMBER_ID, CLAIM_ID, CONTRACT_ID, ICD10_CODE, HCC_CODE, HCC_DESCRIPTION, RAF_WEIGHT, DOS_FROM, DOS_THRU, PROVIDER_NPI, SUBMISSION_TYPE, SUBMISSION_DATE, STATUS, REJECTION_REASON)
- RAPS_SUBMISSIONS (SUBMISSION_ID, MEMBER_ID, CONTRACT_ID, ICD10_CODE, HCC_CODE, HCC_DESCRIPTION, DOS_FROM, DOS_THRU, PROVIDER_NPI, TRANSACTION_TYPE, SUBMISSION_DATE, STATUS, ERROR_CODE)
- CHART_REVIEW_RADV (REVIEW_ID, MEMBER_ID, PROVIDER_NPI, PCP_GROUP, ORIGINAL_ICD10, ORIGINAL_HCC, HCC_DESCRIPTION, ORIGINAL_RAF_WEIGHT, REVIEW_OUTCOME, RAF_IMPACT, REVENUE_IMPACT, REVIEWER, REVIEW_TYPE, REVIEW_DATE)
Use fully qualified table names. LIMIT 25. Return ONLY SQL."""
                            safe_sq = sq.replace("'", "''")
                            sr2 = run_query(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet','{safe_sq}')")
                            sql = sr2[0][0].strip()
                            for fence in ["```sql","```SQL","```"]:
                                sql = sql.replace(fence, "")
                            sql = sql.strip()
                            if sql.upper().startswith("SELECT"):
                                df = run_query_df(sql)
                                if not df.empty:
                                    st.divider()
                                    st.caption("📋 Supporting Data")
                                    st.dataframe(df, use_container_width=True, hide_index=True)
                                    msg["dataframe"] = df
                        except:
                            pass

                    st.session_state.ra_messages.append(msg)
                except Exception as e:
                    st.error(f"Error: {str(e)}")


# ============================================================
# FOOTER
# ============================================================
st.markdown("""
<div class="footer">
    🎯 Risk Adjustment Intelligence · Powered by <a href="https://www.anthropic.com">Claude AI</a> on
    <a href="https://www.snowflake.com">Snowflake</a> ·
    Built with <a href="https://streamlit.io">Streamlit</a> ·
    Designed & Built by Sadaf Pasha
</div>
""", unsafe_allow_html=True)
