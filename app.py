import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION
# =============================================================================
st.set_page_config(
    page_title="Vietnam Tourism Analytics",
    page_icon="VN",
    layout="wide",
    initial_sidebar_state="expanded"
)

COLORS = {
    'primary': '#2E86AB',
    'secondary': '#A23B72', 
    'accent': '#F18F01',
    'success': '#28A745',
    'danger': '#DC3545',
    'dark': "#0C0707"
}

# Default plotly template
PLOTLY_TEMPLATE = 'plotly_white'

st.markdown("""
<style>
    .main {padding: 1rem 2rem;}
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #2E86AB;
    }
    [data-testid="stMetricLabel"] {
        color: #495057 !important;
    }
    [data-testid="stMetricValue"] {
        color: #1B4965 !important;
    }
    h1, h2, h3 {color: #1B4965 !important;}
</style>
""", unsafe_allow_html=True)

# =============================================================================
# DATA LOADING
# =============================================================================
@st.cache_data
def load_data():
    try:
        df = pd.read_csv('data/normalized/merged_tourism_data_extended.csv')
        weather = pd.read_csv('data/normalized/vietnam_weather_monthly_extended.csv')
        df['date'] = pd.to_datetime(df['date_parsed'])
        weather['date'] = pd.to_datetime(weather['date'])
        try:
            preds = pd.read_csv('data/predictions/traffic_predictions_extended.csv')
        except:
            preds = None
        return df, preds, weather
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None

df, predictions, weather = load_data()
if df is None:
    st.error("Cannot load data.")
    st.stop()

# =============================================================================
# SIDEBAR
# =============================================================================
with st.sidebar:
    st.title("Vietnam Tourism")
    st.caption("Traffic Analytics Platform")
    st.divider()
    page = st.radio("Navigation:", ["Overview", "Raw Data", "Weather Features", "Engineered Features", "Predictions", "Model Details"], label_visibility="collapsed")
    st.divider()
    st.markdown("**Quick Stats**")
    st.write(f"Period: 2011-2025")
    st.write(f"Destinations: {df['destination'].nunique():,}")
    st.write(f"Provinces: {df['province'].nunique()}")
    st.write(f"Records: {len(df):,}")

# =============================================================================
# PAGE: OVERVIEW
# =============================================================================
if page == "Overview":
    st.title("Vietnam Tourism Traffic Overview")
    st.caption("Comprehensive analysis of tourism search trends 2011-2025")
    
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("Destinations", f"{df['destination'].nunique():,}")
    with c2:
        st.metric("Provinces", df['province'].nunique())
    with c3:
        st.metric("Avg Traffic", f"{df[df['traffic'] > 0]['traffic'].mean():,.0f}")
    with c4:
        st.metric("Total Traffic", f"{df['traffic'].sum()/1e6:.1f}M")
    with c5:
        st.metric("Time Span", "15 years")
    
    st.divider()
    
    left, right = st.columns([3, 2])
    with left:
        st.subheader("Traffic Trend Over Time")
        trend = df.groupby('date')['traffic'].sum().reset_index()
        trend['rolling_6m'] = trend['traffic'].rolling(6, min_periods=1).mean()
        trend['rolling_12m'] = trend['traffic'].rolling(12, min_periods=1).mean()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=trend['date'], y=trend['traffic'], mode='lines', name='Monthly', line=dict(color=COLORS['primary'], width=1), opacity=0.5))
        fig.add_trace(go.Scatter(x=trend['date'], y=trend['rolling_6m'], mode='lines', name='6-Month MA', line=dict(color=COLORS['accent'], width=2)))
        fig.add_trace(go.Scatter(x=trend['date'], y=trend['rolling_12m'], mode='lines', name='12-Month MA', line=dict(color=COLORS['secondary'], width=2, dash='dash')))
        fig.update_layout(height=380, template=PLOTLY_TEMPLATE, hovermode='x unified', legend=dict(orientation='h', y=1.1), margin=dict(t=30, b=30))
        st.plotly_chart(fig, width='stretch')
    
    with right:
        st.subheader("Traffic Distribution")
        fig = go.Figure(go.Histogram(x=df[df['traffic'] > 0]['traffic'], nbinsx=50, marker_color=COLORS['primary']))
        fig.update_layout(height=380, template=PLOTLY_TEMPLATE, xaxis_title='Traffic', yaxis_title='Frequency', margin=dict(t=30, b=30))
        st.plotly_chart(fig, width='stretch')
    
    left, right = st.columns(2)
    with left:
        st.subheader("Top 15 Destinations")
        top15 = df.groupby('destination')['traffic'].sum().nlargest(15).reset_index().sort_values('traffic')
        fig = go.Figure(go.Bar(x=top15['traffic'], y=top15['destination'], orientation='h', marker=dict(color=top15['traffic'], colorscale='Blues')))
        fig.update_layout(height=450, template=PLOTLY_TEMPLATE, margin=dict(t=20, b=20, l=10))
        fig.update_xaxes(tickformat=',')
        st.plotly_chart(fig, width='stretch')
    
    with right:
        st.subheader("Top 15 Provinces")
        top_prov = df.groupby('province')['traffic'].sum().nlargest(15).reset_index().sort_values('traffic')
        fig = go.Figure(go.Bar(x=top_prov['traffic'], y=top_prov['province'], orientation='h', marker=dict(color=top_prov['traffic'], colorscale='Teal')))
        fig.update_layout(height=450, template=PLOTLY_TEMPLATE, margin=dict(t=20, b=20, l=10))
        fig.update_xaxes(tickformat=',')
        st.plotly_chart(fig, width='stretch')
    
    left, right = st.columns(2)
    with left:
        st.subheader("Seasonal Pattern")
        monthly = df.groupby('month')['traffic'].agg(['mean', 'std']).reset_index()
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        monthly['month_name'] = monthly['month'].apply(lambda x: months[int(x)-1] if pd.notna(x) else '?')
        fig = go.Figure(go.Bar(x=monthly['month_name'], y=monthly['mean'], marker_color=COLORS['primary'], error_y=dict(type='data', array=monthly['std'])))
        fig.update_layout(height=350, template=PLOTLY_TEMPLATE, xaxis_title='Month', yaxis_title='Avg Traffic', margin=dict(t=20, b=20))
        st.plotly_chart(fig, width='stretch')
    
    with right:
        st.subheader("Year-over-Year Traffic")
        yearly = df.groupby('year')['traffic'].sum().reset_index()
        fig = go.Figure(go.Bar(x=yearly['year'], y=yearly['traffic'], marker=dict(color=yearly['traffic'], colorscale='Viridis')))
        fig.update_layout(height=350, template=PLOTLY_TEMPLATE, xaxis_title='Year', yaxis_title='Total Traffic', margin=dict(t=20, b=20))
        fig.update_yaxes(tickformat=',')
        st.plotly_chart(fig, width='stretch')

# =============================================================================
# PAGE: RAW DATA
# =============================================================================
elif page == "Raw Data":
    st.title("Raw Data Exploration")
    st.caption("Explore original data sources before feature engineering")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Traffic", "Geography", "Economics", "Social Media"])
    
    with tab1:
        st.subheader("Google Trends Traffic Data")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Traffic by Quarter**")
            quarterly = df.groupby(['year', 'quarter'])['traffic'].sum().reset_index()
            quarterly['period'] = quarterly['year'].astype(str) + '-Q' + quarterly['quarter'].astype(str)
            fig = px.area(quarterly, x='period', y='traffic', color_discrete_sequence=[COLORS['primary']])
            fig.update_layout(height=300, template=PLOTLY_TEMPLATE, margin=dict(t=20, b=20))
            fig.update_yaxes(tickformat=',')
            st.plotly_chart(fig, width='stretch')
        with c2:
            st.markdown("**Traffic Distribution by Year**")
            fig = px.box(df[df['traffic'] > 0], x='year', y='traffic', color_discrete_sequence=[COLORS['secondary']])
            fig.update_layout(height=300, template=PLOTLY_TEMPLATE, margin=dict(t=20, b=20))
            st.plotly_chart(fig, width='stretch')
        
        st.markdown("**Traffic Heatmap: Year vs Month**")
        pivot = df.groupby(['year', 'month'])['traffic'].sum().reset_index().pivot(index='year', columns='month', values='traffic')
        fig = px.imshow(pivot, labels=dict(x="Month", y="Year", color="Traffic"), color_continuous_scale='YlOrRd', aspect='auto')
        fig.update_layout(height=400)
        st.plotly_chart(fig, width='stretch')
    
    with tab2:
        st.subheader("Geographic Data")
        c1, c2 = st.columns(2)
        with c1:
            if 'region' in df.columns:
                st.markdown("**Destinations by Region**")
                by_region = df.groupby('region').agg({'destination': 'nunique', 'traffic': 'sum'}).reset_index()
                fig = px.bar(by_region, x='region', y='destination', color='traffic', color_continuous_scale='Blues')
                fig.update_layout(height=350, template=PLOTLY_TEMPLATE, xaxis_tickangle=-45)
                st.plotly_chart(fig, width='stretch')
        with c2:
            if 'distance_to_hanoi_km' in df.columns:
                st.markdown("**Distance to Major Cities**")
                dist = df.groupby('province')[['distance_to_hanoi_km', 'distance_to_hcm_km']].first().dropna()
                fig = px.scatter(dist, x='distance_to_hanoi_km', y='distance_to_hcm_km', color_discrete_sequence=[COLORS['secondary']])
                fig.update_layout(height=350, template=PLOTLY_TEMPLATE, xaxis_title='Distance to Hanoi (km)', yaxis_title='Distance to HCM (km)')
                st.plotly_chart(fig, width='stretch')
        
        if 'latitude' in df.columns:
            st.markdown("**Geographic Distribution**")
            geo = df.groupby('province').agg({'latitude': 'first', 'longitude': 'first', 'traffic': 'sum', 'destination': 'nunique'}).reset_index().dropna()
            fig = px.scatter_mapbox(geo, lat='latitude', lon='longitude', size='traffic', color='destination', hover_name='province', zoom=5, color_continuous_scale='Viridis')
            fig.update_layout(mapbox_style='carto-positron', height=500, margin=dict(t=0, b=0, l=0, r=0))
            st.plotly_chart(fig, width='stretch')
    
    with tab3:
        st.subheader("Economic Data")
        c1, c2 = st.columns(2)
        with c1:
            if 'grdp' in df.columns:
                st.markdown("**GRDP by Province (Billion VND) - Latest Data**")
                # Convert Vietnamese number format first, then get max (latest) value per province
                def convert_vn_number(x):
                    if isinstance(x, str):
                        return float(x.replace('.', '').replace(',', '.'))
                    return float(x) if pd.notna(x) else 0
                df_temp = df[['province', 'grdp']].dropna()
                df_temp['grdp_numeric'] = df_temp['grdp'].apply(convert_vn_number)
                grdp_numeric = df_temp.groupby('province')['grdp_numeric'].max()
                grdp_sorted = grdp_numeric.sort_values(ascending=True).tail(20)
                fig = go.Figure(go.Bar(
                    y=grdp_sorted.index, 
                    x=grdp_sorted.values, 
                    orientation='h',
                    marker_color=COLORS['success']
                ))
                fig.update_layout(height=500, template=PLOTLY_TEMPLATE, xaxis_title='GRDP (Billion VND)', margin=dict(l=10))
                fig.update_xaxes(tickformat=',')
                st.plotly_chart(fig, width='stretch')
        with c2:
            if 'density' in df.columns:
                st.markdown("**Population Density Distribution**")
                density = df.groupby('province')['density'].first().dropna()
                fig = go.Figure(go.Histogram(x=density, nbinsx=20, marker_color=COLORS['accent']))
                fig.update_layout(height=500, template=PLOTLY_TEMPLATE, xaxis_title='Density (people/km2)', yaxis_title='Number of Provinces')
                st.plotly_chart(fig, width='stretch')
        
        if 'grdp' in df.columns:
            st.markdown("**GRDP vs Tourism Traffic**")
            # Convert GRDP to numeric and get max (latest) per province
            def convert_vn_number(x):
                if isinstance(x, str):
                    return float(x.replace('.', '').replace(',', '.'))
                return float(x) if pd.notna(x) else 0
            df_temp = df[['province', 'grdp', 'traffic', 'population_thousand']].dropna(subset=['grdp'])
            df_temp['grdp_numeric'] = df_temp['grdp'].apply(convert_vn_number)
            econ = df_temp.groupby('province').agg({
                'grdp_numeric': 'max',  # Latest/highest GRDP
                'traffic': 'sum',
                'population_thousand': 'first'
            }).reset_index().dropna()
            fig = px.scatter(econ, x='grdp_numeric', y='traffic', size='population_thousand', color_discrete_sequence=[COLORS['primary']])
            fig.update_layout(height=400, template=PLOTLY_TEMPLATE, xaxis_title='GRDP (Billion VND)')
            st.plotly_chart(fig, width='stretch')
    
    with tab4:
        st.subheader("YouTube Data")
        if 'youtube_views' in df.columns:
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric("Total Views", f"{df['youtube_views'].sum()/1e6:.1f}M")
            with c2:
                st.metric("Total Likes", f"{df['youtube_likes'].sum()/1e3:.1f}K" if 'youtube_likes' in df.columns else "N/A")
            with c3:
                st.metric("Total Comments", f"{df['youtube_comments'].sum()/1e3:.1f}K" if 'youtube_comments' in df.columns else "N/A")
            
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**YouTube Views by Province**")
                yt = df.groupby('province')['youtube_views'].sum().nlargest(15).reset_index()
                fig = go.Figure(go.Bar(x=yt['province'], y=yt['youtube_views'], marker_color=COLORS['danger']))
                fig.update_layout(height=350, template=PLOTLY_TEMPLATE, xaxis_tickangle=-45)
                st.plotly_chart(fig, width='stretch')
            with c2:
                st.markdown("**YouTube Views vs Traffic**")
                yt_traffic = df.groupby('destination').agg({'youtube_views': 'first', 'traffic': 'sum'}).dropna()
                fig = px.scatter(yt_traffic, x='youtube_views', y='traffic', opacity=0.5, color_discrete_sequence=[COLORS['secondary']])
                fig.update_layout(height=350, template=PLOTLY_TEMPLATE)
                st.plotly_chart(fig, width='stretch')

# =============================================================================
# PAGE: WEATHER FEATURES
# =============================================================================
elif page == "Weather Features":
    st.title("Weather Extended Features")
    st.caption("Aggregated weather variables from daily data to monthly features")
    
    st.markdown("""
    ### Weather Variables Description
    | Variable | Description |
    |----------|-------------|
    | temp_min | Lowest temperature in month |
    | temp_max | Highest temperature in month |
    | temp_amplitude | Temperature range (max - min) |
    | temp_std | Temperature variability |
    | rainfall_total | Monthly total rainfall |
    | rainfall_max_daily | Maximum daily rainfall |
    | rainfall_days | Number of rainy days |
    """)
    
    st.divider()
    
    # Weather overview
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Avg Temperature", f"{df['temp_mean'].mean():.1f}C")
    with c2:
        st.metric("Avg Rainfall", f"{df['rainfall_total'].mean():.0f}mm")
    with c3:
        st.metric("Avg Amplitude", f"{df['temp_amplitude'].mean():.1f}C")
    with c4:
        st.metric("Avg Rainy Days", f"{df['rainfall_days'].mean():.0f}")
    
    st.divider()
    
    # Temperature analysis
    st.subheader("Temperature Analysis")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Temperature Distribution by Province**")
        temp_by_prov = weather.groupby('province')['temp_mean'].mean().sort_values()
        fig = go.Figure(go.Bar(x=temp_by_prov.values, y=temp_by_prov.index, orientation='h', marker=dict(color=temp_by_prov.values, colorscale='RdYlBu_r')))
        fig.update_layout(height=500, template=PLOTLY_TEMPLATE, xaxis_title='Avg Temperature (C)')
        st.plotly_chart(fig, width='stretch')
    
    with c2:
        st.markdown("**Temperature Range (Amplitude) by Province**")
        amp_by_prov = weather.groupby('province')['temp_amplitude'].mean().sort_values()
        fig = go.Figure(go.Bar(x=amp_by_prov.values, y=amp_by_prov.index, orientation='h', marker=dict(color=amp_by_prov.values, colorscale='Oranges')))
        fig.update_layout(height=500, template=PLOTLY_TEMPLATE, xaxis_title='Avg Amplitude (C)')
        st.plotly_chart(fig, width='stretch')
    
    # Seasonal temperature pattern
    st.subheader("Seasonal Weather Patterns")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Monthly Temperature Pattern**")
        temp_monthly = weather.groupby('month')[['temp_min', 'temp_mean', 'temp_max']].mean().reset_index()
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        temp_monthly['month_name'] = temp_monthly['month'].apply(lambda x: months[int(x)-1])
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=temp_monthly['month_name'], y=temp_monthly['temp_max'], name='Max', line=dict(color=COLORS['danger'])))
        fig.add_trace(go.Scatter(x=temp_monthly['month_name'], y=temp_monthly['temp_mean'], name='Mean', line=dict(color=COLORS['accent'])))
        fig.add_trace(go.Scatter(x=temp_monthly['month_name'], y=temp_monthly['temp_min'], name='Min', line=dict(color=COLORS['primary'])))
        fig.update_layout(height=350, template=PLOTLY_TEMPLATE, yaxis_title='Temperature (C)', hovermode='x unified')
        st.plotly_chart(fig, width='stretch')
    
    with c2:
        st.markdown("**Monthly Rainfall Pattern**")
        rain_monthly = weather.groupby('month')[['rainfall_total', 'rainfall_days']].mean().reset_index()
        rain_monthly['month_name'] = rain_monthly['month'].apply(lambda x: months[int(x)-1])
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=rain_monthly['month_name'], y=rain_monthly['rainfall_total'], name='Rainfall (mm)', marker_color=COLORS['primary']), secondary_y=False)
        fig.add_trace(go.Scatter(x=rain_monthly['month_name'], y=rain_monthly['rainfall_days'], name='Rainy Days', line=dict(color=COLORS['accent'], width=3)), secondary_y=True)
        fig.update_layout(height=350, template=PLOTLY_TEMPLATE, hovermode='x unified')
        fig.update_yaxes(title_text="Rainfall (mm)", secondary_y=False)
        fig.update_yaxes(title_text="Rainy Days", secondary_y=True)
        st.plotly_chart(fig, width='stretch')
    
    # Weather vs Traffic
    st.subheader("Weather Impact on Traffic")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Temperature vs Traffic**")
        temp_bins = pd.cut(df['temp_mean'].dropna(), bins=10)
        temp_traffic = df.groupby(temp_bins, observed=True)['traffic'].mean().reset_index()
        temp_traffic['temp_mean'] = temp_traffic['temp_mean'].astype(str)
        fig = go.Figure(go.Bar(x=temp_traffic['temp_mean'], y=temp_traffic['traffic'], marker_color=COLORS['accent']))
        fig.update_layout(height=300, template=PLOTLY_TEMPLATE, xaxis_tickangle=-45, yaxis_title='Avg Traffic')
        st.plotly_chart(fig, width='stretch')
    
    with c2:
        st.markdown("**Amplitude vs Traffic**")
        amp_bins = pd.cut(df['temp_amplitude'].dropna(), bins=10)
        amp_traffic = df.groupby(amp_bins, observed=True)['traffic'].mean().reset_index()
        amp_traffic['temp_amplitude'] = amp_traffic['temp_amplitude'].astype(str)
        fig = go.Figure(go.Bar(x=amp_traffic['temp_amplitude'], y=amp_traffic['traffic'], marker_color=COLORS['danger']))
        fig.update_layout(height=300, template=PLOTLY_TEMPLATE, xaxis_tickangle=-45, yaxis_title='Avg Traffic')
        st.plotly_chart(fig, width='stretch')
    
    with c3:
        st.markdown("**Rainfall vs Traffic**")
        rain_bins = pd.cut(df['rainfall_total'].dropna(), bins=10)
        rain_traffic = df.groupby(rain_bins, observed=True)['traffic'].mean().reset_index()
        rain_traffic['rainfall_total'] = rain_traffic['rainfall_total'].astype(str)
        fig = go.Figure(go.Bar(x=rain_traffic['rainfall_total'], y=rain_traffic['traffic'], marker_color=COLORS['primary']))
        fig.update_layout(height=300, template=PLOTLY_TEMPLATE, xaxis_tickangle=-45, yaxis_title='Avg Traffic')
        st.plotly_chart(fig, width='stretch')
    
    # Correlation heatmap
    st.subheader("Weather Correlation Matrix")
    weather_cols = ['traffic', 'temp_mean', 'temp_min', 'temp_max', 'temp_amplitude', 'temp_std', 'rainfall_total', 'rainfall_days']
    weather_cols = [c for c in weather_cols if c in df.columns]
    corr = df[weather_cols].dropna().corr()
    fig = px.imshow(corr, text_auto='.2f', color_continuous_scale='RdBu_r', aspect='auto')
    fig.update_layout(height=450)
    st.plotly_chart(fig, width='stretch')

# =============================================================================
# PAGE: ENGINEERED FEATURES
# =============================================================================
elif page == "Engineered Features":
    st.title("Engineered Features")
    st.caption("Features created for the prediction model")
    
    st.markdown("""
    ### Feature Categories
    | Category | Features | Description |
    |----------|----------|-------------|
    | Lag Features | traffic_lag_1m, 2m, 3m, 6m, 12m | Historical traffic values |
    | Rolling Features | rolling_mean_3m, 6m, 12m | Moving averages |
    | Time Features | month, quarter, year | Temporal indicators |
    | Destination Stats | dest_mean, dest_max, dest_coverage | Destination-level statistics |
    | Seasonality | seasonal_amplitude, Peak_Months | Seasonal patterns |
    """)
    
    st.divider()
    
    # Destination statistics
    st.subheader("Destination-Level Statistics")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Mean Traffic Distribution**")
        if 'dest_mean_traffic' in df.columns:
            mean_traffic = df.groupby('destination')['dest_mean_traffic'].first().dropna()
            fig = go.Figure(go.Histogram(x=mean_traffic, nbinsx=50, marker_color=COLORS['primary']))
            fig.update_layout(height=300, template=PLOTLY_TEMPLATE, xaxis_title='Mean Traffic', yaxis_title='Count')
            st.plotly_chart(fig, width='stretch')
    
    with c2:
        st.markdown("**Coverage Distribution**")
        if 'dest_coverage_pct' in df.columns:
            coverage = df.groupby('destination')['dest_coverage_pct'].first().dropna()
            fig = go.Figure(go.Histogram(x=coverage, nbinsx=50, marker_color=COLORS['secondary']))
            fig.update_layout(height=300, template=PLOTLY_TEMPLATE, xaxis_title='Coverage %', yaxis_title='Count')
            st.plotly_chart(fig, width='stretch')
    
    # Mean vs Max scatter
    st.markdown("**Destination Mean vs Max Traffic**")
    if 'dest_mean_traffic' in df.columns and 'dest_max_traffic' in df.columns:
        dest_stats = df.groupby('destination')[['dest_mean_traffic', 'dest_max_traffic', 'dest_std_traffic']].first().dropna()
        fig = px.scatter(dest_stats, x='dest_mean_traffic', y='dest_max_traffic', size='dest_std_traffic', opacity=0.6, color_discrete_sequence=[COLORS['accent']])
        fig.add_trace(go.Scatter(x=[0, dest_stats['dest_mean_traffic'].max()], y=[0, dest_stats['dest_mean_traffic'].max()], mode='lines', name='y=x', line=dict(dash='dash', color='gray')))
        fig.update_layout(height=400, template=PLOTLY_TEMPLATE, xaxis_title='Mean Traffic', yaxis_title='Max Traffic')
        st.plotly_chart(fig, width='stretch')
    
    st.divider()
    
    # Seasonality features
    st.subheader("Seasonality Features")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Seasonal Amplitude Distribution**")
        if 'seasonal_amplitude' in df.columns:
            amp = df.groupby('destination')['seasonal_amplitude'].first().dropna()
            fig = go.Figure(go.Histogram(x=amp, nbinsx=40, marker_color=COLORS['accent']))
            fig.update_layout(height=300, template=PLOTLY_TEMPLATE, xaxis_title='Seasonal Amplitude', yaxis_title='Count')
            st.plotly_chart(fig, width='stretch')
    
    with c2:
        st.markdown("**Strong Seasonality Distribution**")
        if 'has_strong_seasonality' in df.columns:
            seasonality = df.groupby('destination')['has_strong_seasonality'].first().value_counts()
            fig = go.Figure(go.Pie(labels=['No Strong Seasonality', 'Strong Seasonality'], values=[seasonality.get(False, 0), seasonality.get(True, 0)], marker_colors=[COLORS['primary'], COLORS['accent']]))
            fig.update_layout(height=300)
            st.plotly_chart(fig, width='stretch')
    
    # Peak months analysis
    st.markdown("**Peak Months Distribution**")
    if 'Primary_Peak_Month' in df.columns:
        peak_dist = df.groupby('destination')['Primary_Peak_Month'].first().dropna().value_counts().sort_index()
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        peak_dist.index = peak_dist.index.map(lambda x: months[int(x)-1] if pd.notna(x) and x <= 12 else '?')
        fig = go.Figure(go.Bar(x=peak_dist.index, y=peak_dist.values, marker_color=COLORS['danger']))
        fig.update_layout(height=300, template=PLOTLY_TEMPLATE, xaxis_title='Peak Month', yaxis_title='Number of Destinations')
        st.plotly_chart(fig, width='stretch')
    
    st.divider()
    
    # Regional features
    st.subheader("Regional Features")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Traffic by Region**")
        if 'region' in df.columns:
            by_region = df.groupby('region')['traffic'].sum().sort_values()
            fig = go.Figure(go.Bar(x=by_region.values, y=by_region.index, orientation='h', marker_color=COLORS['primary']))
            fig.update_layout(height=350, template=PLOTLY_TEMPLATE, xaxis_title='Total Traffic')
            st.plotly_chart(fig, width='stretch')
    
    with c2:
        st.markdown("**Distance vs Traffic**")
        if 'distance_to_hanoi_km' in df.columns:
            dist_data = df.groupby('province').agg({'distance_to_hanoi_km': 'first', 'traffic': 'sum'}).dropna()
            fig = px.scatter(dist_data, x='distance_to_hanoi_km', y='traffic', color_discrete_sequence=[COLORS['secondary']], opacity=0.7)
            fig.update_layout(height=350, template=PLOTLY_TEMPLATE, xaxis_title='Distance to Hanoi (km)', yaxis_title='Total Traffic')
            st.plotly_chart(fig, width='stretch')

# =============================================================================
# PAGE: PREDICTIONS
# =============================================================================
elif page == "Predictions":
    st.title("Traffic Predictions")
    st.caption("Model predictions for tourism destinations")
    
    if predictions is None:
        st.info("Generating predictions from historical averages...")
        latest = df['date'].max()
        latest_data = df[df['date'] == latest].copy()
        avg_by_dest = df.groupby('destination')['traffic'].mean().reset_index()
        avg_by_dest.columns = ['destination', 'predicted_traffic']
        predictions = latest_data[['destination', 'province', 'traffic']].merge(avg_by_dest, on='destination')
        predictions['actual_traffic'] = predictions['traffic']
    
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total Destinations", len(predictions))
    with c2:
        st.metric("Max Predicted", f"{predictions['predicted_traffic'].max():,.0f}")
    with c3:
        st.metric("Avg Predicted", f"{predictions['predicted_traffic'].mean():,.0f}")
    
    st.divider()
    
    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader("Top 20 Predicted Destinations")
        top20 = predictions.nlargest(20, 'predicted_traffic').sort_values('predicted_traffic')
        fig = go.Figure(go.Bar(x=top20['predicted_traffic'], y=top20['destination'], orientation='h', marker=dict(color=top20['predicted_traffic'], colorscale='Viridis', showscale=True)))
        fig.update_layout(height=600, template=PLOTLY_TEMPLATE)
        st.plotly_chart(fig, width='stretch')
    
    with c2:
        st.subheader("Predictions by Province")
        by_prov = predictions.groupby('province')['predicted_traffic'].sum().nlargest(10)
        fig = go.Figure(go.Pie(labels=by_prov.index, values=by_prov.values, hole=0.4))
        fig.update_layout(height=400)
        st.plotly_chart(fig, width='stretch')
        
        st.markdown("**Top Provinces**")
        for prov, val in by_prov.items():
            st.write(f"- {prov}: {val:,.0f}")
    
    st.divider()
    st.subheader("Full Predictions Table")
    st.dataframe(predictions.sort_values('predicted_traffic', ascending=False)[['destination', 'province', 'predicted_traffic']], width='stretch', hide_index=True, height=400)

# =============================================================================
# PAGE: MODEL DETAILS
# =============================================================================
elif page == "Model Details":
    st.title("Model Technical Details")
    st.caption("LightGBM model performance and feature importance")
    
    tab1, tab2, tab3 = st.tabs(["Performance", "Feature Importance", "Data Quality"])
    
    with tab1:
        st.subheader("Model Performance")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Performance Metrics**")
            perf = pd.DataFrame({'Metric': ['R2 Score', 'RMSE', 'MAE', 'Trees'], 'Value': ['0.9900', '3.97', '0.87', '139']})
            st.dataframe(perf, width='stretch', hide_index=True)
        with c2:
            st.markdown("**Training Data**")
            train = pd.DataFrame({'Info': ['Total Records', 'Training', 'Test', 'Period'], 'Value': ['278,436', '260,490', '6,330', '2011-2025']})
            st.dataframe(train, width='stretch', hide_index=True)
        
        st.markdown("**Model Configuration**")
        config = pd.DataFrame({
            'Parameter': ['Algorithm', 'Learning Rate', 'Num Leaves', 'Early Stopping'],
            'Value': ['LightGBM (Gradient Boosting)', '0.05', '63', '50 rounds']
        })
        st.dataframe(config, width='stretch', hide_index=True)
    
    with tab2:
        st.subheader("Feature Importance")
        features = pd.DataFrame({
            'Feature': ['traffic_yoy_change', 'traffic_lag_12m', 'traffic_lag_1m', 'traffic_rolling_mean_3m', 'year', 'dest_mean_traffic', 'temp_amplitude', 'temp_max', 'rainfall_total', 'grdp'],
            'Importance': [1709, 1484, 1298, 319, 286, 270, 152, 139, 119, 118],
            'Category': ['Lag', 'Lag', 'Lag', 'Rolling', 'Time', 'Destination', 'Weather', 'Weather', 'Weather', 'Economic']
        })
        
        fig = go.Figure(go.Bar(x=features['Importance'], y=features['Feature'], orientation='h', marker=dict(color=features['Importance'], colorscale='Viridis')))
        fig.update_layout(height=400, template=PLOTLY_TEMPLATE, xaxis_title='Importance Score')
        st.plotly_chart(fig, width='stretch')
        
        st.markdown("**Feature Categories**")
        by_cat = features.groupby('Category')['Importance'].sum().sort_values(ascending=False)
        fig = go.Figure(go.Pie(labels=by_cat.index, values=by_cat.values, hole=0.4))
        fig.update_layout(height=350)
        st.plotly_chart(fig, width='stretch')
    
    with tab3:
        st.subheader("Data Quality")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Weather", f"{df['temp_mean'].notna().mean()*100:.1f}%")
        with c2:
            st.metric("Region", f"{df['region'].notna().mean()*100:.1f}%" if 'region' in df.columns else "N/A")
        with c3:
            st.metric("YouTube", f"{df['youtube_views'].notna().mean()*100:.1f}%" if 'youtube_views' in df.columns else "N/A")
        with c4:
            st.metric("GRDP", f"{df['grdp'].notna().mean()*100:.1f}%" if 'grdp' in df.columns else "N/A")
        
        st.divider()
        st.markdown("**Missing Data Analysis**")
        cols = ['traffic', 'temp_mean', 'rainfall_total', 'youtube_views', 'grdp', 'population_thousand', 'region']
        cols = [c for c in cols if c in df.columns]
        missing = df[cols].isna().mean() * 100
        fig = go.Figure(go.Bar(x=missing.index, y=missing.values, marker_color=COLORS['danger']))
        fig.update_layout(height=300, template=PLOTLY_TEMPLATE, yaxis_title='Missing %')
        st.plotly_chart(fig, width='stretch')

# =============================================================================
# FOOTER
# =============================================================================
st.divider()
st.caption("Vietnam Tourism Analytics | Data: 2011-2025 | Model: LightGBM | v2.0")
