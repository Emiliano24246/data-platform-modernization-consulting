import boto3
import awswrangler as wr
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="NEO Risk Monitor — Analytics", page_icon="☄️", layout="wide")
st.title("☄️ NEO Risk Monitor — Suite Analítica Gold (NASA NeoWs)")
st.caption("Entorno aislado | Base de datos: asteroid_db_dev")

ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
S3_OUTPUT = f"s3://asteroid-data-lake-dev-{ACCOUNT_ID}/athena-results/"
DATABASE = "asteroid_db_dev"

@st.cache_data(ttl=60)
def cargar_datos():
    return wr.athena.read_sql_query(
        "SELECT * FROM asteroid_risk_metrics LIMIT 500",
        database=DATABASE,
        s3_output=S3_OUTPUT,
    )

with st.sidebar:
    st.header("⚙️ Controles NASA NEO")
    if st.button("🔄 Actualizar desde Athena", use_container_width=True):
        st.cache_data.clear()

df = cargar_datos()

if df.empty:
    st.warning("La tabla asteroid_risk_metrics aún no tiene registros.")
else:
    with st.sidebar:
        fechas_disp = sorted(df["close_approach_date"].astype(str).unique().tolist())
        sel_fechas = st.multiselect("Fecha de aproximación", fechas_disp, default=fechas_disp)
        solo_peligrosos = st.checkbox("Mostrar solo potencialmente peligrosos", value=False)

    df_f = df[df["close_approach_date"].astype(str).isin(sel_fechas)].copy()
    if solo_peligrosos and "is_potentially_hazardous" in df_f.columns:
        df_f = df_f[df_f["is_potentially_hazardous"] == True]

    # KPIs superiores
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Asteroides Analizados", len(df_f))
    if "estimated_diameter_avg_km" in df_f.columns and not df_f.empty:
        c2.metric("Diámetro Máximo", f"{df_f['estimated_diameter_avg_km'].max():.3f} km")
    if "relative_velocity_km_h" in df_f.columns and not df_f.empty:
        c3.metric("Velocidad Máxima", f"{df_f['relative_velocity_km_h'].max():,.0f} km/h")
    if "impact_energy_megatons" in df_f.columns and not df_f.empty:
        c4.metric("Energía Máx. Estimada", f"{df_f['impact_energy_megatons'].max():,.1f} Mt")

    # Fila 1: Dispersión física + Ranking de energía
    col1, col2 = st.columns(2)

    with col1:
        if {"estimated_diameter_avg_km", "estimated_mass_kg", "impact_energy_megatons"}.issubset(df_f.columns):
            fig_disp = px.scatter(
                df_f,
                x="estimated_diameter_avg_km",
                y="estimated_mass_kg",
                size="impact_energy_megatons",
                color="is_potentially_hazardous" if "is_potentially_hazardous" in df_f.columns else "name",
                hover_name="name" if "name" in df_f.columns else None,
                title="1. Diámetro vs. Masa (Tamaño = Energía en Megatones)",
            )
            st.plotly_chart(fig_disp, use_container_width=True)

    with col2:
        if {"name", "impact_energy_megatons"}.issubset(df_f.columns):
            top_energia = df_f.sort_values("impact_energy_megatons", ascending=True).tail(15)
            fig_en = px.bar(
                top_energia,
                x="impact_energy_megatons",
                y="name",
                orientation="h",
                color="impact_energy_megatons",
                color_continuous_scale="YlOrRd",
                title="2. Energía Cinética Potencial por Asteroide (Mt)",
            )
            st.plotly_chart(fig_en, use_container_width=True)

    # Fila 2: Velocidad vs Distancia + Clasificación de Riesgo
    col3, col4 = st.columns(2)

    with col3:
        if {"miss_distance_km", "relative_velocity_km_h"}.issubset(df_f.columns):
            fig_vel = px.scatter(
                df_f,
                x="miss_distance_km",
                y="relative_velocity_km_h",
                size="estimated_diameter_avg_km" if "estimated_diameter_avg_km" in df_f.columns else None,
                hover_name="name" if "name" in df_f.columns else None,
                color="relative_velocity_km_h",
                title="3. Distancia de Aproximación vs. Velocidad Relativa",
            )
            st.plotly_chart(fig_vel, use_container_width=True)

    with col4:
        if "is_potentially_hazardous" in df_f.columns:
            haz = df_f["is_potentially_hazardous"].map({True: "Peligroso", False: "No Peligroso"}).value_counts().reset_index()
            haz.columns = ["Clasificación", "Cantidad"]
            fig_haz = px.pie(
                haz,
                names="Clasificación",
                values="Cantidad",
                hole=0.45,
                color="Clasificación",
                color_discrete_map={"Peligroso": "#EF553B", "No Peligroso": "#00CC96"},
                title="4. Clasificación NASA de Peligrosidad",
            )
            st.plotly_chart(fig_haz, use_container_width=True)

    with st.expander("📋 Ver Tabla Completa de Métricas de Riesgo"):
        st.dataframe(df_f.sort_values("impact_energy_megatons", ascending=False), use_container_width=True)
