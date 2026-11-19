import boto3
import awswrangler as wr
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Football Analytics — KPIs", page_icon="⚽", layout="wide")
st.title("⚽ Football Analytics — Suite Analítica Gold")
st.caption("Entorno aislado | Base de datos: football_db_dev")

ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
S3_OUTPUT = f"s3://football-data-lake-dev-{ACCOUNT_ID}/athena-results/"
DATABASE = "football_db_dev"

@st.cache_data(ttl=60)
def cargar_datos():
    tablas = wr.catalog.tables(database=DATABASE)["Table"].tolist()
    tabla_gold = next((t for t in tablas if "silver" not in t), tablas[0])
    df = wr.athena.read_sql_query(
        f'SELECT * FROM "{tabla_gold}" LIMIT 500',
        database=DATABASE,
        s3_output=S3_OUTPUT,
    )
    return tabla_gold, df

with st.sidebar:
    st.header("⚙️ Controles Football")
    if st.button("🔄 Actualizar desde Athena", use_container_width=True):
        st.cache_data.clear()

tabla_activa, df = cargar_datos()

if df.empty:
    st.warning("La tabla aún no contiene registros.")
else:
    with st.sidebar:
        min_goles = st.slider("Mínimo de goles a favor", 0, int(df["goals_for"].max()), 0) if "goals_for" in df.columns else 0
        busqueda = st.text_input("Buscar equipo", "")

    df_f = df.copy()
    if "goals_for" in df_f.columns:
        df_f = df_f[df_f["goals_for"] >= min_goles]
    if busqueda and "team_name" in df_f.columns:
        df_f = df_f[df_f["team_name"].str.contains(busqueda, case=False, na=False)]

    # KPIs superiores
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Equipos Analizados", len(df_f))
    if "goals_for" in df_f.columns:
        c2.metric("Goles Totales", int(df_f["goals_for"].sum()))
    if "effectiveness_percentage" in df_f.columns and not df_f.empty:
        c3.metric("Efectividad Promedio", f"{df_f['effectiveness_percentage'].mean():.1f}%")
    if "avg_goals_per_match" in df_f.columns and not df_f.empty:
        c4.metric("Goles / Partido Promedio", f"{df_f['avg_goals_per_match'].mean():.2f}")

    # Fila 1: Goles a favor vs en contra + Diferencia de goles
    col1, col2 = st.columns(2)

    with col1:
        if {"team_name", "goals_for", "goals_against"}.issubset(df_f.columns):
            top_gf = df_f.sort_values(by=["goals_for", "goal_difference"], ascending=False).head(15)
            fig_goles = px.bar(
                top_gf,
                x="team_name",
                y=["goals_for", "goals_against"],
                barmode="group",
                title="1. Top 15 Equipos — Goles a Favor vs. En Contra",
                labels={"value": "Goles", "team_name": "Equipo", "variable": "Métrica"},
            )
            st.plotly_chart(fig_goles, use_container_width=True)

    with col2:
        if {"team_name", "goal_difference"}.issubset(df_f.columns):
            top_gd = df_f.sort_values(by="goal_difference", ascending=False).head(15)
            fig_gd = px.bar(
                top_gd,
                x="goal_difference",
                y="team_name",
                orientation="h",
                color="goal_difference",
                color_continuous_scale="RdYlGn",
                title="2. Top 15 por Diferencia de Goles",
            )
            st.plotly_chart(fig_gd, use_container_width=True)

    # Fila 2: Efectividad vs Goles + Balance Victorias/Empates/Derrotas
    col3, col4 = st.columns(2)

    with col3:
        if {"goals_for", "effectiveness_percentage", "team_name"}.issubset(df_f.columns):
            fig_eff = px.scatter(
                df_f,
                x="goals_for",
                y="effectiveness_percentage",
                size="matches_played" if "matches_played" in df_f.columns else None,
                color="goal_difference" if "goal_difference" in df_f.columns else None,
                hover_name="team_name",
                title="3. Capacidad Goleadora vs. % de Efectividad",
            )
            st.plotly_chart(fig_eff, use_container_width=True)

    with col4:
        if {"wins", "draws", "losses"}.issubset(df_f.columns):
            resumen = pd.DataFrame({
                "Resultado": ["Victorias", "Empates", "Derrotas"],
                "Total": [df_f["wins"].sum(), df_f["draws"].sum(), df_f["losses"].sum()],
            })
            fig_res = px.pie(
                resumen,
                names="Resultado",
                values="Total",
                hole=0.45,
                color="Resultado",
                color_discrete_map={"Victorias": "#00CC96", "Empates": "#FFA15A", "Derrotas": "#EF553B"},
                title="4. Distribución Global de Resultados",
            )
            st.plotly_chart(fig_res, use_container_width=True)

    with st.expander(f"📋 Ver Tabla Completa ({DATABASE}.{tabla_activa})"):
        st.dataframe(
            df_f.sort_values(by=["effectiveness_percentage", "goals_for"], ascending=False),
            use_container_width=True,
        )
