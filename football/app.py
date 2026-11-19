import boto3
import awswrangler as wr
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Football Analytics — KPIs", page_icon="⚽", layout="wide")
st.title("⚽ Football Analytics — Capa Gold")
st.caption("Entorno aislado | Base de datos: football_db_dev")

ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
S3_OUTPUT = f"s3://football-data-lake-dev-{ACCOUNT_ID}/athena-results/"
DATABASE = "football_db_dev"

@st.cache_data(ttl=60)
def cargar_datos():
    tablas = wr.catalog.tables(database=DATABASE)["Table"].tolist()
    tabla_gold = next((t for t in tablas if "silver" not in t), tablas[0])
    df = wr.athena.read_sql_query(
        f'SELECT * FROM "{tabla_gold}" LIMIT 200',
        database=DATABASE,
        s3_output=S3_OUTPUT,
    )
    return tabla_gold, df

if st.button("🔄 Actualizar datos desde Athena"):
    st.cache_data.clear()

tabla_activa, df = cargar_datos()
st.info(f"Consultando tabla aislada: `{DATABASE}.{tabla_activa}`")

if df.empty:
    st.warning("La tabla aún no contiene registros.")
else:
    c1, c2, c3 = st.columns(3)
    c1.metric("Equipos analizados", len(df))
    if "goals_for" in df.columns:
        c2.metric("Total Goles Registrados", int(df["goals_for"].sum()))
    if "avg_goals_per_match" in df.columns:
        c3.metric("Promedio Goles / Partido", f"{df['avg_goals_per_match'].mean():.2f}")

    if {"team_name", "goals_for", "goals_against"}.issubset(df.columns):
        top_goleadores = df.sort_values(
            by=["goals_for", "goal_difference"], ascending=False
        ).head(15)
        fig = px.bar(
            top_goleadores,
            x="team_name",
            y=["goals_for", "goals_against"],
            barmode="group",
            title="Top 15 Equipos — Goles a Favor vs. Goles en Contra",
            labels={"value": "Goles", "team_name": "Equipo", "variable": "Métrica"},
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Tabla de Indicadores Gold")
    st.dataframe(
        df.sort_values(by=["effectiveness_percentage", "goals_for"], ascending=False),
        use_container_width=True,
    )
