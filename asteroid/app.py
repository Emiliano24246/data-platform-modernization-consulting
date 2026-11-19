import boto3
import awswrangler as wr
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="NEO Risk Monitor — Analytics", page_icon="☄️", layout="wide")
st.title("☄️ NEO Risk Monitor — Capa Gold (NASA NeoWs)")
st.caption("Entorno aislado | Base de datos: asteroid_db_dev")

ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
S3_OUTPUT = f"s3://asteroid-data-lake-dev-{ACCOUNT_ID}/athena-results/"
DATABASE = "asteroid_db_dev"

@st.cache_data(ttl=60)
def cargar_datos():
    return wr.athena.read_sql_query(
        "SELECT * FROM asteroid_risk_metrics LIMIT 200",
        database=DATABASE,
        s3_output=S3_OUTPUT,
    )

if st.button("🔄 Actualizar datos desde Athena"):
    st.cache_data.clear()

df = cargar_datos()

if df.empty:
    st.warning("La tabla asteroid_risk_metrics aún no tiene registros.")
else:
    cols_num = df.select_dtypes(include="number").columns.tolist()
    c1, c2 = st.columns(2)
    c1.metric("Registros en Gold", len(df))
    c2.metric("Columnas analíticas", len(df.columns))

    if len(cols_num) >= 2:
        fig = px.scatter(
            df,
            x=cols_num[0],
            y=cols_num[1],
            color=df.columns[0],
            title=f"Relación {cols_num[0]} vs {cols_num[1]}",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Métricas de Riesgo de Asteroides")
    st.dataframe(df, use_container_width=True)
