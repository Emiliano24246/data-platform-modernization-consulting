import boto3
import awswrangler as wr
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Binance BTCUSDT — Analytics", page_icon="📈", layout="wide")
st.title("📈 Binance BTCUSDT — Capa Gold (MarketMatrix)")
st.caption("Entorno aislado | Base de datos: marketmatrix_db_dev")

ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
S3_OUTPUT = f"s3://binance-data-lake-dev-{ACCOUNT_ID}/athena-results/"
DATABASE = "marketmatrix_db_dev"

@st.cache_data(ttl=60)
def cargar_datos():
    df = wr.athena.read_sql_query(
        "SELECT * FROM binance_gold_signals ORDER BY date ASC LIMIT 200",
        database=DATABASE,
        s3_output=S3_OUTPUT,
    )
    if "date" in df.columns:
        df = df.sort_values("date").reset_index(drop=True)
    return df

if st.button("🔄 Actualizar datos desde Athena"):
    st.cache_data.clear()

df = cargar_datos()

if df.empty:
    st.warning("La tabla binance_gold_signals aún no tiene registros.")
else:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Registros en Gold", len(df))
    if "close" in df.columns:
        c2.metric("Último Cierre (USDT)", f"${df['close'].iloc[-1]:,.2f}")
    if "rsi_14" in df.columns and df["rsi_14"].dropna().any():
        ultimo_rsi = df["rsi_14"].dropna().iloc[-1]
        c3.metric("RSI (14)", f"{ultimo_rsi:.2f}")
    if "signal" in df.columns:
        c4.metric("Señal Actual", df["signal"].iloc[-1])

    if "close" in df.columns:
        y_cols = [c for c in ["close", "bollinger_upper", "bollinger_lower"] if c in df.columns]
        fig = px.line(
            df,
            x="date",
            y=y_cols,
            title="Evolución Histórica BTCUSDT y Bandas de Bollinger",
            markers=True,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Detalle de Señales Técnicas (Orden Cronológico)")
    st.dataframe(df.sort_values("date", ascending=False), use_container_width=True)
