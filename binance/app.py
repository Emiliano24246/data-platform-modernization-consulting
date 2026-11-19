import boto3
import awswrangler as wr
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Binance BTCUSDT — Analytics", page_icon="📈", layout="wide")
st.title("📈 Binance BTCUSDT — Suite Analítica Gold (MarketMatrix)")
st.caption("Entorno aislado | Base de datos: marketmatrix_db_dev")

ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
S3_OUTPUT = f"s3://binance-data-lake-dev-{ACCOUNT_ID}/athena-results/"
DATABASE = "marketmatrix_db_dev"

@st.cache_data(ttl=60)
def cargar_datos():
    df = wr.athena.read_sql_query(
        "SELECT * FROM binance_gold_signals ORDER BY date ASC LIMIT 500",
        database=DATABASE,
        s3_output=S3_OUTPUT,
    )
    if "date" in df.columns:
        df = df.sort_values("date").reset_index(drop=True)
    return df

with st.sidebar:
    st.header("⚙️ Controles Binance")
    if st.button("🔄 Actualizar desde Athena", use_container_width=True):
        st.cache_data.clear()

df = cargar_datos()

if df.empty:
    st.warning("La tabla binance_gold_signals aún no tiene registros.")
else:
    with st.sidebar:
        fechas = df["date"].tolist()
        rango = st.select_slider(
            "Rango de fechas",
            options=fechas,
            value=(fechas[0], fechas[-1]),
        )
    df_f = df[(df["date"] >= rango[0]) & (df["date"] <= rango[1])].copy()

    # KPIs principales
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Velas Analizadas", len(df_f))
    c2.metric("Último Cierre", f"${df_f['close'].iloc[-1]:,.2f}")
    c3.metric("Máximo Período", f"${df_f['close'].max():,.2f}")
    c4.metric("Mínimo Período", f"${df_f['close'].min():,.2f}")
    ultimo_rsi = df_f["rsi_14"].dropna().iloc[-1] if df_f["rsi_14"].dropna().any() else None
    c5.metric("RSI (14) Actual", f"{ultimo_rsi:.2f}" if ultimo_rsi is not None else "N/A")

    # Gráfica 1: Precio + Bandas de Bollinger
    y_cols = [c for c in ["close", "bollinger_upper", "bollinger_lower"] if c in df_f.columns]
    fig_precio = px.line(
        df_f,
        x="date",
        y=y_cols,
        title="1. Precio de Cierre BTCUSDT y Bandas de Bollinger (20d)",
        markers=True,
    )
    st.plotly_chart(fig_precio, use_container_width=True)

    # Fila de 2 columnas: RSI y MACD
    col_izq, col_der = st.columns(2)

    with col_izq:
        if "rsi_14" in df_f.columns:
            fig_rsi = px.line(
                df_f,
                x="date",
                y="rsi_14",
                title="2. Índice de Fuerza Relativa — RSI (14)",
                markers=True,
            )
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Sobrecompra (70)")
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="green", annotation_text="Sobreventa (30)")
            fig_rsi.update_yaxes(range=[0, 100])
            st.plotly_chart(fig_rsi, use_container_width=True)

    with col_der:
        if "macd_line" in df_f.columns:
            fig_macd = go.Figure()
            fig_macd.add_trace(go.Bar(x=df_f["date"], y=df_f["macd_line"], name="MACD Line"))
            if "macd_signal" in df_f.columns and df_f["macd_signal"].dropna().any():
                fig_macd.add_trace(go.Scatter(x=df_f["date"], y=df_f["macd_signal"], mode="lines+markers", name="Signal Line"))
            fig_macd.update_layout(title="3. Momentum MACD (12, 26, 9)", xaxis_title="Fecha", yaxis_title="Valor")
            st.plotly_chart(fig_macd, use_container_width=True)

    # Fila inferior: Distribución de señales y Rendimiento diario (%)
    col_b1, col_b2 = st.columns(2)

    with col_b1:
        if "signal" in df_f.columns:
            conteo_senales = df_f["signal"].value_counts().reset_index()
            conteo_senales.columns = ["Señal", "Días"]
            fig_pie = px.pie(
                conteo_senales,
                names="Señal",
                values="Días",
                hole=0.45,
                title="4. Distribución de Señales de Trading",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    with col_b2:
        df_f["retorno_pct"] = df_f["close"].pct_change() * 100
        fig_ret = px.bar(
            df_f.dropna(subset=["retorno_pct"]),
            x="date",
            y="retorno_pct",
            color="retorno_pct",
            color_continuous_scale="RdYlGn",
            title="5. Variación Porcentual Diaria (%)",
        )
        st.plotly_chart(fig_ret, use_container_width=True)

    with st.expander("📋 Ver Tabla Completa de Indicadores Gold"):
        st.dataframe(df_f.sort_values("date", ascending=False), use_container_width=True)
