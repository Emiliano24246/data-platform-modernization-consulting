# RFP: MarketMatrix - Pipeline de datos de mercado (por lotes)

**Cliente:** MarketMatrix (Binance Data Engineering Project)
**Área solicitante:** Dirección de Operaciones y Riesgos
**Fecha de publicación:** 13 de octubre de 2026

## 1. Quiénes somos
Somos MarketMatrix, un fondo de inversión de trading cuantitativo. Nuestras estrategias de volatilidad de Bitcoin usan una lógica de triple confirmación con tres indicadores (RSI, Bandas de Bollinger y MACD) calculados sobre las velas (klines) de Binance.

## 2. El problema
- El pipeline que extrae las klines de Binance es inestable: a veces no sabemos con certeza si los datos del día están completos.
- La lógica de triple confirmación que corremos en la nube no está bien estructurada ni orquestada, y depende de pasos manuales.
- Nuestra mesa de control necesita las señales de COMPRA/VENTA actualizadas y confiables al inicio de cada jornada. No necesitamos tiempo real: una actualización diaria (o cada hora) es suficiente.
- El costo del proyecto debe ser razonable frente al valor que nos aporta.

## 3. Qué necesitamos lograr
| # | Necesidad | Cómo sabremos que se cumplió (medible) |
|---|---|---|
| N1 | Pipeline que corra solo y avise cuando falla | El pipeline corre en su horario durante 7 días seguidos sin intervención. Si una corrida falla, recibimos una alerta. |
| N2 | Resultados repetibles | Si se ejecuta dos veces para el mismo día, el resultado es idéntico y no hay filas duplicadas. |
| N3 | Indicadores iguales a los que usamos hoy | RSI, Bollinger y MACD coinciden con nuestro cálculo actual con una tolerancia de 0.000001 sobre un conjunto de 90 días de BTCUSDT que nosotros entregaremos. |
| N4 | Señales de COMPRA/VENTA listas para consultar | Una tabla en Athena se actualiza en cada corrida y la consulta de aceptación termina en menos de 10 segundos. |

## 4. Qué NO queremos en este proyecto
- Servidores virtuales encendidos de forma permanente (EC2).
- Herramientas de código abierto que tengamos que administrar nosotros.
- Procesamiento en tiempo real ni trading de baja latencia.
- El bot que ejecuta las órdenes: nuestros sistemas de trading consumen la señal, pero no son parte de este proyecto.

## 5. Datos que tenemos y datos que faltan
**Tenemos:** la extracción cruda de las klines en CSV y las transformaciones iniciales que ya usa nuestro equipo.
**Entregaremos:** el conjunto de 90 días de BTCUSDT con los resultados esperados de los indicadores (línea base para N3).
**Falta:** la orquestación del pipeline, la capa de datos lista para consulta y las señales en Athena.

## 6. Restricciones
- **Presupuesto máximo:** $550,000 MXN por el proyecto completo.
- **Plazo:** 10 semanas.
- **Seguridad y privacidad:** acceso con permisos de mínimo privilegio (IAM) y buckets de S3 aislados; no queremos que nadie vea más de lo necesario.
- **Quién lo va a usar:** nuestros sistemas de trading algorítmico y la mesa de control, leyendo desde Athena.
- **Servicios:** preferimos servicios básicos de AWS. Cualquier servicio adicional debe justificarse por escrito.

## 7. Qué esperamos recibir
- Pipeline automatizado y plantillas de CloudFormation para desplegarlo.
- Datos organizados en capas (cruda, limpia y lista para consulta) en S3.
- Tabla de señales en Athena.
- Pruebas de regresión que comparen los resultados contra nuestra línea base.
- Estimación de costos con la calculadora de AWS y una alerta de presupuesto.
- Documentación y guía de operación para nuestros ingenieros.

## 8. Cómo evaluaremos las propuestas
- 40 %: experiencia técnica comprobable en servicios de datos de AWS.
- 30 %: claridad del diseño y de cómo se protege la exactitud de los cálculos.
- 20 %: apego al presupuesto máximo.
- 10 %: viabilidad del plazo de 10 semanas con entregas por fases.

## 9. Qué debe incluir su propuesta
- Diagnóstico de nuestro pipeline actual, con evidencia de cada hallazgo.
- Alcance: qué incluye y qué no.
- Entregables con criterio de aceptación, ligados a las necesidades N1 a N4.
- Plan por hitos con horas estimadas por persona.
- Riesgos y supuestos (por ejemplo, los límites de solicitudes de la API de Binance).
- Presupuesto desglosado (incluido el costo estimado de AWS) y esquema de pagos.
- Equipo propuesto y roles.
