# SOW: Asteroid Data Engineering Project

**Consultora:** TAD Data Consulting
**Cliente:** MarketMatrix (Binance Data Engineering Project), Dirección de Operaciones y Riesgos
**Respuesta al RFP del:** 13/10/2026
**Versión:** 2.0
**Equipo:** 
- Tristan Qesen Sanchez Mayen - Lead Data Engineer
- Darinka Sandoval Hernandez - Data Engineer
- Roman Emiliano Salgado Cruz - Data Engineer
- Angel Hazel Tapia Ledesma - Data Engineer
- Yessica Lizeth Caballero Garcia - Data Engineer

## 1. Resumen ejecutivo
Entendimos que la extracción diaria de Binance es inestable, la limpieza depende de pasos manuales y las señales carecen de validación de exactitud. Entregaremos un pipeline serverless por lotes (diario) que extrae klines, valida completitud, calcula RSI, Bollinger y MACD con tolerancia de 0.000001 frente a su línea base y publica las señales en Athena. La solución incluye plantillas CloudFormation, alertas CloudWatch/SNS, pruebas de regresión y AWS Budget. Se entregará en 10 semanas por un monto total de $110,000 MXN.

## 2. Diagnóstico
A partir de la revisión técnica del entorno actual, identificamos los siguientes hallazgos:

1. **Extracción inestable e incompleta:** No existe certeza de que los datos del día estén completos al extraer la vela diaria. **Riesgo:** Generación de señales erróneas para la mesa de control. **Solución:** Lógica de validación de completitud en Lambda que falla y alerta sin publicar si detecta faltantes (Entregables E1 y E2).
2. **Proceso de limpieza no automatizado (pasos manuales):** La limpieza depende de ejecuciones manuales intermedias. **Riesgo:** La mesa de control no dispone de las señales al inicio de la jornada. **Solución:** Cadena automática de tres funciones Lambda orquestada por Amazon EventBridge (Entregable E1).
3. **Uso de Glue con PySpark para volumen pequeño:** Se planteaba Glue/Spark para procesar una sola vela diaria por par. **Riesgo:** Sobrecostos innecesarios por tiempo de aprovisionamiento de nodos. **Solución:** Refactorización a Python con pandas nativo en AWS Lambda (Entregables E1, E2 y E3).
4. **Cálculos en vistas SQL no documentadas sin pruebas de regresión:** Los indicadores se calculan al vuelo en vistas de Athena sin tolerancia matemática verificada. **Riesgo:** Inconsistencia en las señales emitidas e imposibilidad de auditoría. **Solución:** Indicadores calculados en Python con tolerancia absoluta ≤ 0.000001 frente a los 90 días de línea base entregados por el cliente (Entregables E3 y E4).
5. **Falta de monitoreo y notificaciones de falla:** Las interrupciones del pipeline no emiten avisos oportunos. **Riesgo:** Detección tardía de fallos operacionales. **Solución:** Alarmas de CloudWatch integradas con SNS para alertas automáticas por correo (Entregable E1)
6. **Políticas IAM y permisos por revisar:** No se aplica el principio de mínimo privilegio ni aislamiento estricto. **Riesgo:** Exposición no autorizada de datos de mercado. **Solución:** Un bucket S3 por capa, permisos IAM de mínimo privilegio y rol de solo lectura para la mesa de control (Entregable E5).

## 3. Alcance
**Dentro de alcance:**
- Diagnóstico del pipeline actual con evidencia técnica de hallazgos.
- Extracción diaria automatizada de la vela de 1 día para BTCUSDT desde la API pública de Binance a las 00:05 UTC.
- Organización en Amazon S3 en tres capas aisladas: Bronze (crudo), Silver (Parquet limpio con indicadores) y Gold (tabla de señales).
- Verificación de completitud y calidad de datos, con cargas idempotentes y repetibles sin duplicados.
- Cálculo determinista en Python de RSI, Bandas de Bollinger y MACD, con suite de pruebas de regresión automáticas contra la línea base del cliente.
- Tabla de señales (COMPRA FUERTE, VENTA, MANTENER) en Athena generada mediante CTAS.
- Monitoreo con alarmas de CloudWatch, notificaciones SNS, plantillas CloudFormation y alerta de costo en AWS Budget.
- Documentación técnica y Runbook de operación.

**Fuera de alcance:**
- Desarrollo o modificación del bot automatizado que ejecuta las órdenes de trading (razón: el cliente solo consume la señal).
- Procesamiento en tiempo real, streaming, alta frecuencia o trading de baja latencia (razón: el proyecto es por lotes).
- Servidores EC2 permanentes o herramientas de código abierto administradas (razón: el RFP las excluye).
- Jobs de AWS Glue o PySpark (razón: el volumen diario cabe en AWS Lambda con pandas y no requiere el costo de Spark).
- Modificación de estrategias financieras, backtesting u optimización de parámetros de los indicadores (razón: la lógica financiera es responsabilidad del cliente).

## 4. Entregables y criterios de aceptación
| # | Entregable | Criterio de aceptación | Hito |
|---|---|---|---|
| E1 | Pipeline orquestado con alertas | (a) El pipeline por lotes corre en su horario durante 7 días seguidos sin intervención manual. (b) Una falla provocada en la ingesta genera una alerta por correo vía SNS en menos de 15 minutos. | 4 |
| E2 | Capas Bronze y Silver con calidad e idempotencia | (a) Reejecutar la carga para el mismo día produce resultados idénticos en S3 sin filas duplicadas. (b) Si la vela diaria falta o está incompleta, la corrida falla, alerta y no publica. (c) Registros no válidos se aislan. | 1 |
| E3 | Indicadores y pruebas de regresión | (a) Los valores de RSI, Bollinger y MACD coinciden con la línea base con diferencia absoluta ≤ 0.000001 sobre los 90 días de BTCUSDT entregados. (b) La suite de pruebas de regresión pasa e identifica desviaciones. | 2 |
| E4 | Tabla de señales en Athena | (a) La tabla Gold se reconstruye en cada corrida vía CTAS con las señales COMPRA FUERTE, VENTA o MANTENER. (b) Las señales coinciden al 100% con la línea base de 90 días. (c) La consulta de aceptación en Athena responde en < 10. | 3 |
| E5 | Plantillas CloudFormation y AWS Budget | (a) Plantillas en CloudFormation despliegan la infraestructura desde cero. (b) Permisos IAM de mínimo privilegio. (c) Alerta activa de AWS Budget configurada y probada. | 5 |
| E6 | Documentación y Runbook | Documentación de arquitectura y Runbook entregados; el equipo del cliente resuelve una falla simulada guiándose únicamente con el Runbook. | 5 |

## 5. Trazabilidad con el RFP
| Necesidad del RFP | Entregable(s) que la cubre(n) | Estado |
|---|---|---|
| N1 | E1 | Cubierta |
| N2 | E2, E4 | Cubierta |
| N3 | E3 | Cubierta |
| N4 | E4 | Cubierta |

## 6. Enfoque técnico (alto nivel)
Desplegaremos una arquitectura serverless por lotes utilizando CloudFormation. EventBridge ejecutará diariamente a las 00:05 UTC una función Lambda que extraerá la vela de Binance y la depositará intacta en `s3://marketmatrix-bronze/`. Una segunda función Lambda verificará la completitud del dato, limpiará estructuras y calculará RSI, Bollinger y MACD en Python de 64 bits, guardando el resultado en `s3://marketmatrix-silver/` en formato Parquet con sobrescritura determinista por fecha para asegurar idempotencia. Una tercera Lambda ejecutará una consulta CTAS en Athena para regenerar la tabla Gold en `s3://marketmatrix-gold/` con la regla de triple confirmación. El monitoreo se gestionará mediante alarmas CloudWatch conectadas a SNS y un AWS Budget con alertas de gasto.

## 7. Plan por hitos
| Hito | Qué se entrega | Evidencia | Fecha | Horas |
|---|---|---|---|---|
| 0 Arranque | Diagnóstico técnico, línea base y stack base. | Línea base de 90 días recibida; conexión Lambda a Binance validada; diagnóstico documentado; buckets y AWS Budget en CloudFormation. | Sem 2 | 40 |
| 1 Extracción | E2: Capas Bronze/Silver e idempotencia. | Dos ejecuciones del mismo día con resultados idénticos; prueba de falla ante datos incompletos. | Sem 4 | 60 |
| 2 Indicadores | E3: Cálculo e indicadores. | Suite de pruebas de regresión en verde con diferencia ≤ 0.000001 frente a los 90 días. | Sem 6 | 70 |
| 3 Señales | E4: Tabla de señales en Athena. | Tabla Gold en Athena validada; consultas en Athena < 10 s. | Sem 8 | 50 |
| 4 Operación desatendida | E1: Ejecución por 7 días y alertas. | Historial de 7 corridas diarias exitosas desatendidas y prueba de alerta por SNS. | Sem 9 | 50 |
| 5 Cierre | E5/E6: CloudFormation, permisos y Runbook. | Despliegue limpio desde CloudFormation, permisos IAM verificados y simulacro con el Runbook. | Sem 10 | 30 |
| **Total** | | | | **300 h** |

## 8. Equipo y responsabilidades
- **Tristan Qesen Sanchez Mayen (Lead Data Engineer):**  Arquitectura serverless, plantillas CloudFormation, seguridad IAM y coordinación del proyecto. Reporta a la Dirección de Operaciones y Riesgos.
- **Darinka Sandoval Hernandez (Data Engineer):** Implementación en Python de RSI, Bollinger y MACD, precisión numérica y suite de pruebas de regresión. Reporta a Tristan.
- **Roman Emiliano Salgado Cruz (Data Engineer):**  Extracción de klines desde la API de Binance en AWS Lambda, manejo de reintentos e ingesta en Capa Bronze. Reporta a Tristan.
- **Angel Hazel Tapia Ledesma (Data Engineer):** Generación de la tabla Gold de señales en Athena vía CTAS, regla de triple confirmación y asignación de roles de solo lectura. Reporta a Tristan.
- **Yessica Lizeth Caballero Garcia (Data Engineer):** Validación de completitud y calidad, pruebas de idempotencia, seguimiento de la corrida de 7 días, monitoreo CloudWatch/SNS y Runbook. Reporta a Tristan.

## 9. Supuestos y dependencias del cliente
- El cliente entregará el dataset de 90 días de BTCUSDT con las klines y resultados esperados de los indicadores al cierre del Hito 0 (Semana 2).
- El cliente entregará por escrito los periodos exactos de los indicadores (RSI, Bollinger y MACD) y las reglas de coincidencia de señales al cierre del Hito 0.
- El cliente otorgará acceso a una cuenta de AWS con permisos para crear roles IAM y desplegar recursos vía CloudFormation.
- La API pública de Binance es accesible desde la región de AWS seleccionada sin bloqueos de IP.
- Los sistemas del cliente se limitan a consumir la tabla de Athena; la integración con el bot ejecutor es responsabilidad del cliente.
- Los periodos de RSI, Bollinger y MACD no están documentados en el legado.
- La consulta de aceptación de N4 se acuerda en el Hito 0.

## 10. Riesgos
| Riesgo | Probabilidad | Impacto | Qué haremos |
|---|---|---|---|
| Diferencia numérica entre el cálculo en Python y la línea base del cliente. | Alta | Alto | Acordar convenciones de redondeo y periodos en el Hito 0; ejecutar pruebas de regresión automatizadas en cada cambio. |
| Retrasos en la entrega de la línea base de 90 días por parte del cliente. | Media | Alto | Solicitar la información en el Hito 0; si hay demoras, gestionar una prórroga vía Solicitud de Cambio (CR). |
| Límites de velocidad de solicitudes (rate limiting) en la API pública de Binance. | Media | Medio | Minimizar llamadas a la API, aplicar pausas y manejar reintentos exponenciales en Lambda. |
| Muestra de datos diaria entregada de forma incompleta por la API. | Media | Alto | Implementar validaciones que aborten la ejecución, impidan la publicación en Athena y emitan alerta SNS. |

## 11. Presupuesto
| Concepto | Horas | Tarifa MXN/h | Subtotal MXN |
|---|---|---|---|
| Lead Data Engineer (Tristan) | 80 | $350.00 | $28,000 |
| Data Engineer (Yessica Lizeth) | 55 | $280.00 | $15,400 |
| Data Engineer (Darinka) | 55 | $280.00 | $15,400 |
| Data Engineer (Emiliano) | 55 | $280.00 | $15,400 |
| Data Engineer (Angel Hazel) | 55 | $280.00 | $15,400 |
| Colchón de imprevistos (10%)
| Costo estimado de AWS
| **Total**| **300** | | |

 

## 12. Control de cambios y comunicación
Cualquier modificación en los indicadores o requerimientos fuera del alcance acordado se tramitará como una Solicitud de Cambio (CR) con estimación de horas. Se mantendrá una reunión semanal de avance y una revisión técnica quincenal entre el Lead (Tristan) y el cliente.

## 13. Aceptación y pagos
Un hito se dará por aceptado cuando cumpla sus criterios de aceptación y el representante del cliente firme el acta correspondiente. El cliente dispondrá de 5 días hábiles para revisar.

Porcentaje del precio que se libera:
- 20% a la aceptación del Hito 0.
- 30% a la aceptación del Hito 2.
- 20% a la aceptación del Hito 4.
- 30% a la aceptación del Hito 5.
