# SOW: MarketMatrix

**Consultora:** TAD Data Consulting
**Cliente:** Binance Data Engineering Project
**Respuesta al RFP del:** 13/10/2026
**Versión:** 1.0
**Equipo:** 
- Tristan Qesen Sanchez Mayen
- Sandoval Hernandez Darinka
- Salgado Cruz Roman Emiliano
- Tapia Ledesma Angel Hazel

## 1. Resumen ejecutivo
Comprendemos la urgencia de estabilizar su pipeline de datos de Binance para evitar riesgos financieros. Sustituiremos su infraestructura inestable por una arquitectura serverless elástica (AWS Lambda, Glue) que calcule con precisión y sin interrupciones los indicadores RSI, Bollinger y MACD. El proyecto se liberará en 10 semanas con una inversión de $441,560 MXN.

## 2. Alcance
**Dentro de alcance:**
- Orquestación desatendida mediante AWS Lambda y EventBridge.
- Desarrollo de scripts PySpark en AWS Glue para cálculo de indicadores.
- Creación de vistas materializadas de COMPRA/VENTA en Athena.
- Aislamiento de buckets S3 mediante políticas IAM de mínimo privilegio.

**Fuera de alcance:**
- Desarrollo o modificación del algoritmo de ejecución de órdenes de trading (razón: nosotros proveemos la señal, el bot de ejecución pertenece al cliente).
- Aprovisionamiento de servidores virtuales tipo EC2 (razón: se busca evitar costos inactivos).

## 3. Entregables y criterios de aceptación
| # | Entregable | Criterio de aceptación | Hito |
|---|---|---|---|
| E1 | Pipeline Orquestado | EventBridge dispara Lambda sin fallos durante 7 días continuos. | 2 |
| E2 | Cálculo de Indicadores en PySpark | Scripts en Glue procesan las Klines y devuelven RSI, MACD y Bollinger idénticos a su baseline actual. | 3 |
| E3 | Vistas de Señales (Athena) | Tablas actualizadas reflejan señales de COMPRA/VENTA consultables por la mesa de control. | 4 |

## 4. Trazabilidad con el RFP
| Necesidad del RFP | Entregable(s) que la cubre(n) | Estado |
|---|---|---|
| N1 Estabilización del Data Lakehouse | E1, E2 | Cubierta |
| N2 Escalabilidad ante alta frecuencia | E1, E2 | Cubierta |
| N3 Protección del capital (vistas) | E3 | Cubierta |

## 5. Enfoque técnico (alto nivel)
Desplegaremos una arquitectura de procesamiento Batch de alta frecuencia. Los datos Klines (CSV) se ingerirán vía Lambda hacia S3 Bronze. AWS Glue ejecutará scripts en PySpark para procesar volumetría pesada sin afectar el rendimiento, calculando los indicadores financieros y escribiéndolos en Parquet (Silver). AWS Athena expondrá estas señales en vistas materializadas para latencia mínima. Todo será desplegado usando Infraestructura como Código (Terraform/CloudFormation) para garantizar la replicabilidad.

## 6. Plan por hitos
| Hito | Qué se entrega | Evidencia | Fecha | Horas |
|---|---|---|---|---|
| 0 Arranque | Diseño y Setup IAM | Repositorio y permisos IAM (Mínimo Privilegio) configurados | Sem 1 | 80 |
| 1 Refactorización Lambda | Pipeline de extracción | Klines actualizados en bucket S3 aislados | Sem 3 | 160 |
| 2 Lógica PySpark | ETL en AWS Glue | Logs de cálculos matemáticos precisos | Sem 6 | 280 |
| 3 Capa de Consumo | Vistas materializadas Athena | Consultas de COMPRA/VENTA disponibles | Sem 8 | 160 |
| 4 Estabilización | Pruebas de estrés | Sistema soporta ráfagas simuladas sin degradarse | Sem 10 | 120 |

## 7. Equipo y responsabilidades
- **Tristan Qesen Sanchez Mayen:** Lead Data Engineer. A cargo de la arquitectura, seguridad IAM, y revisión de pull requests. Reporta a la Dirección de Operaciones del cliente.
- **Sandoval Hernandez Darinka:** Data Engineer. Responsable de la refactorización lógica de PySpark en AWS Glue para indicadores financieros. Reporta al Lead Data Engineer.
- **Salgado Cruz Roman Emiliano:** Data Engineer. Responsable de orquestación (EventBridge, AWS Lambda) y pruebas de estrés. Reporta al Lead Data Engineer.
- **Tapia Ledesma Angel Hazel:** Data Engineer. Responsable de la creación de vistas materializadas en Amazon Athena y exposición de señales para la mesa de control. Reporta al Lead Data Engineer.

## 8. Supuestos y dependencias del cliente
- El cliente proveerá acceso a los históricos de Klines en CSV para validar la exactitud de los nuevos algoritmos.
- El equipo de trading del cliente validará matemáticamente los indicadores (RSI, Bollinger, MACD) al cierre del Hito 2.
- La API de Binance no impondrá bloqueos de IP (Rate Limits) si operamos bajo las credenciales institucionales proveídas.

## 9. Riesgos
| Riesgo | Probabilidad | Impacto | Qué haremos |
|---|---|---|---|
| Discrepancia matemática en el cálculo de indicadores vs el sistema legado. | Alta | Alto | Pruebas unitarias de regresión en cada iteración de código PySpark contra un dataset de control aprobado. |
| Limitaciones de concurrencia en AWS Lambda | Baja | Medio | Solicitar aumento de cuota de concurrencia en la cuenta de AWS desde la semana 1. |

## 10. Presupuesto
| Concepto | Horas | Tarifa MXN/h | Subtotal MXN |
|---|---|---|---|
| Lead Data Engineer (Tristan) | 300 | $406.25 | $121,875 |
| Data Engineer (Darinka) | 300 | $281.25 | $84,375 |
| Data Engineer (Emiliano) | 300 | $281.25 | $84,375 |
| Data Engineer (Hazel) | 300 | $281.25 | $84,375 |
| Colchón de contingencia financiera (10%)| | | $37,500 |
| Costo estimado AWS inicial | | | $29,060 |
| **Total** | **1200** | | **$441,560** |

*El total propuesto está holgadamente por debajo de su techo financiero de $550,000 MXN.*

## 11. Control de cambios y comunicación
Los cambios en la lógica financiera se tratarán como CRs (Change Requests) críticos. Habrá comités de revisión técnica quincenales entre el Lead Data Engineer (Tristan) y los stakeholders del fondo de inversión.

## 12. Aceptación y pagos
Se requieren actas de aceptación técnica por parte de su mesa de control para liberar pagos. Esquema de facturación: 30% Hito 0, 30% Hito 2, 40% al cierre del Hito 4 tras superar las pruebas de estrés.