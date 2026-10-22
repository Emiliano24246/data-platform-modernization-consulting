# SOW: Sistema de Analítica de Fútbol

**Consultora:** TAD Data Consulting
**Cliente:** Football Soccer Data Engineering Project
**Respuesta al RFP del:** 13/10/2026
**Versión:** 1.1
**Equipo:** 
- Tristan Qesen Sanchez Mayen
- Sandoval Hernandez Darinka
- Salgado Cruz Roman Emiliano
- Tapia Ledesma Angel Hazel

## 1. Resumen ejecutivo
Comprendemos que su infraestructura actual basada en EC2 es costosa e ineficiente, limitando la toma de decisiones tácticas. Migraremos la ingesta de la API-Football hacia una arquitectura Serverless, optimizando la capa analítica con particionamiento Parquet para que el cuerpo técnico consulte KPIs al instante. Este proyecto de 10 semanas asegura una modernización total por una inversión de $441,560 MXN, sin rebasar su tope presupuestal.

## 2. Alcance
**Dentro de alcance:**
- Eliminación de deuda técnica: Desmantelamiento de la instancia EC2.
- Refactorización de scripts Python para operar nativamente en AWS Lambda.
- Implementación de particionamiento optimizado (formato Parquet) en S3 (Silver).
- Generación de vistas avanzadas (Gold) en Athena (goles, efectividad).
**Fuera de alcance:**
- Software de recolección de video o tracking óptico en la cancha (razón: el alcance se limita al procesamiento de eventos y estadísticas de API-Football).
- Uso de bases de datos relacionales tradicionales como RDS (razón: mandato estricto de Serverless/Athena).

## 3. Entregables y criterios de aceptación
| # | Entregable | Criterio de aceptación | Hito |
|---|---|---|---|
| E1 | Ingesta Serverless | Lambda ejecuta los scripts Python extrayendo datos de API-Football sin necesidad de EC2. | 2 |
| E2 | Capa Silver Optimizada | Datos en S3 almacenados en Parquet con particionamiento por liga/fecha, reduciendo tiempo de lectura. | 3 |
| E3 | Vistas Tácticas (Athena) | Cuerpo técnico puede consultar KPIs (efectividad, goles a favor/contra) mediante SQL en Athena. | 4 |

## 4. Trazabilidad con el RFP
| Necesidad del RFP | Entregable(s) que la cubre(n) | Estado |
|---|---|---|
| N1 Migración Serverless | E1 | Cubierta |
| N2 Optimización de Consultas Analíticas | E2 | Cubierta |
| N3 Inteligencia Deportiva (KPIs) | E3 | Cubierta |

## 5. Enfoque técnico (alto nivel)
Nuestra estrategia FinOps apagará inmediatamente los servidores EC2. Los scripts Python existentes se empaquetarán como capas de AWS Lambda (Lambda Layers) para la extracción desde API-Football. La optimización principal se dará en el Data Lake: transformaremos los datos a Parquet utilizando AWS Glue, y aplicaremos un particionamiento de grano fino (Año/Mes/Competición) en S3. Athena escaneará solo las particiones necesarias, abatiendo drásticamente el costo por query y los tiempos de espera del cuerpo técnico.

## 6. Plan por hitos
| Hito | Qué se entrega | Evidencia | Fecha | Horas |
|---|---|---|---|---|
| 0 Auditoría | Plan de migración | Reporte de apagado EC2 y matriz IAM | Sem 1 | 40 |
| 1 Ingesta | Lambdas desplegadas | Extracción desde API-Football exitosa en S3 | Sem 3 | 160 |
| 2 Optimización | Particionamiento | Archivos Parquet organizados en la capa Silver | Sem 6 | 320 |
| 3 Capa Analítica| Vistas de KPIs en Athena | Queries de validación de inteligencia deportiva | Sem 8 | 160 |
| 4 Entrega | Documentación FinOps | Análisis de ahorro de costos EC2 vs Serverless entregado | Sem 10 | 120 |

## 7. Equipo y responsabilidades
- **Tristan Qesen Sanchez Mayen:** Lead Data Engineer. Diseñador de la arquitectura Serverless, estrategia FinOps y particionamiento. Reporta a Inteligencia Deportiva.
- **Darinka Sandoval Hernandez:** Data Engineer. Encargada del ETL en AWS Glue y conversión a Parquet en la capa Silver. Reporta a Tristan.
- **Roman Emiliano Salgado Cruz:** Data Engineer. Encargado de la refactorización del código Python a Lambda Layers y contenedorización para la ingesta de datos. Reporta a Tristan.
- **Angel Hazel Tapia Ledesma:** Data Engineer. Encargado del modelado de vistas analíticas en Athena y pruebas unitarias. Reporta a Tristan.

## 8. Supuestos y dependencias del cliente
- El cliente entregará acceso completo a los scripts Python actuales y credenciales de la API-Football.
- El área de Inteligencia Deportiva validará la lógica de negocio detrás del cálculo de los KPIs tácticos.
- No existen otros servicios productivos acoplados a la instancia EC2 que se va a eliminar.

## 9. Riesgos
| Riesgo | Probabilidad | Impacto | Qué haremos |
|---|---|---|---|
| Los scripts Python actuales tienen dependencias pesadas que superan el límite de tamaño de AWS Lambda (250MB). | Media | Alto | Contenedorizar la extracción utilizando Lambda con imágenes de Docker o ECS Fargate como plan de respaldo. |
| Resistencia técnica al cambio de interfaz de consulta. | Baja | Medio | Proveer capacitación documentada sobre la conexión a Athena. |

## 10. Presupuesto
| Concepto | Horas | Tarifa MXN/h | Subtotal MXN |
|---|---|---|---|
| Lead Data Engineer (Tristan) | 300 | $406.25 | $121,875 |
| Data Engineer (Darinka) | 300 | $281.25 | $84,375 |
| Data Engineer (Emiliano) | 300 | $281.25 | $84,375 |
| Data Engineer (Angel Hazel) | 300 | $281.25 | $84,375 |
| Colchón de imprevistos (10%) | | | $37,500 |
| Costo estimado inicial AWS | | | $29,060 |
| **Total** | **1200** | | **$441,560** |
*El presupuesto es altamente competitivo, manteniéndose por debajo de los $550,000 MXN requeridos en el RFP.*

## 11. Control de cambios y comunicación
La comunicación se centralizará a través de un canal corporativo y revisiones semanales de progreso. Cualquier KPI nuevo que Inteligencia Deportiva requiera fuera de la matriz inicial entrará a un backlog de CR (Change Request) sujeto a estimación de horas adicionales.

## 12. Aceptación y pagos
Los hitos se liberarán tras validación técnica en el ambiente de pruebas. El pago será en tres exhibiciones: 30% anticipo (Hito 0), 40% al liberar el particionamiento (Hito 2), y 30% al cierre y apagado definitivo del entorno EC2 viejo (Hito 4).