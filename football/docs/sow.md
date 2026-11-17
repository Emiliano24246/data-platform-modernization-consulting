# SOW: Football Soccer Data Engineering Project

**Consultora:** data-platform-modernization-consulting 
**Cliente:** Football Soccer Data Engineering Project (Dirección de Selecciones Nacionales)
**Respuesta al RFP del:** 13/10/2026
**Versión:** 2.0
**Equipo:** 
- Tristan Qesen Sanchez Mayen - Lead Data Engineer
- Darinka Sandoval Hernandez - Data Engineer
- Roman Emiliano Salgado Cruz - Data Engineer
- Angel Hazel Tapia Ledesma - Data Engineer
- Yessica Lizeth Caballero Garcia - Data Engineer

---

## 1. Resumen ejecutivo
Modernizaremos la infraestructura de analítica migrando la extracción en servidores EC2 permanentes hacia una arquitectura serverless, idempotente y por capas en AWS en 10 semanas por un monto total de $110,000 MXN. Implementaremos ingesta en Amazon S3 (Bronze crudo, Silver Parquet particionado por temporada y Gold consultable vía DuckDB/Athena), automatización con CloudFormation y EventBridge, y monitoreo con alarmas en CloudWatch y un AWS Budget con alertas. La solución garantiza un costo operativo mensual en AWS de $0.00 MXN (100% de ahorro vs EC2) y permite al cuerpo técnico consultar KPIs analíticos en menos de 10 segundos.

---

## 2. Diagnóstico
A partir de la revisión técnica del entorno actual, identificamos los siguientes hallazgos:

1. **Instancia EC2 encendida 24/7:** La extracción de API-Football se ejecuta en un servidor permanente. **Riesgo:** Sobrecostos fijos acumulados por tiempo de procesamiento ocioso. **Solución:** Reemplazar la instancia EC2 por funciones AWS Lambda detonadas por calendario mediante Amazon EventBridge (Entregables E1 y E2).
2. **Ausencia de Capa Bronze (Dato Crudo):** Se aplican transformaciones directas sin guardar el payload original de la API. **Riesgo:** Imposibilidad de re-procesar los datos si cambia la lógica de negocio o se detecta un error de cálculo]. **Solución:** Crear un bucket Bronze en S3 para almacenar la respuesta JSON original de la API de forma intacta (Entregables E1 y E2).
3. **Particionamiento ineficiente en S3:** Los archivos están particionados únicamente por año. **Riesgo:** Consultas SQL en Athena lentas y costosas por escaneo masivo de datos no requeridos. **Solución:** Transformación a formato Parquet con particionamiento por `temporada` en la capa Silver (Entregable E3).
4. **Falta de idempotencia en la carga:** Reejecutar el proceso el mismo día duplica registros en los resultados finales. **Riesgo:** Inconsistencia en las métricas de efectividad y goles consumidas por los analistas. **Solución:** Lógica de sobrescritura determinista por partición dentro de la función Lambda de limpieza (Entregable E3).
5. **Falta de monitoreo y visibilidad de costos:** No existen notificaciones si la ingesta falla ni control sobre el gasto de los servicios de AWS. **Riesgo:** Fallos no detectados a tiempo y desbordamiento del presupuesto. **Solución:** Creación de alarmas en CloudWatch con notificaciones por correo vía SNS y alerta activa en AWS Budget (Entregables E3 y E5).

---

## 3. Alcance
**Dentro de alcance:**
- Despliegue de infraestructura mediante plantillas replicables de AWS CloudFormation.
- Ingesta serverless programada con AWS Lambda y EventBridge para consumir API-Football sin servidores fijos.
- Almacenamiento organizado en Amazon S3 bajo arquitectura de tres capas: Bronze (JSON crudo), Silver (Parquet limpio y particionado por `temporada`) y Gold (vistas de KPIs).
- Almacenamiento seguro de credenciales de la API en AWS Systems Manager Parameter Store.
- Tablas y Vistas analíticas en Amazon Athena para consultas SQL de efectividad y goles por equipo y partido.
- Monitoreo de fallos con CloudWatch + SNS y creación de un AWS Budget con alertas automáticas.
- Documentación técnica y guía de operación (Runbook) para atención de fallas y mantenimiento.

**Fuera de alcance:**
- Software de procesamiento de video o tracking óptico en la cancha (razón: el alcance se limita al procesamiento de datos estadísticos estructurados de API-Football).
- Desarrollo de dashboards gráficos (QuickSight/PowerBI), aplicaciones web o interfaces de usuario (razón: el cliente consultará los resultados con SQL directo en Athena).
- Aprovisionamiento o administración de bases de datos relacionales tradicionales como RDS (razón: se cumple la restricción estricta de arquitectura serverless con Athena y S3).

---

## 4. Entregables y criterios de aceptación
| # | Entregable | Criterio de aceptación | Hito |
|---|---|---|---|
| **E1** | Infraestructura e Ingesta Serverless | Plantillas CloudFormation desplegadas. Función Lambda extrae el JSON de API-Football leyendo credenciales de Parameter Store vía EventBridge, sin usar instancias EC2. | 1 |
| **E2** | Capa Cruda (Bronze) | Respuestas de API-Football guardadas en S3 Bronze en su formato JSON original e intacto para permitir re-procesamientos. | 1 |
| **E3** | Capa Silver, Idempotencia y Alertas | Lógica Lambda que convierte JSON a Parquet particionado por `temporada`. Reejecuciones el mismo día no duplican registros. Alarma en CloudWatch notifica vía SNS si la ingesta falla. | 2 |
| **E4** | Capa Gold (KPIs Tácticos) | Tablas y vistas en Athena para consultar efectividad y goles. La consulta de aceptación sobre una temporada completa escanea menos de 100 MB y responde en menos de 10 segundos. | 3 |
| **E5** | FinOps, Documentación y Cierre | Entrega de estimación realizada en AWS Calculator, alerta de AWS Budget activa, reporte comparativo de ahorro de costos vs EC2 y Runbook de operación. | 4 |

---

## 5. Trazabilidad con el RFP
| Necesidad del RFP | Entregable(s) que la cubre(n) | Estado |
|---|---|---|
| **N1:** Reducción de costo operativo y eliminación de EC2 | E1, E5 | Cubierta |
| **N2:** Consultas rápidas sobre una temporada completa | E3, E4 | Cubierta |
| **N3:** KPIs tácticos listos para el cuerpo técnico | E4 | Cubierta |
| **N4:** Extracción automática y confiable (sin duplicados) | E1, E3 | Cubierta |
| **N5:** Conservar el dato crudo tal como llega de la API | E2 | Cubierta |

---

## 6. Enfoque técnico (alto nivel)
Desplegaremos todo el entorno mediante plantillas de AWS CloudFormation. Un evento programado en Amazon EventBridge activará diariamente una función AWS Lambda que leerá la API Key desde Parameter Store, consumirá la API-Football y depositará la respuesta en `s3://soccer-data-lake-bronze/` en formato JSON intacto. Una segunda función Lambda procesará los JSONs, los limpiará e impondrá tipado, reescribiéndolos en `s3://soccer-data-lake-silver/` en formato Parquet aplicando sobrescritura determinista sobre la partición `temporada={año}` para asegurar idempotencia. Para la capa analítica (Gold), los analistas podrán ejecutar consultas sobre Amazon Athena o de forma gratuita sin costo de escaneo mediante DuckDB/Google Colab conectado directamente al S3. El monitoreo operativo se gestionará con alarmas CloudWatch con notificaciones vía SNS por correo y un AWS Budget con alertas automáticas de costo.

---

## 7. Plan por hitos
| Hito | Qué se entrega | Evidencia | Fecha | Horas |
|---|---|---|---|---|
| 0 Arranque | Diagnóstico detallado y diseño de arquitectura. | Plantilla base en CloudFormation y repositorio configurado. | Sem 2 | 40 |
| 1 Ingesta | Ingesta serverless y persistencia en Capa Bronze. | Lambda ejecutando con EventBridge guardando JSONs en S3. | Sem 4 | 80 |
| 2 Optimización | Capa Silver en Parquet, idempotencia y alertas. | Archivos Parquet particionados por temporada y notificación SNS probada. | Sem 7 | 90 |
| 3 Capa Analítica | Vistas de KPIs tácticos en Athena. | Queries de validación ejecutadas en < 10 s escaneando < 100 MB. | Sem 9 | 60 |
| 4 Entrega | Documentación FinOps, Runbook y apagado EC2. | Reporte AWS Calculator, AWS Budget activo y Runbook entregado. | Sem 10 | 30 |
| **Total** | | | | **300 h** |

---

## 8. Equipo y responsabilidades
- **Tristan Qesen Sanchez Mayen (Lead Data Engineer):** Diseño de la arquitectura en CloudFormation, modelo de seguridad IAM, estrategia FinOps y coordinación general. Reporta a la Dirección de Selecciones Nacionales. (80 h).
- **Darinka Sandoval Hernandez (Data Engineer):** Desarrollo de la transformación a Parquet en Capa Silver, estrategia de particionamiento por temporada e idempotencia. Reporta a Tristan. (55 h).
- **Roman Emiliano Salgado Cruz (Data Engineer):** Desarrollo de funciones Lambda de ingesta (Capa Bronze), integración con la API-Football y automatización en EventBridge. Reporta a Tristan. (55 h).
- **Angel Hazel Tapia Ledesma (Data Engineer):** Construcción del catálogo en AWS Glue, Vistas analíticas de KPIs en Athena/DuckDB y pruebas de rendimiento de consultas SQL. Reporta a Tristan. (55 h).
- **Yessica Lizeth Caballero Garcia (Data Engineer):** Configuración de alertas CloudWatch/SNS, gestión de credenciales en Parameter Store y elaboración del Runbook de operación. Reporta a Tristan. (55 h).

---

## 9. Supuestos y dependencias del cliente
- El cliente entregará las credenciales activas de la API-Football y la factura actual de AWS de la instancia EC2 para establecer la línea base de costos.
- El equipo de Inteligencia Deportiva del cliente validará las fórmulas SQL de "efectividad" durante las semanas 7 y 8.
- No existen otros sistemas ni procesos activos dentro de la instancia EC2 que impidan su apagado definitivo al cierre del proyecto.

---

## 10. Riesgos
| Riesgo | Probabilidad | Impacto | Qué haremos |
|---|---|---|---|
| Las dependencias de las librerías en Python superan el límite de tamaño directo de AWS Lambda. | Media | Medio | Empaquetar las dependencias dentro de AWS Lambda Layers optimizadas o comprimidas. |
| Cambios no anunciados en la estructura JSON devuelta por la API-Football. | Baja | Alto | Almacenar siempre el dato en Bronze crudo y aplicar validación de esquema previa a la transformación a Silver. |
| Retrasos por parte del cliente en la validación de los KPIs analíticos. | Media | Medio | Acordar la matriz de fórmulas durante el Hito 0 e implementarlas mediante Vistas SQL modulares. |

---

## 11. Presupuesto
| Concepto | Horas | Tarifa MXN/h | Subtotal MXN |
|---|---|---|---|
| Lead Data Engineer (Tristan) | 80 | $350.00 | $28,000.00 |
| Data Engineer (Yessica Lizeth) | 55 | $280.00 | $15,400.00 |
| Data Engineer (Darinka) | 55 | $280.00 | $15,400.00 |
| Data Engineer (Emiliano) | 55 | $280.00 | $15,400.00 |
| Data Engineer (Angel Hazel) | 55 | $280.00 | $15,400.00 |
| Colchón de imprevistos (10%) | - | - | $8,960.00 |
| Costo estimado de AWS (Proyección 10 semanas)* | - | - | $0.00 |
| **Total** | **300** | | **$110,000.00** |



---

## 12. Control de cambios y comunicación
La comunicación se realizará mediante reuniones semanales de seguimiento y un canal corporativo dedicado. Cualquier requerimiento adicional de KPIs o cambios en el alcance se gestionará mediante una Solicitud de Cambio (CR), evaluando su impacto en costo y tiempo antes de la aprobación del cliente.

---

## 13. Aceptación y pagos
Los entregables se darán por aceptados tras su validación en el entorno de pruebas conforme a los criterios de aceptación. El esquema de pagos se distribuirá de la siguiente manera:
- **30% Anticipo (Hito 0):** A la firma del SOW y aprobación del diseño de arquitectura inicial.
- **40% Avance (Hito 2):** Al liberar la ingesta idempotente en Capa Silver en formato Parquet.
- **30% Cierre (Hito 4):** A la entrega de vistas en Athena, documentación, Runbook operativo y apagado formal de EC2.