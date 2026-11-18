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
Modernizaremos la infraestructura de analítica migrando la extracción en servidores EC2 permanentes hacia una arquitectura serverless, idempotente y por capas en AWS en 10 semanas por $110,000 MXN de honorarios. Los cargos de AWS se estimarán y aprobarán por separado; dependen del uso y no se garantiza que sean cero. Implementaremos ingesta en un bucket S3 con prefijos Bronze, Silver, cuarentena y Gold; automatización con CloudFormation y EventBridge; y monitoreo con alarmas CloudWatch/SNS y AWS Budget. El objetivo es que el costo mensual estimado sea al menos 50 % menor que la línea base EC2 que entregue el cliente.

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
- Almacenamiento en un bucket Amazon S3 con prefijos aislados por permisos: Bronze (JSON crudo), Silver (Parquet particionado por `season`), cuarentena, Gold y resultados de Athena.
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
| **E4** | Capa Gold (KPIs Tácticos) | Tabla `football_kpis_gold` y vistas `football_effectiveness` y `football_match_kpis` en Athena. La consulta de aceptación sobre una temporada completa escanea menos de 100 MB y responde en menos de 10 segundos. | 3 |
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
Desplegaremos el entorno mediante CloudFormation. EventBridge activará diariamente a las 12:00 UTC una Lambda que leerá la API key `SecureString` desde Parameter Store, consultará API-Football para el día anterior y guardará la respuesta JSON intacta en `s3://football-data-lake-<entorno>-<cuenta>/bronze/`. Así se da tiempo a que finalicen los partidos antes de calcular resultados. Silver validará estructura, tipos y marcadores; conservará en Parquet los partidos todavía no disputados (goles nulos), aislará registros malformados en `cuarentena/` y escribirá los archivos válidos en `silver/season={season}/`. Un evento manual `{"mode":"backfill"}` procesa los archivos Bronze existentes y publica un solo marcador `control/gold-ready/` al finalizar. Ese marcador activa Gold, que considera únicamente estados finales `FT`, `AET` y `PEN`, crea la tabla agregada y las vistas `football_effectiveness` y `football_match_kpis` en Athena, y materializa la tabla en `gold/kpis/`. La efectividad se define provisionalmente como porcentaje de victorias y debe ser aprobada por el cliente. Alertas CloudWatch/SNS y AWS Budget monitorean la operación y el costo.

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
- **Angel Hazel Tapia Ledesma (Data Engineer):** Construcción del catálogo en AWS Glue, vistas analíticas de KPIs en Athena y pruebas de rendimiento de consultas SQL. Reporta a Tristan. (55 h).
- **Yessica Lizeth Caballero Garcia (Data Engineer):** Configuración de alertas CloudWatch/SNS, gestión de credenciales en Parameter Store y elaboración del Runbook de operación. Reporta a Tristan. (55 h).

---

## 9. Supuestos y dependencias del cliente
- El cliente entregará las credenciales activas de la API-Football y la factura actual de AWS de la instancia EC2 para establecer la línea base de costos.
- El equipo de Inteligencia Deportiva del cliente validará las fórmulas SQL de "efectividad" durante las semanas 7 y 8.
- La métrica provisional `effectiveness_percentage` se calcula como `wins / matches_played * 100`; los criterios de aceptación de KPIs dependen de la aprobación de la fórmula de negocio y de las tres consultas acordadas.
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
| Diferencia del importe fijo pendiente de desglose | - | - | $11,440.00 |
| Cargos de AWS (estimación por AWS Calculator) | - | - | Por estimar; se aprueban y pagan por separado |
| **Total de honorarios** | **300** | | **$110,000.00** |



---

## 12. Control de cambios y comunicación
La comunicación se realizará mediante reuniones semanales de seguimiento y un canal corporativo dedicado. Cualquier requerimiento adicional de KPIs o cambios en el alcance se gestionará mediante una Solicitud de Cambio (CR), evaluando su impacto en costo y tiempo antes de la aprobación del cliente.

---

## 13. Aceptación y pagos
Los entregables se darán por aceptados tras su validación en el entorno de pruebas conforme a los criterios de aceptación. El esquema de pagos se distribuirá de la siguiente manera:
- **30% Anticipo (Hito 0):** A la firma del SOW y aprobación del diseño de arquitectura inicial.
- **40% Avance (Hito 2):** Al liberar la ingesta idempotente en Capa Silver en formato Parquet.
- **30% Cierre (Hito 4):** A la entrega de vistas en Athena, documentación, Runbook operativo y apagado formal de EC2.