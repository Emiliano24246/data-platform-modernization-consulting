# SOW: Binance-Data-Engineering-Project

**Consultora:** TAD Data Consulting
**Cliente:** Asteroid DataEngineering Project (red de museos y planetarios), Dirección General de Exposiciones
**Respuesta al RFP del:** 13 de octubre de 2026
**Versión:** 2.0
**Equipo:** 
- Tristan Qesen Sanchez Mayen - Lead Data Engineer
- Darinka Sandoval Hernandez - Data Engineer
- Roman Emiliano Salgado Cruz - Data Engineer
- Angel Hazel Tapia Ledesma - Data Engineer
- Yessica Lizeth Caballero Garcia - Data Engineer

---

## 1. Resumen ejecutivo
Entendimos que procesan a mano un JSON complejo de la API de la NASA y sus pantallas muestran datos desactualizados. Entregaremos un pipeline serverless e idempotente en AWS que extrae diariamente los datos de asteroides, los valida mandando errores a cuarentena, calcula el Risk Score y la energía de impacto, y deja tablas consultables en Athena. La solución incluye plantillas en CloudFormation, alertas en CloudWatch/SNS, AWS Budget y Runbook de operación. Se ejecutará en 10 semanas por un monto total de $110,000 MXN.

---

## 2. Diagnóstico
A partir de la revisión técnica del entorno actual, identificamos los siguientes hallazgos:

1. **Procesamiento manual e ineficiente:** Los datos de la API de la NASA (JSON con más de 300 campos) se procesan manualmente. **Riesgo:** Consumo excesivo de tiempo de personal especializado y pantallas de exhibición desactualizadas. **Solución:** Automatizar la ingesta diaria desatendida mediante AWS Lambda y Amazon EventBridge (Entregables E1 y E2).
2. **Falta de conservación del dato crudo:** No existe un registro histórico confiable del payload original de la API de la NASA. **Riesgo:** Imposibilidad de re-procesar los datos ante cambios en el modelo o auditorías. **Solución:** Almacenar el JSON crudo e intacto en el bucket S3 Bronze (Entregable E1).
3. **Ausencia de controles de calidad de datos y cuarentena:** Los registros inválidos o con esquemas alterados no son filtrados automáticamente. **Riesgo:** Contaminación de las tablas de métricas con valores nulos o erróneos que afectan las pantallas. **Solución:** Implementar reglas de validación en AWS Lambda que desvían registros no válidos a una ruta de cuarentena en S3 (Entregables E2 y E3).
4. **Propuesta previa con herramientas de streaming y Glue (PySpark):** Se contemplaba un Schema Registry pensado para streaming y Jobs de Glue para procesar volumen pequeño. **Riesgo:** Costos innecesarios en servicios fuera de la capa gratuita de AWS y complejidad técnica no justificada. **Solución:** Refactorización a Python nativo en AWS Lambda para limpieza, validación de esquema y conversión a Parquet en la capa Silver (Entregables E2 y E3).
5. **Opacidad en la supervisión operativa y de costos:** No existen notificaciones en caso de fallo ni alertas presupuestales activas. **Riesgo:** Interrupción no detectada de las pantallas de los museos y sobrecostos en AWS. **Solución:** Alarmas en CloudWatch integradas con SNS para alertas por correo y configuración de AWS Budget (Entregables E1 y E5).

---

## 3. Alcance
**Dentro de alcance:**
- Ingesta diaria automatizada desatendida de la API NeoWs de la NASA mediante AWS Lambda y EventBridge.
- Arquitectura de almacenamiento en Amazon S3 en tres capas: Bronze (JSON crudo), Silver (Parquet limpio y particionado) y Gold (métricas de exhibición).
- Reglas de calidad de datos, validación de esquema y ruta de aislamiento en cuarentena en S3 para registros no válidos.
- Aplicación de las fórmulas de Risk Score y energía de impacto en la capa Gold para su consulta en Amazon Athena.
- Infraestructura como Código (IaC) desplegada exclusivamente mediante AWS CloudFormation.
- Monitoreo con alarmas de CloudWatch, notificaciones SNS, alerta de costo con AWS Budget y Runbook de operación.

**Fuera de alcance:**
- Desarrollo de la interfaz gráfica, software o tableros para las pantallas del museo o planetario (razón: el alcance se limita estrictamente al procesamiento de datos en backend).
- Invención, diseño o validación del modelo físico/matemático de Risk Score y energía de impacto (razón: las fórmulas son provistas directamente por el cliente).
- Herramientas, bases de datos relacionales (RDS) o servicios fuera del ecosistema de AWS (razón: restricción explícita en el RFP).
- Uso de Jobs de AWS Glue con PySpark o herramientas de streaming como Schema Registry (razón: el procesamiento es diario por lotes y se resuelve en la capa gratuita con Lambda).

---

## 4. Entregables y criterios de aceptación
| # | Entregable | Criterio de aceptación | Hito |
|---|---|---|---|
| **E1** | Ingesta Bronze y Alertas | (a) Cada día el JSON de la NASA se descarga y almacena crudo en S3 Bronze mediante EventBridge y Lambda durante 7 días seguidos sin intervención. (b) Reejecutar la carga el mismo día no duplica datos. (c) Una falla en la ingesta emite un aviso por correo vía SNS. | 1 |
| **E2** | Capa Silver y Cuarentena de Calidad | (a) Transformación del JSON crudo a Parquet en S3 Silver. (b) Registros con nulos en campos obligatorios o cuyo esquema no coincida con el esperado son rechazados, enviados a una ruta de cuarentena en S3 y registrados. | 2 |
| **E3** | Capa Gold y Métricas en Athena | (a) Tablas en Athena con las métricas de Risk Score y energía de impacto calculadas aplicando las fórmulas entregadas por el cliente. (b) La consulta de aceptación sobre 30 días de datos termina en menos de 10 segundos. | 3 |
| **E4** | Infraestructura como Código y AWS Budget | (a) Plantillas en CloudFormation que despliegan toda la infraestructura desde cero. (b) Alerta activa de AWS Budget configurada y probada. (c) Permisos IAM configurados bajo el principio de mínimo privilegio. | 4 |
| **E5** | Documentación y Runbook | Guía de operación (Runbook) entregada; el equipo técnico de los museos resuelve un escenario de falla simulado utilizando únicamente la documentación entregada]. | 4 |

---

## 5. Trazabilidad con el RFP
| Necesidad del RFP | Entregable(s) que la cubre(n) | Estado |
|---|---|---|
| **N1:** Actualización diaria sin intervención humana (7 días sin duplicar) | E1, E2 | Cubierta |
| **N2:** Datos confiables (validación de esquema y cuarentena) | E2 | Cubierta |
| **N3:** Métricas listas para exhibiciones (Risk Score, Athena < 10 s) | E3 | Cubierta |
| **N4:** Aviso cuando algo falla (alertas) | E1 | Cubierta |

---

## 6. Enfoque técnico (alto nivel)
Desplegaremos una arquitectura serverless por lotes mediante plantillas de AWS CloudFormation. Amazon EventBridge activará una función AWS Lambda diariamente que consumirá la API NeoWs de la NASA y depositará el payload en `s3://asteroid-data-lake-bronze/` en su formato JSON original. Una segunda función Lambda evaluará la calidad de los datos y la conformidad del esquema: los registros no válidos se enviarán a `s3://asteroid-data-lake-cuarentena/`, mientras que los datos limpios se transformarán a Parquet en `s3://asteroid-data-lake-silver/`. En la capa Gold, Athena consultará los datos para aplicar las fórmulas del Risk Score y energía de impacto provistas por el cliente. Las fallas de ingesta y umbrales de costos dispararán alarmas en CloudWatch notificando por correo a través de Amazon SNS.

---

## 7. Plan por hitos
| Hito | Qué se entrega | Evidencia | Fecha | Horas |
|---|---|---|---|---|
| **0 Arranque** | Diagnóstico técnico y diseño en CloudFormation. | Repositorio base con CloudFormation, matriz IAM y diagnóstico firmado. | Sem 2 | 40 |
| **1 Ingesta** | E1: Capa Bronze e ingesta serverless. | Lambda guardando JSONs en S3 Bronze vía EventBridge y alerta por SNS probada. | Sem 4 | 60 |
| **2 Calidad** | E2: Capa Silver y ruta de cuarentena. | Archivos Parquet en S3 Silver, pruebas de idempotencia y registros no válidos derivados a cuarentena. | Sem 6 | 80 |
| **3 Métricas** | E3: Capa Gold y consultas en Athena. | Vistas de Risk Score en Athena ejecutando en < 10 s sobre dataset de 30 días. | Sem 8 | 70 |
| **4 Cierre** | E4/E5: IaC final, AWS Budget y Runbook. | Despliegue automatizado limpio, alerta de presupuesto activa y prueba del Runbook con el cliente. | Sem 10 | 50 |
| **Total** | | | | **300 h** |

---

## 8. Equipo y responsabilidades
- **Tristan Qesen Sanchez Mayen (Lead Data Engineer):** Arquitectura serverless, diseño de plantillas CloudFormation, modelo de seguridad IAM y coordinación del proyecto. Reporta a la Dirección General de Exposiciones. 
- **Darinka Sandoval Hernandez (Data Engineer):** Desarrollo de las reglas de calidad de datos, validación de esquemas JSON y canalización de registros a la ruta de cuarentena en S3. Reporta a Tristan.
- **Roman Emiliano Salgado Cruz (Data Engineer):** Transformación de datos a formato Parquet en Capa Silver, pruebas de idempotencia y reejecución sin duplicados. Reporta a Tristan.
- **Angel Hazel Tapia Ledesma (Data Engineer):** Modelado de las tablas/vistas en Athena (Gold), implementación de las fórmulas de Risk Score y pruebas de rendimiento de consultas. Reporta a Tristan.
- **Yessica Lizeth Caballero Garcia (Data Engineer):** Implementación de la ingesta desatendida en Lambda, orquestación con EventBridge, configuración de monitoreo CloudWatch/SNS y elaboración del Runbook. Reporta a Tristan.

---

## 9. Supuestos y dependencias del cliente
- El cliente proporcionará la llave API válida para la API NeoWs de la NASA y entregará la fórmula exacta del Risk Score y energía de impacto ya validada por su equipo técnico al cierre del Hito 0 (Semana 2).
- El equipo de tecnología de los museos estará disponible en el Hito 3 para validar las consultas de las pantallas hacia Amazon Athena.
- El volumen de datos astronómicos procesados se mantiene en rangos que permiten operar bajo la capa gratuita y servicios básicos de AWS.

---

## 10. Riesgos
| Riesgo | Probabilidad | Impacto | Qué haremos |
|---|---|---|---|
| Cambios imprevisibles en la estructura del JSON devuelto por la API de la NASA. | Media | Alto | La función Lambda de calidad validará los campos obligatorios antes de transformar a Silver, desviando registros fuera de esquema a la ruta de cuarentena y emitiendo una alerta. |
| Retrasos en la entrega de las fórmulas del Risk Score por parte del cliente. | Media | Medio | Solicitar las fórmulas validadas en el Hito 0 e implementar la capa Gold de forma modular mediante Vistas SQL en Athena. |
| Interrupciones o caídas temporales de disponibilidad en la API pública de la NASA. | Media | Medio | Implementar reintentos exponenciales en AWS Lambda y notificaciones vía SNS si la ingesta falla tras múltiples intentos. |

---

## 11. Presupuesto
| Concepto | Horas | Tarifa MXN/h | Subtotal MXN |
|---|---|---|---|
| Lead Data Engineer (Tristan) | 80 | $350.00 | $28,000 |
| Data Engineer (Darinka) | 55 | $280.00 | $15,400 |
| Data Engineer (Emiliano) | 55 | $280.00 | $15,400 |
| Data Engineer (Angel Hazel) | 55 | $280.00 | $15,400 |
| Data Engineer (Yessica Lizeth) | 55 | $280.00 | $15,400 |
| Colchón de imprevistos (10%)
| Costo estimado de AWS (Proyección 10 semanas)
| **Total**| **300** | | |



---

## 12. Control de cambios y comunicación
Cualquier solicitud de modificación en el alcance acordado o la inclusión de nuevas métricas se gestionará mediante un formato de Solicitud de Cambio (CR), evaluando previamente su impacto en costo y tiempo antes de su aprobación por parte del cliente. Se mantendrán reuniones semanales de seguimiento y revisiones técnicas al cierre de cada hito.

---

## 13. Aceptación y pagos
Un hito se dará por concluido una vez que cumpla con los criterios de aceptación y el representante del cliente firme la hoja de validación correspondiente. 

El esquema de pagos se liberará de la siguiente manera:
- **20%** a la aceptación del Hito 0 (Inicio y arquitectura en CloudFormation).
- **30%** a la aceptación del Hito 2 (Ingesta idempotente y calidad con cuarentena en Silver).
- **30%** a la aceptación del Hito 3 (Capa Gold y consultas en Athena validadas).
- **20%** a la entrega final en el Hito 4 (IaC final, AWS Budget, Runbook y cierre).