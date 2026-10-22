# SOW: NEO Risk Monitor

**Consultora:** TAD Data Consulting
**Cliente:** Asteroid DataEngineering Project
**Respuesta al RFP del:** 13/10/2026
**Versión:** 1.0
**Equipo:** 
- Tristan Qesen Sanchez Mayen
- Sandoval Hernandez Darinka
- Salgado Cruz Roman Emiliano
- Tapia Ledesma Angel Hazel

## 1. Resumen ejecutivo
Entendemos que requieren modernizar el procesamiento de datos astronómicos de asteroides provenientes de la API de la NASA para alimentar las pantallas de sus museos de forma automática. Entregaremos un pipeline serverless en AWS (Lambda, Glue, Athena) que procesará la información en formato Parquet y calculará el "Risk Score". El proyecto se ejecutará en 10 semanas con un costo total estimado de $426,250 MXN, manteniéndonos por debajo de su presupuesto máximo.

## 2. Alcance
**Dentro de alcance:**
- Extracción automatizada diaria de la API NeoWs de la NASA.
- Construcción de arquitectura Medallón (Bronze, Silver, Gold) en Amazon S3.
- Implementación de reglas de calidad de datos y cuarentena en S3.
- Modelado en Amazon Athena para consulta de métricas de riesgo.
**Fuera de alcance:**
- Desarrollo de la interfaz gráfica de las pantallas del museo (razón: nuestro enfoque es puramente ingeniería de datos en backend).
- Uso de servidores EC2 (razón: buscamos eliminar costos por inactividad).

## 3. Entregables y criterios de aceptación
| # | Entregable | Criterio de aceptación | Hito |
|---|---|---|---|
| E1 | Ingesta Bronze en AWS Lambda | Los JSON de la NASA se guardan crudos en S3 diariamente sin fallos. | 2 |
| E2 | Transformación Silver (Glue) | Datos limpios y convertidos a Parquet; registros nulos enviados a ruta de cuarentena. | 3 |
| E3 | Capa Gold (Athena) | Tablas materializadas permiten consultar el "Risk Score" y energía de impacto en menos de 3 segundos. | 4 |

## 4. Trazabilidad con el RFP
| Necesidad del RFP | Entregable(s) que la cubre(n) | Estado |
|---|---|---|
| N1 Pipeline automatizado en AWS | E1, E2 | Cubierta |
| N2 Calidad de Datos (Data Quality) | E2 | Cubierta |
| N3 Generación de métricas para exhibición | E3 | Cubierta |

## 5. Enfoque técnico (alto nivel)
Se utilizará una arquitectura orientada a eventos. AWS EventBridge disparará diariamente funciones AWS Lambda para consumir la API de la NASA. Los datos crudos (JSON) aterrizarán en el bucket S3 (Bronze). Esto detonará un Job de AWS Glue (PySpark) que limpiará los datos, validará nulos y particionará en formato Parquet en la capa Silver. Finalmente, AWS Athena leerá estos datos procesados para generar las vistas analíticas (Gold). Esta decisión garantiza escalabilidad y un modelo de pago por uso (Serverless).

## 6. Plan por hitos
| Hito | Qué se entrega | Evidencia | Fecha | Horas |
|---|---|---|---|---|
| 0 Arranque | Kick-off y repositorios | Repositorio GitHub con IaC inicial | Sem 1 | 40 |
| 1 Diseño | Arquitectura y ADRs | Diagramas de red y permisos IAM | Sem 2 | 80 |
| 2 Ingesta | Capa Bronze Serverless | Logs de ejecución exitosa en CloudWatch | Sem 4 | 160 |
| 3 ETL | Jobs de Glue (Silver) | Archivos Parquet particionados en S3 | Sem 7 | 240 |
| 4 Analítica | Vistas Athena (Gold) | Consultas SQL de prueba con latencia < 3s | Sem 9 | 160 |
| 5 Cierre | Documentación y entrega | Manual técnico entregado y firmado | Sem 10 | 120 |

## 7. Equipo y responsabilidades
- **Tristan Qesen Sanchez Mayen:** Lead Data Engineer. Responsable del diseño arquitectónico en AWS, estandarización y configuración de IAM. Reporta a la Dirección del Museo.
- **Sandoval Hernandez Darinka:** Data Engineer. Responsable de los scripts de transformación en AWS Glue (PySpark) y calidad de datos. Reporta al Lead Data Engineer.
- **Salgado Cruz Roman Emiliano:** Data Engineer. Responsable del desarrollo de las funciones Lambda y consultas en Athena. Reporta al Lead Data Engineer.
- **Tapia Ledesma Angel Hazel:** Data Engineer. Responsable del monitoreo del pipeline, pruebas y validación de las vistas analíticas. Reporta al Lead Data Engineer.

## 8. Supuestos y dependencias del cliente
- El cliente proporcionará la llave API válida de la API NeoWs de la NASA desde el día 1.
- El equipo técnico del cliente (desarrolladores de las pantallas) estará disponible en el Hito 4 para probar la conexión con Amazon Athena.
- Asumimos que los esquemas del JSON de la NASA no sufrirán cambios estructurales (breaking changes) durante estas 10 semanas.

## 9. Riesgos
| Riesgo | Probabilidad | Impacto | Qué haremos |
|---|---|---|---|
| La API de la NASA cambia la estructura del JSON | Media | Alto | Implementar validadores de esquema (schema registry) en el Hito 2 para alertar y desviar a cuarentena sin detener el pipeline. |
| Cuellos de botella en la aprobación de accesos IAM | Alta | Medio | Solicitar la creación de roles con políticas predefinidas desde el Hito 0. |

## 10. Presupuesto
| Concepto | Horas | Tarifa MXN/h | Subtotal MXN |
|---|---|---|---|
| Lead Data Engineer (Tristan) | 400 | $406.25 | $162,500.00 |
| Data Engineer (Darinka) | 266 | $281.25 | $74,812.50 |
| Data Engineer (Emiliano) | 267 | $281.25 | $75,093.75 |
| Data Engineer (Angel) | 267 | $281.25 | $75,093.75 |
| Colchón de imprevistos (5%) | | | $19,375.00 |
| Costo estimado de AWS (3 meses) | | | $19,375.00 |
| **Total** | **1200** | | **$426,250.00** |

*El presupuesto total de $426,250 MXN se mantiene por debajo del límite máximo de $450,000 MXN establecido en el RFP.*

## 11. Control de cambios y comunicación
Cualquier solicitud fuera del alcance será documentada en un formato de Change Request (CR) y requerirá la firma del Lead Data Engineer (Tristan) y el contacto del Cliente. Tendremos reuniones de revisión de avance (Dailies de 15 min internos y una revisión de Sprint con el cliente cada viernes).

## 12. Aceptación y pagos
Un hito se considera terminado cuando se cumple el criterio de aceptación y el cliente firma la hoja de validación. Los pagos se liberarán de la siguiente forma: 20% al Hito 0, 30% al Hito 2, 30% al Hito 4, y 20% contra entrega final en el Hito 5.