# ADR 0001: Migracion de instancia EC2 permanente a arquitectura serverless con AWS Lambda y EventBridge


---

## 1. Contexto y definicion del problema

La Direccion de Selecciones Nacionales utilizaba una instancia virtual dedicada de Amazon EC2 encendida continuamente (24 horas al dia, 7 dias a la semana) para ejecutar scripts en Python que consultan la API-Football. Las necesidades operativas del cuerpo tecnico y los analistas requieren actualizaciones por lotes correspondientes a los partidos de cada jornada o torneo, no un procesamiento en tiempo real de baja latencia.

El uso de un servidor encendido permanentemente genera costos fijos acumulados por tiempo de computo ocioso, ademas de requerir administracion del sistema operativo, parchado de seguridad y gestion manual de reintentos ante caidas del proceso.

---

## 2. Analisis cuantitativo y comparativa tecnica

### 2.1 Volumen de datos por corrida

- **Registros por jornada:** Entre 10 y 30 partidos por fecha de competicion.
- **Tamano del payload crudo (JSON):** Entre 50 KB y 200 KB por consulta de jornada (< 0.2 MB). Para una temporada regular completa de 380 partidos, el volumen acumulado es de aproximadamente 2.5 MB a 5 MB.
- **Tamano procesado (Parquet):** Menos de 1 MB por temporada completa particionada por `season`.
- **Consumo de memoria en ejecucion:** ~105 MB a 125 MB en runtime Python 3.12 con soporte para transformaciones tabulares.

### 2.2 Tiempo de ejecucion y limites de servicio

- **AWS Lambda:**
  - Tiempo medio de ejecucion para extraccion y conversion a Parquet: 1.8 a 3.2 segundos.
  - Limite de ejecucion disponible: 900 segundos (15 minutos).
  - Utilizacion del limite temporal: Menor al 0.35%.
  - Memoria configurada: 512 MB. Consumo real: ~115 MB (22.5% de la capacidad asignada).
- **Amazon EC2 (t3.small / t3.micro):**
  - Tiempo activo de computo util: Menos de 5 minutos al mes.
  - Tiempo ocioso facturado: Mas de 719 horas mensuales (99.88% de desperdicio de recursos).

### 2.3 Comparativa de costos operativos (Region us-east-1)

| Parametro | Amazon EC2 (t3.small On-Demand) | AWS Lambda (512 MB) + EventBridge |
|---|---|---|
| Esquema de cobro | Facturacion por hora continua ($0.0208 USD/h) | Facturacion por milisegundo de ejecucion util |
| Horas facturadas al mes | 720 horas continuas | ~0.025 horas (90 segundos acumulados al mes) |
| Costo mensual de computo | $14.98 USD / mes ($179.76 USD / ano) | ~$0.00075 USD / mes, antes de descuentos, para 45 GB-s (512 MB durante 90 s); no incluye otros servicios AWS |
| Costo de administracion | Mantenimiento de SO, parches y reinicios | Cero administracion de servidores subyacentes |

La transicion a AWS Lambda reduce el gasto de computo directo en mas del 99.9% bajo estos supuestos de ejecucion. Esta comparacion estima computo y no incluye el costo total de S3, Glue, Athena, CloudWatch, SNS, Parameter Store ni transferencia de datos; el ahorro mensual total debe validarse con la calculadora de AWS y la factura EC2 que entregue el cliente.

### 2.4 Evaluacion frente a AWS Glue / PySpark

Tambien se descarto el uso de AWS Glue para la transformacion de partidos. El arranque en frio de los nodos Spark toma entre 1 y 3 minutos para procesar un archivo de menos de 1 MB, generando un costo minimo de 2 DPUs ($0.0147 USD por corrida) sin aportar ventajas de escalabilidad horizontal frente a una funcion Lambda liviana.

---

## 3. Punto de inflexion: Criterios para reevaluar la arquitectura

La arquitectura serverless con AWS Lambda cubre plenamente el alcance de datos estadisticos de partidos. Se considerara una migracion hacia contenedores dedicados (Amazon ECS/EKS) o procesamiento distribuido si se presentan las siguientes circunstancias:

1. **Incorporacion de analitica de video y tracking optico de jugadores:**
   Si la federacion contrata fuentes de datos posicionales a 25 cuadros por segundo con coordenadas espaciales $(x, y)$ de 22 jugadores mas el balon, el volumen por partido aumentaria a 15-30 GB de datos continuos, requiriendo ingestion mediante Amazon Kinesis Data Streams y computo continuo en instancias optimizadas para GPU o clusters EMR.

2. **Procesamiento de torneos masivos simultaneos:**
   Si la plataforma se abre para ingerir simultaneamente datos de mas de 1,000 ligas a nivel global con transformaciones cruzadas que excedan el limite de 15 minutos o demanden particionamientos masivos en paralelo.

---

## 4. Decision

Se adopta una **arquitectura serverless orientada a eventos con AWS Lambda y Amazon EventBridge** para la extraccion e ingesta de datos de partidos hacia Amazon S3, eliminando por completo la instancia permanente Amazon EC2.
