# ADR 0001: Seleccion de AWS Lambda frente a AWS Glue y herramientas de streaming para el procesamiento diario de NEOs

---

## 1. Contexto y definicion del problema

La Direccion General de Exposiciones de la red de museos y planetarios requiere actualizar diariamente las pantallas interactivas con datos de asteroides cercanos a la Tierra (NEOs) obtenidos de la API publica NeoWs de la NASA. El payload recibido es un archivo JSON con mas de 300 campos anidados que describen diametros estimados, trayectorias orbitales y fechas de aproximacion.

En evaluaciones tecnicas previas se planteo el uso de trabajos de extraccion masiva en AWS Glue (PySpark). Dado que las exhibiciones de museos se actualizan unicamente una vez al dia (fuera del horario de visitantes) y no requieren procesamiento en tiempo real ni ingesta continua, se evaluo la pertinencia tecnica y financiera de emplear AWS Lambda en lugar de AWS Glue o plataformas orientadas a streaming.

---

## 2. Analisis cuantitativo y comparativa tecnica

### 2.1 Volumen de datos por corrida diaria

- **Objetos por lote diario:** Entre 10 y 35 asteroides detectados en aproximacion cercana cada 24 horas.
- **Tamano del payload crudo (JSON anidado):** Entre 150 KB y 450 KB (< 0.5 MB) por consulta diaria.
- **Tamano historico mensual (30 dias):** Menor a 15 MB en JSON crudo y aproximadamente 2 MB una vez aplanado y comprimido en formato Parquet.
- **Consumo de memoria RAM:** Entre 110 MB y 135 MB durante la deserializacion del arbol JSON, validacion de campos obligatorios y serializacion columnar.

El tamaño de los datos cabe completamente en la memoria principal de un proceso individual, sin requerir particionado distribuido en memoria entre multiples nodos.

### 2.2 Tiempo de ejecucion y limites de servicio

- **AWS Lambda:**
  - Tiempo de ejecucion medido: 1.6 a 2.4 segundos para validar esquema, derivar anomalías a la ruta de cuarentena y persistir en Parquet.
  - Limite de tiempo de Lambda: 900 segundos (15 minutos).
  - Porcentaje utilizado del limite: Menor al 0.27%.
  - Limite de memoria configurado: 512 MB. Consumo real: ~125 MB (24.4% de la capacidad).
- **AWS Glue (Apache Spark Job):**
  - Tiempo de aprovisionamiento de nodos DPU: 1 a 3 minutos antes de comenzar la ejecucion del codigo.
  - Tiempo de computo util: Menos de 1 segundo.
  - Evaluacion: Ineficiencia operativa al tardar 60 veces mas tiempo en inicializar la infraestructura que en procesar el archivo.

### 2.3 Comparativa de costos operativos

| Parametro | AWS Glue (PySpark, 2 DPUs G.1X) | AWS Lambda (512 MB RAM) |
|---|---|---|
| Esquema de facturacion | Minimo 2 DPUs, cobro por minuto ($0.44/DPU-h) | Cobro por milisegundo utilizado |
| Costo por corrida diaria | $0.01467 USD | $0.000018 USD |
| Costo mensual (30 corridas) | $0.440 USD | $0.00054 USD (cubierto por Capa Gratuita) |
| Costo anual (365 corridas) | $5.35 USD | $0.0066 USD |

Frente a una arquitectura de streaming (que exigiria mantener brokers de eventos o conectores con costos base superiores a $20 USD mensuales), la solucion serverless por lotes opera con un costo mensual proyectado de $0.00 USD en la capa gratuita de AWS.

---

## 3. Punto de inflexion: Criterios para migrar a AWS Glue

La arquitectura mantendra AWS Lambda para la ingesta y validacion de esquemas en Silver. Se considerara la transicion formal hacia AWS Glue bajo las siguientes condiciones:

1. **Ingesta del catalogo astronomico completo del sistema solar:**
   Si la red de museos decide incorporar el catalogo completo del JPL Small-Body Database (mas de 1.3 millones de asteroides y cometas registrados con parametros orbitales historicos de decadas), el volumen a procesar superaria los 50 GB a 100 GB, justificando el uso de DataFrames distribuidos en Apache Spark.

2. **Simulaciones de mecanica celeste y propagacion orbital masiva:**
   Si se requiere calcular predicciones de trayectoria a 100 anos mediante integraciones numericas de n-cuerpos en paralelo que excedan los 15 minutos de duracion de Lambda y demanden capacidades computacionales de computo paralelo distribuido.

---

## 4. Decision

Se adopta **AWS Lambda orquestado por Amazon EventBridge** para la ingesta de la API NeoWs, validacion de calidad con aislamiento en ruta de cuarentena y transformacion a Parquet en la capa Silver, descartando el uso de AWS Glue y herramientas de streaming para este alcance.
