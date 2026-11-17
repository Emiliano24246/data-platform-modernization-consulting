# ADR 01: Seleccion de AWS Lambda frente a AWS Glue para la transformacion Bronze a Silver

---

## 1. Contexto y definicion del problema

El pipeline diario de MarketMatrix ingesta velas de Binance para el par BTCUSDT en la capa Bronze y requiere una transformacion hacia la capa Silver consistente en:
1. Validar la estructura y completitud de la vela diaria frente a los 12 campos estandar de la API de Binance.
2. Aislar registros anomalos o incompletos en una ruta de cuarentena.
3. Calcular indicadores tecnicos deterministas con precision de 64 bits (RSI de 14 periodos, Bandas de Bollinger de 20 periodos a 2 desviaciones estandar y MACD 12/26/9).
4. Persistir los datos limpios y enriquecidos en formato columnar Parquet particionado por fecha.

En el diseño preliminar del cliente se considero utilizar AWS Glue ETL con Apache Spark bajo el supuesto de disponer de creditos de AWS o seguir convenciones tipicas de Data Lakes corporativos. Sin embargo, en consultoria de datos cada servicio en la nube debe justificarse cuantitativamente por volumen, tiempo de procesamiento, limites operativos y costo.

---

## 2. Analisis cuantitativo y comparativa tecnica

### 2.1 Volumen de datos por corrida

- **Registros procesados por lote:** 1 vela diaria en operacion regular, o una ventana movil de 90 a 100 registros historicos para inicializar las medias de los indicadores tecnicos.
- **Tamano en crudo (JSON):** Aproximadamente 400 bytes para una vela individual; cerca de 35 KB a 50 KB para la serie completa de 90 dias (< 0.05 MB).
- **Tamano procesado (Parquet):** Entre 8 KB y 15 KB por archivo.
- **Consumo de memoria en tiempo de ejecucion:** Entre 95 MB y 120 MB (incluyendo el runtime de Python 3.12 y las bibliotecas matematicas).

El volumen se ubica en el orden de kilobytes, una escala para la cual un motor de computo distribuido como Apache Spark resulta desproporcionado.

### 2.2 Tiempo de ejecucion y limites de servicio

- **AWS Lambda:**
  - Limite de tiempo de ejecucion: 900 segundos (15 minutos).
  - Tiempo medido de ejecucion: 1.8 a 2.6 segundos con arranque en frio; 250 a 400 milisegundos en ejecuciones posteriores sobre contenedor activo.
  - Uso del limite temporal: Menor al 0.3% de la capacidad disponible.
  - Limite de memoria configurable: Hasta 10,240 MB (10 GB).
  - Memoria configurada: 512 MB. Consumo real: ~115 MB (22.5% de la cuota asignada).

- **AWS Glue (Apache Spark Job):**
  - Aprovisionamiento de nodos (arranque en frio de DPUs): 1 a 3 minutos para inicializar el cluster efimero de Spark (Glue 4.0 con workers G.1X).
  - Tiempo de computo util: Menor a 1 segundo para procesar 35 KB.
  - Sobrecarga: Mas del 98% de la duracion total del trabajo de Glue se consume en la preparacion de infraestructura, no en el procesamiento de datos.

### 2.3 Comparativa de costos (Region us-east-1, sin considerar Capa Gratuita)

| Parametro | AWS Glue (PySpark, 2 DPUs G.1X) | AWS Lambda (512 MB RAM) |
|---|---|---|
| Configuracion minima | Minimo 2 DPUs por trabajo Spark | 512 MB (0.5 GB) |
| Facturacion minima | 1 minuto ($0.44 por DPU-hora) | Incrementos de 1 ms ($0.0000166667 por GB-s) |
| Costo por corrida | 2 DPU * (1/60 h) * $0.44 = $0.01467 USD | 0.5 GB * 2.0 s * $0.0000166667 = $0.0000167 USD |
| Costo mensual (30 corridas) | $0.440 USD | $0.00050 USD |
| Costo anual (365 corridas) | $5.35 USD | $0.0061 USD |

AWS Glue resulta aproximadamente 878 veces mas costoso por corrida que AWS Lambda para este volumen de trabajo. Aunque las cantidades son bajas en terminos absolutos, el costo relativo y la ineficiencia operativa de inicializar un cluster distribuido para procesar menos de 50 KB no justifican su implementacion.

---

## 3. Punto de inflexion: Criterios para migrar a AWS Glue

La arquitectura mantendra AWS Lambda como motor de transformacion mientras la operacion permanezca en los rangos proyectados. Se realizara la transicion formal hacia AWS Glue si se presenta cualquiera de las siguientes condiciones tecnicas:

1. **Crecimiento de volumen mayor o igual a 100x en datos de alta resolucion:**
   Si el fondo cuantitativo extiende el alcance para procesar mas de 500 pares de trading con resolucion de 1 minuto, o si se incorpora la profundidad del libro de ordenes (order book Level 2/3) y registros de operaciones tick a tick, superando los 10 GB a 50 GB diarios.

2. **Aproximacion a los limites de AWS Lambda:**
   Si el tiempo total de calculo sobre series temporales multiactivo supera los 10 minutos (> 65% del limite de timeout de 15 minutos), o si el requerimiento de memoria RAM excede los 8 GB (> 80% del limite maximo de 10 GB).

3. **Necesidad de procesamiento distribuido (Data Shuffling):**
   Si se requieren operaciones de transformacion con agrupaciones masivas, uniones distribuidas entre multiples conjuntos de datos historicos de terabytes o algoritmos de machine learning sobre Spark MLlib que no puedan ejecutarse en la memoria de un solo nodo.

4. **Gobernanza corporativa unificada:**
   Si las politicas de seguridad y linaje del cliente demandan integracion obligatoria con AWS Lake Formation a traves de Glue DynamicFrames para auditoria centralizada.

---

## 4. Decision

Se adopta **AWS Lambda con soporte Parquet** para la transformacion de datos de la capa Bronze a la capa Silver.

Se establecen las siguientes metricas de control en Amazon CloudWatch:
- Alerta preventiva si la duracion de la funcion supera los 30 segundos.
- Alerta preventiva si el consumo de memoria supera los 400 MB (80% del valor asignado).
- Registro de metricas de tamano de archivo para evaluar oportunamente el punto de inflexion.
