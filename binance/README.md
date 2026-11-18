# Pipeline de Senales de Mercado Binance (MarketMatrix)

Pipeline serverless por lotes orientado a eventos para extraer velas diarias BTCUSDT de la API publica de Binance, validarlas, calcular indicadores tecnicos y publicar senales de trading en Athena. Usa un solo bucket S3 con prefijos Bronze, Silver, cuarentena, Gold y resultados de Athena.

---

## 1. Descripcion del sistema

El pipeline automatiza el procesamiento diario de datos de mercado para la mesa de control y los algoritmos cuantitativos de MarketMatrix:

1. **Capa Bronze (`bronze/`):** Almacena el payload crudo e intacto de la vela diaria de Binance junto con metadatos de auditoria (marca de tiempo de ingesta, endpoint de origen y simbolo). La operacion es idempotente: multiples ejecuciones para el mismo dia sobreescriben la particion determinista correspondiente sin duplicar registros.
2. **Capa Silver (`silver/`):** Detonada automaticamente por notificaciones de Amazon S3 (`s3:ObjectCreated:*`) sobre el prefijo `bronze/`. Valida la estructura y consistencia numerica de la vela. Si el dato es invalido o incompleto, se aisla en la carpeta `cuarentena/` y se aborta el flujo. Si el dato es integro, calcula con precision de 64 bits los indicadores tecnicos (RSI 14, Bollinger 20 con 2 desviaciones estandar y MACD 12/26/9) y lo persiste en formato columnar Parquet.
3. **Capa Gold (`gold/`):** Silver publica un marcador en `control/gold-ready/` al completar una transformacion o un backfill. Ese marcador activa Gold, que ejecuta una consulta CTAS en Athena y materializa las senales.

### Convenciones de indicadores

Hasta recibir y comparar la linea base del cliente, estas formulas son supuestos explicitos, no una verificacion de la tolerancia `0.000001` del SOW:

- RSI(14): promedio simple de los primeros 14 cambios y suavizado Wilder posterior; primer valor en la fila 15 de precios.
- Bollinger(20, 2): desviacion estandar poblacional (`ddof=0`).
- MACD(12, 26, 9): EMA iniciadas con SMA de cada periodo; la linea de senal tambien se inicia con SMA.

La linea base de 90 dias del cliente debe confirmar estos metodos y la diferencia numerica antes de afirmar que E3 cumple.

---

## 2. Diagrama de arquitectura y flujo de eventos

```
Amazon EventBridge (Programacion diaria 00:05 UTC)
       |
       v
+-------------------------------+
| Lambda: extract_bronze        | <--- API publica de Binance (BTCUSDT)
+-------------------------------+
       |
       | (Pone la vela cerrada de ayer UTC)
       v
Amazon S3: bucket/bronze/year=YYYY/month=MM/BTCUSDT_YYYYMMDD.json
       |
       | (Evento s3:ObjectCreated en prefix 'bronze/')
       v
+-------------------------------+      [Falla de validacion]
| Lambda: transform_silver      | -----------------------------> S3: bucket/cuarentena/
+-------------------------------+                                 (Alarma CloudWatch/SNS)
       |
       | (Calculo de indicadores RSI, Bollinger, MACD)
       v
Amazon S3: bucket/silver/year=YYYY/month=MM/BTCUSDT_YYYYMMDD.parquet
       |
       | (Marcador `control/gold-ready/` tras completar Silver)
       v
+-------------------------------+
| Lambda: signals_gold          |
+-------------------------------+
       |
       | (Ejecuta consulta CTAS con triple confirmacion)
       v
Amazon Athena / S3: bucket/gold/signals/ (Tabla SQL lista para la mesa de control)
```

### Prevencion de bucles infinitos (Non-recursive Event Routing)
Para evitar que una funcion Lambda se dispare a si misma:
- La regla de EventBridge dispara unicamente a `extract_bronze`.
- La notificacion S3 para `transform_silver` escucha exclusivamente el prefijo `bronze/` y escribe en `silver/` y `cuarentena/`.
- La notificacion S3 para `signals_gold` escucha exclusivamente `control/gold-ready/` y escribe en `gold/` y `athena-results/`.
- El prefijo `gold/` no tiene ningun listener ni notificacion configurada, cerrando el ciclo de eventos de forma segura.

---

## 3. Estructura del proyecto

```
binance/
├── README.md                          # Documentacion tecnica del proyecto
├── infra/
│   └── template.yaml                  # Plantilla CloudFormation base
├── src/
│    ├── extract_bronze/                
│    │   └── handler.py                # Handler para la ingestion de datos
│    ├── extract_silver/                
│    │   └── handler.py                # Handler para la transformacion
│    └── extract_gold/                 
│        └── handler.py                # Handler para la obtencion de señales
├── tests/
│    └── test_pipeline.py              # Tests para el pipeline
└── docs/
    ├── rfp.md                         # Requerimiento formal del cliente (RFP)
    ├── sow.md                         # Alcance de trabajo y compromisos (SOW)
    ├── adr/
    │   └── ADR-01-Lambda_vs_Glue.md   # Registro de decision de arquitectura
    ├── ArquitecturaBinance.jpg        # Vista previa visual anterior
    └── ArquitecturaBinance.json       # Fuente actual del diagrama
```

---

## 4. Estrategia de triple confirmacion en Gold

La clasificacion de senales de mercado se evalua de acuerdo a la siguiente matriz de decision:

| Senal | Condicion RSI (14) | Condicion Bollinger (20, 2) | Condicion MACD (12, 26, 9) |
|---|---|---|---|
| **COMPRA FUERTE** | RSI < 30.0 (Sobrevendido) | Precio de cierre <= Banda Inferior | MACD > Linea de Senal (Cruce alcista) |
| **VENTA** | RSI > 70.0 (Sobrecomprado) | Precio de cierre >= Banda Superior | MACD < Linea de Senal (Cruce bajista) |
| **MANTENER** | Cualquier otra combinacion que no cumpla simultaneamente los tres criterios |

---

## 5. Decisiones arquitectonicas (ADR)

Las decisiones tecnicas y justificaciones cuantitativas de diseno para este proyecto se documentan formalmente mediante registros de decision de arquitectura:

- [ADR 0001: Seleccion de AWS Lambda frente a AWS Glue para la transformacion Bronze a Silver](docs/adr/ADR-01-Lambda_vs_Glue.md)

---

## 6. Boceto de despliegue en AWS (Referencia preliminar)

El despliegue de la infraestructura esta proyectado para realizarse a traves de AWS CloudFormation. El siguiente flujo resume los pasos generales para el aprovisionamiento, sujeto a ajustes conforme evolucione el desarrollo:

1. **Empaquetado de artefactos:** Crear `extract_bronze.zip`, `transform_silver.zip` y `signals_gold.zip`, cada uno con `handler.py` en la raiz, y subirlos al prefijo indicado por `CodeBucket`/`CodePrefix`.
2. **Layer de Silver:** Obtener el ARN vigente del layer `AWSSDKPandas-Python312` para la region y arquitectura de despliegue. Incluye pandas y pyarrow.
3. **Creacion del stack:** Desplegar `infra/template.yaml` proporcionando bucket y prefijo de codigo, ARN del layer, entorno, correo de alertas y limite del AWS Budget.
4. **Validacion de notificaciones y permisos:** Confirmar la suscripcion SNS y comprobar los permisos de cada Lambda. Para limitar Gold a una ejecucion, desplegar con `GoldReservedConcurrency=1` si la cuenta AWS permite reservar concurrencia.
5. **Comprobacion inicial:** Ejecutar Extract para generar el objeto Bronze y comprobar el marcador Gold y el resultado en Athena.

### Reprocesamiento historico

Invoca `transform_silver` manualmente con `{"mode": "backfill"}`. El proceso lee y ordena los JSON de Bronze del simbolo configurado, recalcula todos los indicadores, sobrescribe los Parquet Silver y publica un unico marcador para Gold. Deben existir las velas diarias consecutivas; si falta una, el proceso falla para evitar indicadores calculados sobre una serie incompleta. No ejecutes varias cargas historicas en paralelo.

---

## 7. Ejecucion de pruebas y verificacion de resultados

El repositorio cuenta con una suite de pruebas unitarias construidas sobre la biblioteca estandar `unittest` de Python.

### Ejecutar todas las pruebas

Desde la raiz del repositorio:

```bash
python -m unittest discover binance/tests -v
```

Las pruebas enfocadas de extraccion, validacion, indicadores, backfill y errores:

```bash
python -m unittest binance.tests.test_pipeline -v
```

Las pruebas simulan las llamadas a AWS. El despliegue requiere un layer compatible de pandas y pyarrow y no se realiza durante las pruebas locales.
