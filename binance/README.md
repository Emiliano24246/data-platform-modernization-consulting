# Pipeline de Senales de Mercado Binance (MarketMatrix)

Pipeline serverless por lotes (batch) orientado a eventos para la extraccion diaria de velas (klines) de Bitcoin (BTCUSDT) desde la API de Binance, validacion de completitud de datos, calculo de indicadores tecnicos (RSI, Bandas de Bollinger y MACD) y generacion de senales de trading (COMPRA FUERTE, VENTA, MANTENER) mediante Amazon Athena y arquitectura medallon en Amazon S3.

---

## 1. Descripcion del sistema

El pipeline automatiza el procesamiento diario de datos de mercado para la mesa de control y los algoritmos cuantitativos de MarketMatrix:

1. **Capa Bronze (`bronze/`):** Almacena el payload crudo e intacto de la vela diaria de Binance junto con metadatos de auditoria (marca de tiempo de ingesta, endpoint de origen y simbolo). La operacion es idempotente: multiples ejecuciones para el mismo dia sobreescriben la particion determinista correspondiente sin duplicar registros.
2. **Capa Silver (`silver/`):** Detonada automaticamente por notificaciones de Amazon S3 (`s3:ObjectCreated:*`) sobre el prefijo `bronze/`. Valida la estructura y consistencia numerica de la vela. Si el dato es invalido o incompleto, se aisla en la carpeta `cuarentena/` y se aborta el flujo. Si el dato es integro, calcula con precision de 64 bits los indicadores tecnicos (RSI 14, Bollinger 20 con 2 desviaciones estandar y MACD 12/26/9) y lo persiste en formato columnar Parquet.
3. **Capa Gold (`gold/`):** Detonada automaticamente por notificaciones de Amazon S3 sobre el prefijo `silver/`. Aplica la estrategia de triple confirmacion y ejecuta una consulta CTAS (Create Table As Select) en Amazon Athena para materializar las senales listas para consulta SQL por parte de los sistemas de trading.

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
       | (Pone objeto con fecha de ingesta)
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
       | (Evento s3:ObjectCreated en prefix 'silver/')
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
- La notificacion S3 para `signals_gold` escucha exclusivamente el prefijo `silver/` y escribe en `gold/` y `athena_query_results/`.
- El prefijo `gold/` no tiene ningun listener ni notificacion configurada, cerrando el ciclo de eventos de forma segura.

---

## 3. Estructura del proyecto

```
binance/
├── README.md                          # Documentacion tecnica del proyecto
├── infra/
│   └── template.yaml                  # Plantilla CloudFormation base
├── src/
├── tests/
└── docs/
    ├── rfp.md                         # Requerimiento formal del cliente (RFP)
    ├── sow.md                         # Alcance de trabajo y compromisos (SOW)
    ├── adr/
    │   └── ADR-01-Lambda_vs_Glue.md  # Registro de decision de arquitectura
    ├── ArquitecturaBinance.jpg       # Diagrama de arquitectura Binance (jpg)
    └── ArquitecturaBinance.json      # Diagrama de arquitectura Binance (json)
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

1. **Revision de la plantilla de infraestructura:** Verificar los recursos declarados en `infra/template.yaml` (bucket S3 por capas, roles IAM de minimo privilegio, funciones Lambda, catalogo en Glue y regla de EventBridge).
2. **Empaquetado de artefactos:** Generar los paquetes de codigo para cada funcion Lambda ubicadas en el directorio `src/`.
3. **Creacion del stack en CloudFormation:** Ejecutar el aprovisionamiento del stack mediante la consola de AWS o AWS CLI proporcionando los parametros base (entorno, correo para notificaciones de SNS y simbolo de mercado).
4. **Validacion de notificaciones y permisos:** Confirmar la suscripcion del correo electronico en Amazon SNS y verificar que las funciones Lambda tengan permisos de lectura y escritura restringidos a sus respectivos prefijos en S3.
5. **Comprobacion inicial:** Ejecutar una corrida manual de la funcion de extraccion para validar la creacion del objeto en Bronze y constatar la ejecucion en cadena hacia Silver y Gold.

---

## 7. Ejecucion de pruebas y verificacion de resultados

El repositorio cuenta con una suite de pruebas unitarias y de integracion construidas sobre la biblioteca estandar `unittest` de Python.

### Ejecutar todas las pruebas

Desde la raiz del repositorio:

```bash
python3 -m unittest discover binance/tests -v
```

### Prueba de dos ejecuciones (Entregable E2 - Idempotencia)

Para ejecutar de manera especifica la prueba que verifica que dos ejecuciones consecutivas para el mismo dia producen el mismo resultado sin duplicados:

```bash
python3 -m unittest binance/tests/test_idempotency.py -v
```
