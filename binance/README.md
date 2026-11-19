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
│   └── template.yaml                  # Plantilla CloudFormation completa
├── src/
│    ├── extract_bronze/                
│    │   └── handler.py                # Handler para la ingestion de datos
│    ├── transform_silver/                
│    │   └── handler.py                # Handler para la transformacion
│    └── signals_gold/                 
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

## 6. Guia paso a paso para el levantamiento en AWS

### Paso 1: Prerrequisitos
1. Cuenta de AWS activa con credenciales configuradas (Consola o AWS CLI con permisos de IAM, CloudFormation, Lambda, S3, Glue y Athena).
2. Crear un bucket S3 para almacenar el codigo de las Lambdas (por ejemplo, en la region `us-east-1`):
   ```bash
   aws s3 mb s3://binance-code-<tu-account-id> --region us-east-1
   ```
3. Obtener el ARN regional del layer oficial administrado `AWSSDKPandas-Python312` (arquitectura x86_64) para tu region. Para `us-east-1`:
   `arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12`
   *(Verifica la version vigente en la documentacion oficial de AWS SDK for pandas).*

### Paso 2: Empaquetar y subir las funciones Lambda
Desde la carpeta `binance/src`:
```bash
# Empaquetar cada funcion con handler.py en la raiz del ZIP
cd src/extract_bronze  && zip -j ../../extract_bronze.zip handler.py
cd ../transform_silver && zip -j ../../transform_silver.zip handler.py
cd ../signals_gold     && zip -j ../../signals_gold.zip handler.py
cd ../..

# Subir los ZIPs al bucket de codigo
aws s3 cp extract_bronze.zip s3://binance-code-<tu-account-id>/lambda/
aws s3 cp transform_silver.zip s3://binance-code-<tu-account-id>/lambda/
aws s3 cp signals_gold.zip s3://binance-code-<tu-account-id>/lambda/
```

### Paso 3: Despliegue con AWS CloudFormation

#### Opcion A: Desde la Consola Web de AWS (Recomendada)
1. Iniciar sesion en la consola de AWS y dirigirse al servicio **CloudFormation** en la region seleccionada (`us-east-1`).
2. Hacer clic en **Create stack** -> **With new resources (standard)**.
3. En **Specify template**, seleccionar **Upload a template file** y cargar el archivo `binance/infra/template.yaml`.
4. Especificar los parametros del stack:
   - **Stack name:** `marketmatrix-binance-pipeline`
   - **Environment:** `dev` (o `prod`)
   - **AlertEmail:** Tu direccion de correo electronico (donde recibiras alertas de fallas y presupuesto).
   - **CodeBucket:** El nombre del bucket creado en el Paso 1 (`binance-code-<tu-account-id>`).
   - **CodePrefix:** `lambda/`
   - **PandasLayerArn:** El ARN del layer `AWSSDKPandas-Python312` en tu region.
   - **BudgetLimitUsd:** `5` (Limite mensual para la alerta).
5. En la seccion de opciones, avanzar hasta la pantalla de revision y marcar la casilla:
   `[x] I acknowledge that AWS CloudFormation might create IAM resources with custom names.`
6. Hacer clic en **Submit** y esperar a que el estado cambie a `CREATE_COMPLETE` (~2-3 minutos).

#### Opcion B: Mediante AWS CLI
```bash
aws cloudformation deploy \
    --template-file infra/template.yaml \
    --stack-name marketmatrix-binance-pipeline \
    --parameter-overrides \
        Environment=dev \
        AlertEmail=tu-correo@ejemplo.com \
        CodeBucket=binance-code-<tu-account-id> \
        CodePrefix=lambda/ \
        PandasLayerArn=arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12 \
        BudgetLimitUsd=5 \
    --capabilities CAPABILITY_NAMED_IAM
```

### Paso 4: Confirmacion de suscripciones SNS y Budget
Inmediatamente tras el despliegue, revisa tu bandeja de entrada de correo electronico y confirma los enlaces de suscripcion enviados por **AWS Notifications** (tanto para el topico de alarmas SNS como para la alerta de AWS Budgets).

---

## 7. Pruebas y verificacion

### 1. Pruebas unitarias locales (Sin costo de AWS)
Para verificar la logica del pipeline localmente antes de desplegar o realizar cambios:
```bash
python3 -m unittest discover -s binance/tests -v
```
*(Nota: Para ejecutar las pruebas que validan formulas de pandas/pyarrow en tu equipo local, asegurate de tener instalado pandas en tu entorno: `pip install pandas pyarrow`).*

### 2. Verificacion en vivo en AWS
1. **Ejecucion manual de ingesta (Bronze):**
   - En la consola de AWS, ir a **Lambda** -> buscar la funcion `binance-extract-bronze-dev` (o el nombre generado).
   - En la pestaña **Test**, crear un evento de prueba con payload vacio `{}` o especificando una fecha previa:
     ```json
     {
       "target_date": "2026-11-17"
     }
     ```
   - Hacer clic en **Test** y comprobar que retorne codigo `200` y el mensaje de exito con la clave de S3 generada.
2. **Verificacion de la cascada de eventos (Silver y Gold):**
   - Ir a **S3** y abrir el bucket `binance-data-lake-dev-<account-id>`.
   - Verificar que exista el objeto en `bronze/year=YYYY/month=MM/BTCUSDT_YYYYMMDD.json`.
   - Verificar que la creacion en Bronze haya activado automaticamente la transformacion a Parquet en `silver/year=YYYY/month=MM/BTCUSDT_YYYYMMDD.parquet`.
   - Constatar que se haya generado el marcador en `control/gold-ready/` y las senales finales en `gold/signals/`.
3. **Consulta de senales en Amazon Athena:**
   - Ir al servicio **Amazon Athena**.
   - En el editor de consultas, seleccionar el Workgroup `primary` o el configurado, y la base de datos `marketmatrix_db_dev`.
   - Ejecutar la consulta de aceptacion:
     ```sql
     SELECT "date", symbol, close, rsi_14, bollinger_upper, bollinger_lower, macd_histogram, signal_action
     FROM binance_gold_signals
     ORDER BY "date" DESC
     LIMIT 10;
     ```
   - Comprobar que responde en menos de 2 segundos escaneando menos de 100 KB.

### 3. Reprocesamiento historico (Backfill)
Para regenerar todo el historico disponible en Bronze y reconstruir los indicadores:
- Invocar manualmente `transform_silver` con el payload:
  ```json
  {
    "mode": "backfill"
  }
  ```
- El proceso lee ordenadamente las velas de Bronze, sobreescribe las particiones Silver correspondientes y publica el marcador para actualizar la tabla Gold en Athena.

---

## 8. Desmantelamiento (Teardown)
Si deseas apagar completamente el proyecto y asegurarte de que no quede ningun recurso:
1. Ir a **S3** y vaciar el bucket `binance-data-lake-dev-<account-id>` (CloudFormation no permite borrar buckets con objetos).
2. Ir a **CloudFormation** -> Seleccionar `marketmatrix-binance-pipeline` -> Hacer clic en **Delete**.
3. Eliminar el bucket de codigo `binance-code-<tu-account-id>` si ya no lo necesitas.
