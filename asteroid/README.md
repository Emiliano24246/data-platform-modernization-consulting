# Pipeline NEO Risk Monitor - Exhibiciones de Museos y Planetarios

Pipeline serverless por lotes (batch) orientado a eventos para la ingesta diaria de asteroides cercanos a la Tierra (NEOs) desde la API NeoWs de la NASA, validacion de esquemas y cuarentena de datos corruptos, y calculo de metricas de exhibicion (masa estimada, energia cinetica de impacto y Risk Score) mediante Amazon Athena y Amazon S3.

---

## 1. Descripcion del sistema

1. **Capa Bronze (`bronze/`):** Descarga el JSON crudo e intacto de la API NeoWs de la NASA y lo almacena con metadatos de auditoria en `bronze/year=YYYY/month=MM/neo_YYYYMMDD.json`.
2. **Capa Silver (`silver/`):** Activada automaticamente por eventos S3 en `bronze/`. Valida obligatoriedad de campos de identificacion, diametros y cinematicas de aproximacion. Si detecta datos nulos o corruptos, los aisla en `cuarentena/`. Los registros integros se aplanan y persisten en Parquet (`silver/year=YYYY/month=MM/neo_YYYYMMDD.parquet`).
3. **Capa Gold (`gold/`):** Activada automaticamente por eventos S3 en `silver/`. Aplica los modelos fisicos para calcular masa estimada, energia de impacto en Megatones de TNT y Risk Score para las pantallas interactivas de museos.

---

## 2. Diagrama de arquitectura

```
Amazon EventBridge (Programacion diaria 01:00 UTC)
       |
       v
+-------------------------------+
| Lambda: extract_bronze        | <--- API NeoWs de la NASA
+-------------------------------+
       |
       v
Amazon S3: bucket/bronze/year=YYYY/month=MM/neo_YYYYMMDD.json
       |
       | (Evento s3:ObjectCreated en prefix 'bronze/')
       v
+-------------------------------+      [Falla de validacion]
| Lambda: transform_silver      | -----------------------------> S3: bucket/cuarentena/
+-------------------------------+
       |
       v
Amazon S3: bucket/silver/year=YYYY/month=MM/neo_YYYYMMDD.parquet
       |
       | (Evento s3:ObjectCreated en prefix 'silver/')
       v
+-------------------------------+
| Lambda: metrics_gold          |
+-------------------------------+
       |
       v
Amazon Athena / S3: bucket/gold/metrics/ (Tablas de Risk Score para exhibiciones)
```

---

## 3. Estructura del proyecto

```
asteroid/
├── README.md                          # Documentacion tecnica del proyecto
├── infra/
│   └── template.yaml                  # Plantilla CloudFormation completa
├── src/
│    ├── extract_bronze/               
│    │   └── handler.py                # Handler para la ingestion de datos
│    ├── transform_silver/               
│    │   └── handler.py                # Handler para la transformacion
│    └── metrics_gold/                  
│        └── handler.py                # Handler para el calculo de metricas
├── tests/
│    └── test_transform_silver.py      # Test para la transformacion
└── docs/
    ├── rfp.md                         # Requerimiento formal del cliente (RFP)
    ├── sow.md                         # Alcance de trabajo y compromisos (SOW)
    ├── adr/
    │   └── ADR-01-Lambda_vs_Glue.md   # Registro de decision de arquitectura
    ├── ArquitecturaAsteroid.jpg       # Diagrama de arquitectura Asteroid (jpg)
    └── ArquitecturaAsteroid.json      # Diagrama de arquitectura Asteroid (json)
```

---

## 4. Decisiones arquitectonicas (ADR)

Las decisiones tecnicas y justificaciones cuantitativas de diseno para este proyecto se documentan formalmente mediante registros de decision de arquitectura:

- [ADR 01: Seleccion de AWS Lambda frente a AWS Glue y herramientas de streaming para el procesamiento diario de NEOs](docs/adr/ADR-01-Lambda_vs_Glue.md)

---

## 5. Guia paso a paso para el levantamiento en AWS

### Paso 1: Prerrequisitos
1. **Crear el bucket S3 para alojar el codigo:**
   ```bash
   aws s3 mb s3://asteroid-code-<tu-account-id> --region us-east-1
   ```
2. **Obtener el ARN del Layer `AWSSDKPandas-Python312` (x86_64):**
   Para la region `us-east-1`:
   `arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12`
3. **Llave de API de la NASA (Opcional):**
   - Puedes usar la llave publica de prueba `DEMO_KEY`.
   - O crear en Parameter Store el parametro `SecureString` `/asteroid/dev/nasa_api_key` con tu llave personal de api.nasa.gov.

### Paso 2: Empaquetar y subir el codigo de las Lambdas
Desde el directorio `asteroid/src`:
```bash
# Empaquetar cada funcion en la raiz del ZIP
cd src/extract_bronze   && zip -j ../../extract_bronze.zip handler.py
cd ../transform_silver  && zip -j ../../transform_silver.zip handler.py
cd ../metrics_gold      && zip -j ../../metrics_gold.zip handler.py
cd ../..

# Subir los ZIPs a S3
aws s3 cp extract_bronze.zip s3://asteroid-code-<tu-account-id>/lambda/
aws s3 cp transform_silver.zip s3://asteroid-code-<tu-account-id>/lambda/
aws s3 cp metrics_gold.zip s3://asteroid-code-<tu-account-id>/lambda/
```

### Paso 3: Despliegue con AWS CloudFormation

#### Opcion A: Desde la Consola Web de AWS
1. Abrir **CloudFormation** en la consola de AWS en la region `us-east-1`.
2. Hacer clic en **Create stack** -> **With new resources (standard)**.
3. Subir el archivo de plantilla `asteroid/infra/template.yaml`.
4. Especificar los parametros:
   - **Stack name:** `asteroid-risk-pipeline`
   - **Environment:** `dev` (o `prod`)
   - **AlertEmail:** Tu correo electronico para recibir alertas.
   - **CodeBucket:** `asteroid-code-<tu-account-id>`
   - **CodePrefix:** `lambda/`
   - **PandasLayerArn:** `arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12`
   - **NasaApiKey:** Dejar vacio si creaste el parametro en SSM, o colocar `DEMO_KEY`.
   - **BudgetLimitUsd:** `1`
5. Avanzar a la pantalla final, marcar `[x] I acknowledge that AWS CloudFormation might create IAM resources with custom names.` y hacer clic en **Submit**.

#### Opcion B: Mediante AWS CLI
```bash
aws cloudformation deploy \
    --template-file infra/template.yaml \
    --stack-name asteroid-risk-pipeline \
    --parameter-overrides \
        Environment=dev \
        AlertEmail=tu-correo@ejemplo.com \
        CodeBucket=asteroid-code-<tu-account-id> \
        CodePrefix=lambda/ \
        PandasLayerArn=arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12 \
        NasaApiKey=DEMO_KEY \
        BudgetLimitUsd=1 \
    --capabilities CAPABILITY_NAMED_IAM
```

### Paso 4: Confirmacion de suscripciones
Revisa tu correo electronico y confirma las suscripciones enviadas por **AWS Notifications** (SNS y AWS Budgets).

---

## 6. Pruebas y verificacion

### 1. Pruebas unitarias locales
Ejecutar la suite de pruebas unitarias locales:
```bash
python3 -m unittest discover -s asteroid/tests -v
```

### 2. Verificacion en vivo en AWS
1. **Ejecucion manual de ingesta:**
   - En **AWS Lambda**, abrir la funcion `asteroid-extract-bronze-dev` (o el nombre generado).
   - En la pestaña **Test**, crear un evento de prueba:
     ```json
     {
       "target_date": "2026-11-17"
     }
     ```
   - Ejecutar la prueba y verificar que retorne status `200`.
2. **Validacion de datos en S3:**
   - En **S3**, abrir el bucket `asteroid-data-lake-dev-<account-id>`.
   - Verificar la presencia de `bronze/year=YYYY/month=MM/neo_YYYYMMDD.json`.
   - Comprobar que se creo `silver/year=YYYY/month=MM/neo_YYYYMMDD.parquet`.
   - Verificar la publicacion final en `gold/metrics/`.
3. **Consulta de Risk Score en Amazon Athena:**
   - En **Athena**, seleccionar la base de datos `asteroid_db_dev`.
   - Ejecutar la consulta analitica para las pantallas de museos:
     ```sql
     SELECT name, close_approach_date, estimated_diameter_km, relative_velocity_kmh, estimated_kinetic_energy_megatons, risk_score
     FROM asteroid_risk_metrics
     ORDER BY risk_score DESC
     LIMIT 10;
     ```

---

## 7. Desmantelamiento (Teardown)
Para retirar todos los recursos y evitar cualquier cargo posterior:
1. Vaciar el bucket S3 `asteroid-data-lake-dev-<account-id>`.
2. En CloudFormation, seleccionar `asteroid-risk-pipeline` y hacer clic en **Delete**.
3. Eliminar el bucket de codigo `asteroid-code-<tu-account-id>`.
