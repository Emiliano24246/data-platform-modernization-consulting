# Pipeline de Analitica Tactica de Futbol (Selecciones Nacionales)

Pipeline serverless por lotes (batch) orientado a eventos para la ingesta de partidos y estadisticas desde API-Football, transformacion a Parquet particionado por temporada y generacion de KPIs tacticos (efectividad, victorias, derrotas, promedio de goles) para el cuerpo tecnico mediante Amazon Athena y Amazon S3.

---

## 1. Descripcion del sistema

1. **Capa Bronze (`bronze/`):** A las 12:00 UTC, la extraccion consulta los partidos del dia anterior en API-Football y guarda intacta la respuesta JSON. La API key se obtiene como `SecureString` desde SSM Parameter Store. Un evento manual puede indicar `target_date`.
2. **Capa Silver (`silver/`):** Los eventos S3 de `bronze/` activan la validacion y conversion a Parquet particionado por temporada. Los partidos aun no disputados pueden tener goles nulos y se conservan; los marcadores solo entran a los KPIs cuando su estado es final (`FT`, `AET` o `PEN`). Los registros con estructura o valores invalidos van a `cuarentena/`.
3. **Capa Gold (`gold/`):** Silver publica un marcador en `control/gold-ready/` despues de completar la transformacion. Gold filtra partidos finalizados, reconstruye `football_kpis_gold` y crea las vistas `football_effectiveness` y `football_match_kpis` en Athena.

La efectividad se expresa provisionalmente como porcentaje de victorias (`wins / matches_played * 100`). El cliente debe confirmar la formula de negocio antes de aceptar los KPIs.

---

## 2. Diagrama de arquitectura

```
Amazon EventBridge (diario 12:00 UTC; consulta el dia anterior)
       |
       v
+-------------------------------+
| Lambda: extract_bronze        | <--- SSM SecureString (/football/api_key)
+-------------------------------+ <--- API-Football
       |
       v
Amazon S3: bucket/bronze/year=YYYY/month=MM/fixtures_YYYYMMDD.json
       |
       | (Evento s3:ObjectCreated en prefix 'bronze/')
       v
+-------------------------------+      [Falla de validacion]
| Lambda: transform_silver      | -----------------------------> S3: bucket/cuarentena/
+-------------------------------+
       |
       v
Amazon S3: bucket/silver/season=YYYY/fixtures_YYYYMMDD.parquet
       |
       | (Silver publica control/gold-ready/ al terminar)
       v
+-------------------------------+
| Lambda: kpis_gold             |
+-------------------------------+
       |
       v
Amazon Athena / S3: bucket/gold/kpis/ (Tabla y vistas de KPIs para analistas)
```

---

## 3. Estructura del proyecto

```
football/
├── README.md                          # Documentacion tecnica del proyecto
├── infra/
│   └── template.yaml                  # Plantilla CloudFormation completa
├── src/
│    ├── extract_bronze/               
│    │   └── handler.py                # Handler para la ingestion de datos
│    ├── transform_silver/               
│    │   └── handler.py                # Handler para la transformacion
│    └── kpis_gold/                 
│        └── handler.py                # Handler para la obtencion de metricas
├── tests/
│    └── test_pipeline.py              # Tests para el pipeline (10 pruebas unitarias)
└── docs/
    ├── rfp.md                         # Requerimiento formal del cliente (RFP)
    ├── sow.md                         # Alcance de trabajo y compromisos (SOW)
    ├── adr/
    │   └── ADR-01-Lambda_vs_EC2.md     # Registro de decision de arquitectura
    ├── ArquitecturaFootballSoccer.jpg  # Vista previa visual anterior
    └── ArquitecturaFootballSoccer.json # Fuente del diagrama
```

---

## 4. Decisiones arquitectonicas (ADR)

Las decisiones tecnicas y justificaciones cuantitativas de diseno para este proyecto se documentan formalmente mediante registros de decision de arquitectura:

- [ADR 0001: Migracion de instancia EC2 permanente a arquitectura serverless con AWS Lambda y EventBridge](docs/adr/ADR-01-Lambda_vs_EC2.md)

---

## 5. Guia paso a paso para el levantamiento en AWS

### Paso 1: Prerrequisitos y Credenciales
1. **Crear la clave de API-Football en SSM Parameter Store:**
   - En la consola de AWS, ir a **Systems Manager** -> **Parameter Store** -> **Create parameter**.
   - Nombre: `/football/api_key`
   - Tier: Standard
   - Type: `SecureString`
   - Value: Tu clave de API-Football obtenida de api-sports.io.
   *(Alternativamente via AWS CLI: `aws ssm put-parameter --name "/football/api_key" --value "TU_API_KEY" --type "SecureString"`).*
2. **Crear el bucket S3 para codigo de despliegue:**
   ```bash
   aws s3 mb s3://football-code-<tu-account-id> --region us-east-1
   ```
3. **Obtener el ARN del Layer compatible con Python 3.12 y PyArrow:**
   Se recomienda usar el layer administrado oficial `AWSSDKPandas-Python312` (x86_64), el cual incluye tanto `pyarrow` como `pandas`. Para `us-east-1`:
   `arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12`

### Paso 2: Empaquetar y subir el codigo de las Lambdas
Desde el directorio `football/src`:
```bash
# Empaquetar cada funcion en la raiz del ZIP
cd src/extract_bronze  && zip -j ../../extract_bronze.zip handler.py
cd ../transform_silver && zip -j ../../transform_silver.zip handler.py
cd ../kpis_gold        && zip -j ../../kpis_gold.zip handler.py
cd ../..

# Subir los archivos al bucket S3
aws s3 cp extract_bronze.zip s3://football-code-<tu-account-id>/lambda/
aws s3 cp transform_silver.zip s3://football-code-<tu-account-id>/lambda/
aws s3 cp kpis_gold.zip s3://football-code-<tu-account-id>/lambda/
```

### Paso 3: Despliegue con AWS CloudFormation

#### Opcion A: Desde la Consola Web de AWS
1. Ir al servicio **CloudFormation** en la region `us-east-1`.
2. Seleccionar **Create stack** -> **With new resources (standard)**.
3. Cargar el archivo de plantilla `football/infra/template.yaml`.
4. Configurar los parametros:
   - **Stack name:** `football-analytics-pipeline`
   - **Environment:** `dev` (o `prod`)
   - **AlertEmail:** Correo electronico para notificaciones operacionales y de presupuesto.
   - **CodeBucket:** `football-code-<tu-account-id>`
   - **CodePrefix:** `lambda/`
   - **PyArrowLayerArn:** `arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12`
   - **SSMApiKeyName:** `/football/api_key`
   - **LeagueId:** `1` (o el identificador de la liga deseada, ej. `39` para Premier League).
   - **Season:** `2026`
   - **BudgetLimitUsd:** `5`
5. Avanzar a la pantalla de confirmacion, marcar la casilla de permisos IAM (`CAPABILITY_NAMED_IAM`) y hacer clic en **Submit**.

#### Opcion B: Mediante AWS CLI
```bash
aws cloudformation deploy \
    --template-file infra/template.yaml \
    --stack-name football-analytics-pipeline \
    --parameter-overrides \
        Environment=dev \
        AlertEmail=tu-correo@ejemplo.com \
        CodeBucket=football-code-<tu-account-id> \
        CodePrefix=lambda/ \
        PyArrowLayerArn=arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:12 \
        SSMApiKeyName=/football/api_key \
        LeagueId="1" \
        Season="2026" \
        BudgetLimitUsd=5 \
    --capabilities CAPABILITY_NAMED_IAM
```

### Paso 4: Confirmacion de suscripciones
Revisa tu correo electronico y confirma las dos solicitudes de suscripcion enviadas por **AWS Notifications** (SNS y AWS Budgets).

---

## 6. Pruebas y verificacion

### 1. Pruebas unitarias locales
Ejecutar la suite de pruebas unitarias que valida reintentos ante fallas de red, parsing de partidos finalizados, particionado y generacion de vistas CTAS:
```bash
python3 -m unittest discover -s football/tests -v
```

### 2. Verificacion en vivo en AWS
1. **Ejecucion manual de extraccion:**
   - En **AWS Lambda**, abrir `football-extract-bronze-dev` (o el nombre generado).
   - En la pestaña **Test**, configurar un evento de prueba:
     ```json
     {
       "target_date": "2026-11-17"
     }
     ```
   - Ejecutar la prueba y comprobar el status `200` y la clave de S3 generada.
2. **Validacion de cascada en S3:**
   - En **S3**, revisar el bucket `football-data-lake-dev-<account-id>`.
   - Comprobar que se creo `bronze/year=YYYY/month=MM/fixtures_YYYYMMDD.json`.
   - Verificar la creacion automatica en `silver/season=2026/fixtures_YYYYMMDD.parquet`.
   - Constatar la aparicion del marcador en `control/gold-ready/` y la publicacion en `gold/kpis/`.
3. **Consulta de KPIs en Amazon Athena:**
   - En **Athena**, seleccionar la base de datos `football_db_dev`.
   - Ejecutar la consulta de aceptacion para ver la efectividad del cuerpo tecnico:
     ```sql
     SELECT team_name, matches_played, wins, draws, losses, goals_for, goals_against, win_percentage
     FROM football_effectiveness
     ORDER BY win_percentage DESC;
     ```

---

## 7. Desmantelamiento (Teardown)
Para retirar todos los recursos y garantizar cero costo:
1. Vaciar el bucket S3 `football-data-lake-dev-<account-id>`.
2. En CloudFormation, seleccionar `football-analytics-pipeline` y hacer clic en **Delete**.
3. Eliminar el bucket de codigo `football-code-<tu-account-id>` y el parametro `/football/api_key` de Parameter Store.
