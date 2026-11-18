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
│   └── template.yaml                  # Plantilla CloudFormation base
├── src/
├── tests/
└── docs/
    ├── rfp.md                         # Requerimiento formal del cliente (RFP)
    ├── sow.md                         # Alcance de trabajo y compromisos (SOW)
    ├── adr/
    │   └── ADR-01-Lambda_vs_EC2.md   # Registro de decision de arquitectura
    ├── ArquitecturaFootballSoccer.jpg  # Vista previa visual anterior
    └── ArquitecturaFootballSoccer.json # Fuente del diagrama
```

---

## 4. Decisiones arquitectonicas (ADR)

Las decisiones tecnicas y justificaciones cuantitativas de diseno para este proyecto se documentan formalmente mediante registros de decision de arquitectura:

- [ADR 0001: Migracion de instancia EC2 permanente a arquitectura serverless con AWS Lambda y EventBridge](docs/adr/ADR-01-Lambda_vs_EC2.md)

---

## 5. Boceto de despliegue en AWS (Referencia preliminar)

El despliegue de la infraestructura esta proyectado para realizarse a traves de AWS CloudFormation. El siguiente flujo resume los pasos generales para el aprovisionamiento, sujeto a ajustes conforme evolucione el desarrollo:

1. **Credencial API:** Crear en SSM Parameter Store el parametro `SecureString` `/football/api_key` (o el nombre indicado en `SSMApiKeyName`).
2. **Artefactos y dependencias:** Crear `extract_bronze.zip`, `transform_silver.zip` y `kpis_gold.zip`, cada uno con `handler.py` en la raiz. Subirlos a `CodeBucket`/`CodePrefix`; proporcionar un ARN de layer Python 3.12 que incluya `pyarrow`.
3. **Crear el stack:** Desplegar `infra/template.yaml` con entorno, liga, temporada, bucket de codigo, layer y correo. La plantilla crea un bucket S3 con prefijos, tabla Glue Silver, Lambdas, eventos, alarmas SNS y AWS Budget.
4. **Validar eventos:** Confirmar la suscripcion SNS y probar la cadena Bronze → Silver → marcador Gold. Para cargar Bronze historico, invocar `transform_silver` con `{"mode":"backfill"}`; procesa los archivos en orden y publica un marcador Gold al final.
5. **Validar KPIs:** Consultar `football_kpis_gold`, `football_effectiveness` y `football_match_kpis` en Athena. La definicion de efectividad debe ser aprobada por el cliente.

---

## 6. Pruebas y verificacion

Ejecutar las pruebas unitarias:

```bash
python -m unittest discover -s football/tests -v
```
