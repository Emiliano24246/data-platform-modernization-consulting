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
│   └── template.yaml                  # Plantilla CloudFormation base
├── src/
├── tests/
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

## 5. Boceto de despliegue en AWS (Referencia preliminar)

El despliegue de la infraestructura esta proyectado para realizarse a traves de AWS CloudFormation. El siguiente flujo resume los pasos generales para el aprovisionamiento, sujeto a ajustes conforme evolucione el desarrollo:

1. **Revision de la plantilla de infraestructura:** Verificar la definicion de recursos en `infra/template.yaml` (bucket S3 por capas, roles IAM con minimo privilegio, funciones Lambda de ingesta, calidad y calculo de metricas, y regla de EventBridge).
2. **Empaquetado de componentes:** Preparar los paquetes de codigo correspondientes a las funciones Lambda dentro de `src/`.
3. **Creacion del stack en CloudFormation:** Ejecutar el aprovisionamiento del stack mediante la consola de AWS o AWS CLI suministrando los parametros base (entorno de ejecucion, llave de API de la NASA y correo electronico para alertas operativas).
4. **Validacion de rutas y suscripciones:** Confirmar la suscripcion enviada al correo registrado en Amazon SNS y verificar que el bucket S3 contenga las rutas de capas (`bronze/`, `silver/`, `gold/` y `cuarentena/`).
5. **Comprobacion de ejecucion:** Detonar una ejecucion manual de prueba para validar el procesamiento completo desde la ingesta cruda hasta la disponibilidad de tablas en Amazon Athena.

---

## 6. Pruebas y verificacion

Ejecutar las pruebas unitarias:

```bash
python3 -m unittest discover -s asteroid/tests -v
```
