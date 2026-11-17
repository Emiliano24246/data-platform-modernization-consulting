# Pipeline de Analitica Tactica de Futbol (Selecciones Nacionales)

Pipeline serverless por lotes (batch) orientado a eventos para la ingesta de partidos y estadisticas desde API-Football, transformacion a Parquet particionado por temporada y generacion de KPIs tacticos (efectividad, victorias, derrotas, promedio de goles) para el cuerpo tecnico mediante Amazon Athena y Amazon S3.

---

## 1. Descripcion del sistema

1. **Capa Bronze (`bronze/`):** Descarga el JSON original e intacto de los partidos de la jornada desde API-Football utilizando credenciales seguras de Parameter Store. Guarda los datos bajo la clave `bronze/year=YYYY/month=MM/fixtures_YYYYMMDD.json`.
2. **Capa Silver (`silver/`):** Se activa automaticamente por eventos de Amazon S3 en `bronze/`. Valida la integridad estructural de los partidos y consistencia de goles. Envia datos corruptos a `cuarentena/` y genera archivos limpios en formato Parquet particionados por temporada (`silver/season=YYYY/fixtures_YYYYMMDD.parquet`).
3. **Capa Gold (`gold/`):** Se activa automaticamente al llegar archivos a `silver/`. Materializa tablas analiticas con efectividad y metricas por equipo en Athena.

---

## 2. Diagrama de arquitectura

```
Amazon EventBridge (Programacion diaria 02:00 UTC)
       |
       v
+-------------------------------+
| Lambda: extract_bronze        | <--- SSM Parameter Store (/football/api_key)
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
       | (Evento s3:ObjectCreated en prefix 'silver/')
       v
+-------------------------------+
| Lambda: kpis_gold             |
+-------------------------------+
       |
       v
Amazon Athena / S3: bucket/gold/kpis/ (Vistas de efectividad y KPIs para analistas)
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
    ├── ArquitecturaFootball.jpg       # Diagrama de arquitectura Football (jpg)
    └── ArquitecturaFootball.json      # Diagrama de arquitectura Football (json)
```

---

## 4. Decisiones arquitectonicas (ADR)

Las decisiones tecnicas y justificaciones cuantitativas de diseno para este proyecto se documentan formalmente mediante registros de decision de arquitectura:

- [ADR 0001: Migracion de instancia EC2 permanente a arquitectura serverless con AWS Lambda y EventBridge](docs/adr/ADR-01-Lambda_vs_EC2.md)

---

## 5. Boceto de despliegue en AWS (Referencia preliminar)

El despliegue de la infraestructura esta proyectado para realizarse a traves de AWS CloudFormation. El siguiente flujo resume los pasos generales para el aprovisionamiento, sujeto a ajustes conforme evolucione el desarrollo:

1. **Almacenamiento de credenciales:** Registrar la clave de acceso de la API-Football de forma segura en AWS Systems Manager Parameter Store.
2. **Revision de la plantilla de infraestructura:** Verificar los recursos declarados en `infra/template.yaml` (bucket S3 con prefijos por capas, roles de ejecucion IAM de minimo privilegio, funciones Lambda de ingesta y transformacion, y regla de EventBridge).
3. **Empaquetado y creacion del stack:** Preparar los paquetes de despliegue para las funciones Lambda en `src/` y ejecutar la creacion del stack mediante CloudFormation suministrando los parametros iniciales (entorno, identificador de liga, temporada y correo de alertas).
4. **Validacion de eventos y permisos:** Comprobar la suscripcion al topico de notificaciones SNS y constatar que las notificaciones S3 disparen secuencialmente la capa Silver ante nuevos archivos en Bronze.
5. **Verificacion analitica:** Ejecutar una consulta de prueba en Athena sobre las vistas de la capa Gold para constatar la generacion de metricas.

---

## 6. Pruebas y verificacion

Ejecutar las pruebas unitarias y de idempotencia:

```bash
python3 -m unittest discover -s football/tests -v
```
