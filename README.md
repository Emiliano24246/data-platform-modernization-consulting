# Data Platform Modernization Consulting

Portafolio de consultoría en ingeniería de datos sobre AWS. El repositorio reúne los proyectos de clientes, cada uno con su **RFP**, su **SOW** y su **documentación técnica**. Todos están enfocados en modernizar los flujos de datos existentes hacia una arquitectura **por lotes (batch), sin servidor y con servicios básicos de AWS**, pensada para optimizar los costos y el rendimiento del sistema.

## Objetivo

Mostrar cómo se diagnostica un sistema de datos existente y se convierte en una propuesta técnica verificable:

- Diagnosticar el sistema actual con evidencia.
- Traducir los problemas del cliente en necesidades medibles (RFP).
- Definir alcance, entregables con criterio de aceptación, hitos, horas, riesgos y presupuesto (SOW).
- Diseñar una arquitectura simple, confiable y barata: ingesta automática, capa cruda intacta, datos limpios en Parquet y tablas listas para consultar con SQL.
- Dejarla desplegable y probable: infraestructura como código, pruebas automáticas y una guía para verificarla en AWS.

## Proyectos

| Proyecto | Cliente | Fuente de datos | Problema | Documentos |
|---|---|---|---|---|
| **Football** | Dirección de Selecciones Nacionales | API-Football | Extracción en una instancia EC2 siempre encendida y consultas lentas por particionar solo por año | [RFP](football/docs/rfp.md) · [SOW](football/docs/sow.md) · [Técnica](football/docs/technical.md) |
| **Binance** | MarketMatrix (fondo cuantitativo) | Klines de Binance | Pipeline inestable de velas e indicadores (RSI, Bollinger, MACD) | [RFP](binance/docs/rfp.md) · [SOW](binance/docs/sow.md) · [Técnica](binance/docs/technical.md) |
| **Asteroid** | Dirección General de Exposiciones (museos) | API NeoWs de la NASA | Procesamiento manual de un JSON de más de 300 campos y pantallas con datos desactualizados | [RFP](asteroid/docs/rfp.md) · [SOW](asteroid/docs/sow.md) · [Técnica](asteroid/docs/technical.md) |

## Arquitectura de referencia

Los tres proyectos siguen el mismo patrón por capas (medallón):

```
EventBridge (calendario diario)
        ↓
Lambda de ingesta ──► Bronze: dato crudo tal como llega de la fuente (S3)
        ↓
Lambda de limpieza ──► Silver: dato limpio, validado y en Parquet (S3)
        │                  └─► Cuarentena: registros inválidos (S3)
        ↓
Lambda de métricas + Athena (CTAS) ──► Gold: métricas y KPIs listos para consultar
        ↓
Consumo por SQL (cuerpo técnico, sistemas de trading, pantallas de museo)

Transversal: IAM de mínimo privilegio · CloudWatch + SNS (alertas) · Parameter Store (credenciales) · CloudFormation (infraestructura como código) · AWS Budgets (costo)
```

Cada proyecto usa **un solo bucket de S3** con prefijos aislados por capa (`bronze/`, `silver/`, `cuarentena/`, `gold/`, `athena-results/`).

| Proyecto | Horario de la ingesta | Qué dispara la capa Gold |
|---|---|---|
| Football | 12:00 UTC (partidos del día anterior) | Marcador en `control/gold-ready/` que publica Silver |
| Binance | 00:05 UTC (vela diaria cerrada) | Marcador en `control/gold-ready/` que publica Silver |
| Asteroid | 01:00 UTC | Cada Parquet nuevo en `silver/` |

### Qué calcula cada proyecto en Gold

- **Football:** victorias, empates, derrotas, porcentaje de victorias, goles a favor y en contra, promedio de goles por partido.
- **Binance:** RSI, Bandas de Bollinger, MACD y señal de COMPRA/VENTA por triple confirmación.
- **Asteroid:** masa, energía de impacto y Risk Score (las fórmulas las define y valida el cliente).

## Principios de diseño

- **Batch, no streaming:** actualización diaria (o por hora), suficiente para cada caso.
- **Capa cruda intacta:** se guarda lo que entrega la fuente para poder reprocesar.
- **Idempotencia:** correr la carga dos veces da el mismo resultado, sin duplicados.
- **Calidad de datos:** validación de esquema y cuarentena de registros inválidos.
- **Monitoreo:** alertas ante fallas y una guía de operación (runbook).
- **Costo controlado:** estimación con la calculadora de AWS y un AWS Budget con alerta.
- **Servicios básicos:** cualquier servicio adicional (por ejemplo Glue) se justifica por escrito.
- **Infraestructura como código:** todo se despliega desde una plantilla de CloudFormation.

## Tecnologías

| Tecnología | Para qué se usa |
|---|---|
| Amazon S3 | Almacenamiento del Data Lake (Bronze, Silver, Gold, cuarentena) |
| AWS Lambda (Python 3.12) | Ingesta, validación y transformación |
| AWS Lambda Layer (AWSSDKPandas) | pandas y pyarrow para Silver, que Lambda no incluye |
| Amazon EventBridge | Programación de las corridas diarias |
| Amazon Athena | Consultas SQL y generación de tablas Gold (CTAS) |
| AWS Glue Data Catalog | Catálogo de tablas |
| Parquet | Almacenamiento columnar y particionado para consultas rápidas |
| Amazon CloudWatch + Amazon SNS | Logs, métricas, alarmas y avisos por correo |
| AWS IAM | Permisos de mínimo privilegio (un rol por Lambda) |
| AWS Systems Manager Parameter Store | Credenciales de las APIs |
| AWS CloudFormation | Infraestructura como código |
| AWS Budgets | Alerta de costo mensual |
| `unittest` (Python) | Pruebas automáticas locales, sin necesidad de una cuenta de AWS |

## Estructura del repositorio

```
.
├── README.md
├── football/
│   ├── docs/            rfp.md, sow.md, technical.md
│   ├── src/             extract_bronze/, transform_silver/, kpis_gold/
│   ├── tests/           test_pipeline.py
│   └── template.yaml
├── binance/
│   ├── docs/            rfp.md, sow.md, technical.md
│   ├── src/             extract_bronze/, transform_silver/, signals_gold/
│   ├── tests/           test_pipeline.py
│   └── template.yaml
└── asteroid/
    ├── docs/            rfp.md, sow.md, technical.md
    ├── src/             extract_bronze/, transform_silver/, metrics_gold/
    ├── tests/           test_transform_silver.py
    └── template.yaml
```

## Cómo empezar

### Requisitos

- Python 3.12 (la misma versión que usan las Lambdas).
- Git.
- Solo para desplegar: una cuenta de AWS y AWS CLI v2.

### Correr las pruebas (sin AWS)

Las pruebas reemplazan `boto3` con un *stub*, así que **no necesitan credenciales, cuenta de AWS ni internet**. Corre cada proyecto por separado, desde su carpeta (`football` y `binance` tienen un archivo con el mismo nombre, `test_pipeline.py`).

**Windows (PowerShell):**

```powershell
cd football
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install pyarrow
python -m unittest discover -s tests -v
```

**macOS / Linux:**

```bash
cd football
python3 -m venv .venv && source .venv/bin/activate
pip install pyarrow
python -m unittest discover -s tests -v
```

| Proyecto | Dependencias para las pruebas |
|---|---|
| Football | `pyarrow` |
| Binance | `pandas numpy pyarrow` |
| Asteroid | `pandas pyarrow` |

Si todo va bien, la salida termina con `OK`.

**Problemas frecuentes**

- `ModuleNotFoundError` (pandas, pyarrow): instala la dependencia en el entorno virtual activo.
- `FileNotFoundError ... handler.py`: las pruebas buscan `src/<función>/handler.py` en la carpeta del proyecto; revisa que corres el comando desde esa carpeta.
- `NoRegionError` (solo Binance, si tienes `boto3` instalado): ejecuta `$env:AWS_DEFAULT_REGION="us-east-1"` en PowerShell (`export AWS_DEFAULT_REGION=us-east-1` en bash) antes de las pruebas.
- Bloqueo al activar el entorno virtual en PowerShell: ejecuta `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` una vez.

### Desplegar y probar en AWS

Cada proyecto tiene su guía paso a paso en `docs/technical.md`: empaquetado de las Lambdas, despliegue con CloudFormation, pruebas de idempotencia, cuarentena, alertas y consultas de aceptación en Athena, y limpieza de recursos.

## Estado del proyecto

| Proyecto | RFP y SOW | Documentación técnica | Plantilla CloudFormation | Pruebas automáticas |
|---|---|---|---|---|
| Football | Sí | Sí | Sí | Ingesta, Silver y Gold |
| Binance | Sí | Sí | Sí | Ingesta, Silver (indicadores) y Gold |
| Asteroid | Sí | Sí | Sí | Parcial: solo Silver |

Pendientes: pruebas de la extracción, la cuarentena y las fórmulas Gold de Asteroid, y la prueba contra la línea base de 90 días de Binance cuando el cliente la entregue.

## Propósito profesional

Este portafolio documenta el flujo completo de una consultoría de datos: del diagnóstico al diseño, la implementación, las pruebas y la operación. Muestra habilidades en ingeniería de datos sobre AWS (arquitectura por lotes sin servidor, infraestructura como código, calidad de datos y control de costos) y en la comunicación técnica con clientes.

## Equipo de Consultoría

| Nombre | Rol |
|---|---|
| Tristán Qesen Sánchez Mayen | Lead Data Engineer |
| Darinka Sandoval Hernández | Data Engineer |
| Emiliano Román Salgado Cruz | Data Engineer |
| Angel Hazel Tapia Ledesma | Data Engineer |
| Yessica Lizeth Caballero García | Data Engineer |

## Convenciones

- Cada proyecto vive en su carpeta y siempre tiene `docs/rfp.md`, `docs/sow.md`
- El código de cada Lambda vive en `src/<función>/handler.py`, y sus pruebas en `tests/`.
- La infraestructura de cada proyecto se define en un único `template.yaml`.