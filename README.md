# Data Platform Modernization Consulting

Portafolio de consultoría en ingeniería de datos sobre AWS. El repositorio reúne tres proyectos de clientes ficticios, cada uno con su **RFP** (lo que pide el cliente) y su **SOW** (lo que la consultora se compromete a entregar). Todos modernizan un flujo de datos existente hacia una arquitectura **por lotes (batch), sin servidor y con servicios básicos de AWS**, pensada para caber en la capa gratuita o costar centavos.

## Objetivo

Mostrar cómo se diagnostica un sistema de datos existente y se convierte en una propuesta técnica verificable:

- Diagnosticar el sistema actual con evidencia.
- Traducir los problemas del cliente en necesidades medibles (RFP).
- Definir alcance, entregables con criterio de aceptación, hitos, horas, riesgos y presupuesto (SOW).
- Diseñar una arquitectura simple, confiable y barata: ingesta automática, capa cruda intacta, datos limpios en Parquet y tablas listas para consultar con SQL.

## Proyectos

| Proyecto | Cliente (ficticio) | Fuente de datos | Problema | Documentos |
|---|---|---|---|---|
| **Football** | Dirección de Selecciones Nacionales | API-Football | Extracción en una instancia EC2 siempre encendida y consultas lentas por particionar solo por año | [RFP](football/docs/consultoria/rfp.md) · [SOW](football/docs/consultoria/sow.md) |
| **Binance** | MarketMatrix (fondo cuantitativo) | Klines de Binance | Pipeline inestable de velas e indicadores (RSI, Bollinger, MACD) | [RFP](binance/docs/consultoria/rfp.md) · [SOW](binance/docs/consultoria/sow.md) |
| **Asteroid** | Dirección General de Exposiciones (museos) | API NeoWs de la NASA | Procesamiento manual de un JSON de más de 300 campos y pantallas con datos desactualizados | [RFP](asteroid/docs/consultoria/rfp.md) · [SOW](asteroid/docs/consultoria/sow.md) |

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
Athena (CTAS) ──► Gold: métricas y KPIs listos para consultar
        ↓
Consumo por SQL (cuerpo técnico, sistemas de trading, pantallas de museo)

Transversal: IAM de mínimo privilegio · CloudWatch (alertas) · Parameter Store (credenciales) · CloudFormation (infraestructura como código)
```

### Qué calcula cada proyecto en Gold
- **Football:** victorias, empates, derrotas, porcentaje de victorias, goles a favor y en contra, promedio de goles por partido.
- **Binance:** RSI, Bandas de Bollinger, MACD y señal de COMPRA/VENTA por triple confirmación.
- **Asteroid:** Risk Score y energía de impacto (la fórmula la define y valida el cliente).

## Principios de diseño
- **Batch, no streaming:** actualización diaria (o por hora), suficiente para cada caso.
- **Capa cruda intacta:** se guarda lo que entrega la fuente para poder reprocesar.
- **Idempotencia:** correr la carga dos veces da el mismo resultado, sin duplicados.
- **Calidad de datos:** validación de esquema y cuarentena de registros inválidos.
- **Monitoreo:** alertas ante fallas y una guía de operación (runbook).
- **Costo controlado:** estimación con la calculadora de AWS y un AWS Budget con alerta.
- **Servicios básicos:** cualquier servicio adicional (por ejemplo Glue) se justifica por escrito.

## Tecnologías

| Tecnología | Para qué se usa |
|---|---|
| Amazon S3 | Almacenamiento del Data Lake (Bronze, Silver, Gold, cuarentena) |
| AWS Lambda (Python) | Ingesta, validación y transformación |
| Amazon EventBridge | Programación y orquestación de las corridas |
| Amazon Athena | Consultas SQL y generación de tablas Gold (CTAS) |
| AWS Glue Data Catalog | Catálogo de tablas |
| Parquet | Almacenamiento columnar y particionado para consultas rápidas |
| Amazon CloudWatch | Logs, métricas y alertas |
| AWS IAM | Permisos de mínimo privilegio |
| AWS Systems Manager Parameter Store | Credenciales de las APIs |
| AWS CloudFormation | Infraestructura como código |

## Estructura del repositorio

```
data-platform-modernization-consulting/
├── README.md
├── .gitignore
├── football/
│   └── docs/consultoria/     # rfp.md y sow.md
├── binance/
│   └── docs/consultoria/     # rfp.md y sow.md
└── asteroid/
    └── docs/consultoria/     # rfp.md y sow.md
```

Cuando se implemente un proyecto, se agregan en su carpeta `src/` (código), `infra/` (plantillas de CloudFormation) y `tests/` (pruebas).

## Estado del proyecto

- [x] RFP de los tres proyectos, escritos desde la voz del cliente
- [x] Estructura del repositorio y README
- [ ] SOW corregidos con diagnóstico, horas y costo AWS consistentes
- [ ] Estimación de costos con la calculadora de AWS
- [ ] Plantillas de CloudFormation
- [ ] Implementación y pruebas

## Equipo

Equipo 3, Temas Selectos de Ingeniería, Facultad de Ingeniería, UNAM.

| Nombre | Rol |
|---|---|
| _Nombre_ | Lead Data Engineer |
| _Nombre_ | Data Engineer |
| _Nombre_ | Data Engineer |
| _Nombre_ | Data Engineer |

## Propósito profesional

El repositorio aplica conceptos reales de ingeniería de datos: arquitecturas de Data Lake, ETL por lotes, calidad de datos, infraestructura como código y estimación de costos en la nube. Las propuestas simulan el trabajo de una consultora con clientes pequeños, donde diseñar con pocos recursos es parte del valor.

## Convenciones
- Cada proyecto vive en su carpeta y siempre tiene `rfp.md` y `sow.md`.
- Nombres de archivos y carpetas en minúsculas, con guiones y sin acentos ni espacios.
