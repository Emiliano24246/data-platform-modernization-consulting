# RFP: NEO Risk Monitor - Datos de asteroides para exhibiciones

**Cliente:** Asteroid DataEngineering Project (red de museos y planetarios)
**Área solicitante:** Dirección General de Exposiciones
**Fecha de publicación:** 13 de octubre de 2026

## 1. Quiénes somos
Somos la Dirección General de Exposiciones de una red de museos y planetarios. Queremos que nuestras exhibiciones muestren datos astronómicos reales de asteroides cercanos a la Tierra (NEOs) de forma atractiva para el público.

## 2. El problema
- Los datos vienen de la API NeoWs de la NASA en un JSON crudo de más de 300 campos, y hoy los procesamos a mano.
- Esa tarea repetitiva la hace personal especializado, lo cual nos cuesta tiempo y dinero.
- Nuestras pantallas muestran datos desactualizados porque la actualización depende de que alguien la haga.
- Para una exhibición nos basta con una actualización diaria; no necesitamos tiempo real.

## 3. Qué necesitamos lograr
| # | Necesidad | Cómo sabremos que se cumplió (medible) |
|---|---|---|
| N1 | Actualización diaria sin intervención humana | Cada día el JSON de la NASA se convierte a formato Parquet en S3 sin que nadie intervenga, durante 7 días seguidos. Correr la carga dos veces el mismo día no duplica datos. |
| N2 | Datos confiables | Los registros con nulos en los campos obligatorios no llegan a las tablas finales: se envían a una ruta de cuarentena en S3 y quedan registrados. Los mismos controles rechazan registros cuyo esquema no coincide con el esperado. |
| N3 | Métricas listas para las exhibiciones | Existen tablas en Athena con el Risk Score y la energía de impacto. La consulta de aceptación sobre 30 días de datos termina en menos de 10 segundos. |
| N4 | Aviso cuando algo falla | Si una corrida diaria falla, recibimos una alerta. |

## 4. Qué NO queremos en este proyecto
- Herramientas, bases de datos o servicios de terceros fuera del ecosistema de AWS.
- Procesos que requieran ejecución o mantenimiento manual diario.
- El desarrollo de las pantallas o tableros de visualización.

## 5. Datos que tenemos y datos que faltan
**Tenemos:** acceso a la API gratuita de la NASA (NeoWs).
**Entregaremos:** la fórmula del Risk Score y de la energía de impacto, ya validada por nuestro equipo. Los consultores no definen el modelo físico; solo lo aplican.
**Falta:** el pipeline de limpieza y cálculo, la validación de calidad y las tablas consultables.

## 6. Restricciones
- **Presupuesto máximo:** $450,000 MXN para el desarrollo completo.
- **Plazo:** 10 semanas, alineado con el calendario de nuevas exhibiciones.
- **Seguridad y privacidad:** catálogo de datos gestionado en AWS Glue con permisos IAM estrictos.
- **Quién lo va a usar:** nuestros sistemas de pantallas consultarán las tablas de Athena; el equipo de tecnología de los museos operará el pipeline.
- **Servicios:** preferimos servicios básicos de AWS. Cualquier servicio adicional debe justificarse por escrito.

## 7. Qué esperamos recibir
- Un pipeline por capas (cruda, limpia y lista para consulta) en S3, automatizado.
- Validaciones de calidad y ruta de cuarentena.
- Tablas de métricas en Athena.
- Plantillas de CloudFormation para desplegar todo.
- Estimación de costos con la calculadora de AWS y una alerta de presupuesto.
- Documentación y una guía de operación (runbook) para que nuestro equipo atienda fallas.

## 8. Cómo evaluaremos las propuestas
- 40 %: experiencia técnica comprobable en servicios de datos de AWS.
- 30 %: calidad del diseño por capas y de la validación de datos.
- 20 %: apego al presupuesto máximo.
- 10 %: viabilidad del plazo de 10 semanas.

## 9. Qué debe incluir su propuesta
- Diagnóstico de nuestro proceso actual, con evidencia de cada hallazgo.
- Alcance: qué incluye y qué no.
- Entregables con criterio de aceptación, ligados a las necesidades N1 a N4.
- Plan por hitos con horas estimadas por persona.
- Riesgos y supuestos.
- Presupuesto desglosado (incluido el costo estimado de AWS) y esquema de pagos.
- Equipo propuesto y roles.
