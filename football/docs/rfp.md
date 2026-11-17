# RFP: Modernización del sistema de analítica de fútbol

**Cliente:** Football Soccer Data Engineering Project
**Área solicitante:** Dirección de Selecciones Nacionales
**Fecha de publicación:** 13 de octubre de 2026

## 1. Quiénes somos
Somos la Dirección de Selecciones Nacionales. Usamos datos de partidos obtenidos de la API-Football para que el cuerpo técnico prepare sus decisiones tácticas. El sistema se usó para la Copa Mundial de la FIFA 2026 y queremos poder reutilizarlo en otros torneos.

## 2. El problema
- La extracción de datos históricos corre en una instancia EC2 que mantenemos encendida de forma permanente. Sospechamos que pagamos por tiempo sin uso, pero hoy no tenemos el desglose del costo.
- Los datos están en un bucket de S3 particionado solo por año. Las consultas sobre una temporada son lentas y los analistas tienen que esperar o pedir ayuda a sistemas.
- Queremos que el costo de operar el sistema sea claramente menor al actual y que podamos justificarlo ante la federación.

## 3. Qué necesitamos lograr
| # | Necesidad | Cómo sabremos que se cumplió (medible) |
|---|---|---|
| N1 | Reducir el costo mensual de extraer y procesar los datos | El costo mensual estimado de la solución es al menos 50 % menor que el costo actual de la instancia EC2 (nosotros entregamos la factura como línea base). No queda ninguna instancia encendida de forma permanente. |
| N2 | Consultas rápidas sobre una temporada completa | La consulta de aceptación de KPIs sobre una temporada completa termina en menos de 10 segundos y escanea menos de 100 MB. |
| N3 | KPIs listos para el cuerpo técnico | Existen vistas en Athena con efectividad y goles por equipo y por partido. Un analista las consulta con SQL, sin ayuda de sistemas, y obtiene los resultados esperados en 3 consultas de aceptación acordadas. |
| N4 | Extracción automática y confiable | La carga corre cada día sin intervención durante 7 días seguidos. Si se ejecuta dos veces el mismo día no duplica datos. Si falla, recibimos una alerta. |
| N5 | Conservar el dato tal como llega de la API | Cada respuesta de la API queda guardada sin modificar, de modo que podemos reprocesarla si encontramos un error en la transformación. |

## 4. Qué NO queremos en este proyecto
- Servidores virtuales encendidos de forma permanente (EC2).
- Software externo con licencia o costos ocultos; queremos servicios de AWS.
- Análisis de video o tracking óptico.
- Una interfaz o tablero para el cuerpo técnico; nos basta con tablas y vistas consultables.

## 5. Datos que tenemos y datos que faltan
**Tenemos:** scripts funcionales en Python que consumen la API-Football, el plan y las credenciales de la API, y los datos históricos actuales en S3.
**Entregaremos:** la factura actual de AWS de la instancia EC2 (línea base de costo) y la definición de los KPIs (cómo se calcula "efectividad").
**Falta:** la ingesta rehecha sin servidor, la capa de datos limpia y particionada, y las vistas de KPIs.

## 6. Restricciones
- **Presupuesto máximo:** $550,000 MXN por el proyecto completo.
- **Plazo:** 10 semanas, para llegar a nuestro calendario de preparación.
- **Seguridad y privacidad:** los datos de scouting son confidenciales. Solo las personas autorizadas deben poder leerlos (permisos de mínimo privilegio) y los datos deben estar cifrados.
- **Quién lo va a usar:** el cuerpo técnico y los analistas, consultando con SQL en Athena.
- **Servicios:** preferimos servicios básicos de AWS. Cualquier servicio adicional debe justificarse por escrito.

## 7. Qué esperamos recibir
- Datos organizados en capas (cruda, limpia y lista para análisis) en S3.
- Ingesta automática y vistas de KPIs en Athena.
- Plantillas de CloudFormation para desplegar todo.
- Estimación de costos con la calculadora de AWS y una alerta de presupuesto.
- Documentación y una guía de operación (qué hacer si algo falla).

## 8. Cómo evaluaremos las propuestas
- 40 %: experiencia técnica comprobable en arquitecturas de datos en AWS sin servidor.
- 30 %: costo operativo mensual de la solución propuesta.
- 20 %: apego al presupuesto máximo.
- 10 %: viabilidad del cronograma de 10 semanas.

## 9. Qué debe incluir su propuesta
- Diagnóstico de nuestro sistema actual, con evidencia de cada hallazgo.
- Alcance: qué incluye y qué no.
- Entregables con criterio de aceptación, ligados a las necesidades N1 a N5.
- Plan por hitos con horas estimadas por persona.
- Riesgos y supuestos.
- Presupuesto desglosado (incluido el costo estimado de AWS) y esquema de pagos.
- Equipo propuesto y roles.
