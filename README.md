# Tarea 3 - Procesamiento de Datos con Apache Spark

**Curso:** Big Data (202016911)  
**Universidad:** UNAD - ECBTI  
**Estudiante:** Juan Pablo Cuellar Vanegas  
**Grupo:** 94  

---

## Descripción

Este repositorio contiene el código desarrollado para la Tarea 3 del curso de Big Data, que aborda el análisis espaciotemporal de incidentes de terrorismo en Colombia utilizando Apache Hadoop, Apache Spark y Apache Kafka.

El caso de estudio se centra en el conjunto de datos **"Reporte Delito Terrorismo"** publicado por la Policía Nacional en el portal de Datos Abiertos Colombia:

- **Fuente:** https://www.datos.gov.co/Seguridad-y-Defensa/Reporte-Delito-Terrorismo-Polic-a-Nacional/37p5-impc/about_data
- **API CSV:** https://www.datos.gov.co/api/v3/views/37p5-impc/query.csv

---

## Estructura del repositorio

├── tarea3_batch.py # Procesamiento batch con Spark

├── kafka_producer.py # Productor de datos simulados (Kafka)

├── spark_streaming_consumer.py # Consumidor Spark Streaming

├── README.md

└── graficos/ # Gráficos generados

    ├── top_departamentos.png
    ├── incidentes_por_anio.png
    ├── mapa_calor_anio_mes.png
    └── evolucion_armas_por_anio.png
