# Tarea 3 - Procesamiento de Datos con Apache Spark

**Curso:** Big Data (202016911)  
**Universidad:** UNAD - ECBTI  
**Estudiante:** Juan Pablo Cuellar Vanegas  
**Grupo:** 94  

---

# Descripción

Este repositorio contiene el código desarrollado para la Tarea 3 del curso de Big Data, que aborda el análisis espacio tiempo de incidentes de terrorismo en Colombia utilizando Hadoop, Spark y Kafka.

El caso de estudio se centra en el conjunto de datos **"Reporte Delito Terrorismo"** publicado por la Policía Nacional en el portal de Datos Abiertos Colombia:

- **Fuente:** https://www.datos.gov.co/Seguridad-y-Defensa/Reporte-Delito-Terrorismo-Polic-a-Nacional/37p5-impc/about_data
- **API CSV:** https://www.datos.gov.co/api/v3/views/37p5-impc/query.csv

---

# ¿Qué hace el código?

El script `tarea3_batch.py` realiza las siguientes operaciones sobre el dataset:

1. **Carga** el archivo CSV almacenado en HDFS.
2. **Limpia y transforma.**
3. **Análisis exploratorio (EDA)** con Df de Spark:
   - Top 10 departamentos con mayor incidencia.
   - Evolución anual de incidentes.
   - Top 10 tipos de armas/medios más usados.
   - Top 10 municipios más afectados.
   - Distribución Año × Mes.
4. **Genera 4 visualizaciones** con `matplotlib`:
   - Gráfico de barras horizontal (Top 10 departamentos).
   - Gráfico de línea (Evolución anual).
   - Mapa de calor (Incidentes por año y mes).
   - Gráfico de barras apiladas (Top 5 armas por año).

Adicionalmente, el repositorio incluye los scripts de **procesamiento en tiempo real** (`kafka_producer.py` y `spark_streaming_consumer.py`) desarrollados a partir del Anexo 3.

# Estructura del repositorio

    ├── tarea3_batch.py # Procesamiento batch con Spark
    
    ├── kafka_producer.py # Productor de datos simulados (Kafka)
    
    ├── spark_streaming_consumer.py # Consumidor Spark Streaming
    
    ├── README.md
    
    └── graficos/ # Gráficos generados

        ├── top_departamentos.png
        ├── incidentes_por_anio.png
        ├── mapa_calor_anio_mes.png
        └── evolucion_armas_por_anio.png

### Nota
Para el desarrollo del script de procesamiento batch (tarea3_batch.py), el autor se apoyó en un asistente de IA (DeepSeek) como Tools de consulta para:
Resolver dudas sobre la sintaxis de PySpark y matplotlib.

Depurar errores de indentación y tipos de datos.

Optimizar la estructura del código para mejorar su legibilidad.

Todo el código fue revisado, comprendido y adaptado por el autor para ajustarse a los requerimientos del caso de estudio. el modelo de negocio, la selección del dataset, el diseño de las visualizaciones y la interpretación de los resultados.

---

# Requisitos previos

Para ejecutar el código se requiere un entorno con las siguientes herramientas instaladas:

- **Ubuntu Server 22.04 LTS** (máquina virtual en VirtualBox)
- **Apache Hadoop 3.3.6** (HDFS + YARN)
- **Apache Spark 3.5.3**
- **Apache Kafka 3.9.2** (solo para el componente de streaming)
- **Python 3.10+**

# Librerías Python necesarias

```bash
pip install pyspark==3.5.1 kafka-python matplotlib numpy pandas
```
---
# Instrucciones de ejecución

## 1. Iniciar Hadoop (HDFS + YARN)
```
su - hadoop
start-all.sh
exit

# Iniciar Spark (Master + Worker)
start-master.sh
start-slave.sh spark://bigdata:7077
```

## 2. Cargar el dataset en HDFS
```su - hadoop
wget "https://www.datos.gov.co/api/v3/views/37p5-impc/query.csv" -O terrorismo.csv
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put terrorismo.csv /Tarea3/
exit
```
## 3. Ejecutar el procesamiento batch
```python3 tarea3_batch.py```
## 4. Ejecutar el procesamiento en tiempo real
```# Iniciar ZooKeeper
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &

# Iniciar Kafka
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &

/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic sensor_data
```
## Proceso de ejecucion en la terminal
primero ejecutar ```python3 kafka_producer.py``` 
despues se ejecuta ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py```
