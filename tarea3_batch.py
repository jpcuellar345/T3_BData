import matplotlib
matplotlib.use("Agg")  # Para que funcione sin entorno gráfico
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, desc, sum as _sum

# ============================================================
# 1. Crear sesión Spark
# ============================================================
spark = SparkSession.builder.appName("Tarea3_Batch_Terrorismo").getOrCreate()

# ============================================================
# 2. Cargar el conjunto de datos desde HDFS
#    (Punto de la guía: "Cargar el conjunto de datos seleccionado")
# ============================================================
file_path = "hdfs://localhost:9000/Tarea3/terrorismo.csv"

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load(file_path)

# Eliminar columnas de metadatos de la API (las que empiezan con ":")
columnas_utiles = ["departamento", "municipio", "codigo_dane", "armas_medios", "fecha_hecho", "cantidad"]
df = df.select(*columnas_utiles)

# ============================================================
# 3. EDA inicial
# ============================================================
print("=" * 60)
print("=== ESQUEMA DEL DATASET ===")
df.printSchema()

print("=" * 60)
print("=== PRIMERAS 10 FILAS ===")
df.show(10, truncate=False)

print("=" * 60)
print("=== TOTAL DE REGISTROS ===")
print(f"Total: {df.count()} filas")

# ============================================================
# 4. Limpieza y transformación
#    (Punto de la guía: "Realizar limpieza, transformación y EDA")
# ============================================================
df_clean = df.withColumn("fecha_hecho", to_date(col("fecha_hecho"), "dd/MM/yyyy")) \
             .filter(col("cantidad").isNotNull()) \
             .filter(col("departamento").isNotNull()) \
             .filter(col("fecha_hecho").isNotNull())

print("=" * 60)
print("=== DATOS DESPUÉS DE LIMPIEZA ===")
print(f"Filas válidas: {df_clean.count()}")

# ============================================================
# 5. Estadísticas básicas
# ============================================================
print("=" * 60)
print("=== ESTADÍSTICAS DE LA COLUMNA CANTIDAD ===")
df_clean.select("cantidad").summary().show()

# ============================================================
# 6. Top 10 departamentos con más incidentes
# ============================================================
print("=" * 60)
print("=== TOP 10 DEPARTAMENTOS CON MÁS INCIDENTES ===")
df_clean.groupBy("departamento").agg(_sum("cantidad").alias("total_incidentes")).orderBy(desc("total_incidentes")).show(10, truncate=False)

# ============================================================
# 7. Incidentes por año
# ============================================================
print("=" * 60)
print("=== INCIDENTES POR AÑO ===")
df_clean.withColumn("anio", year("fecha_hecho")) \
    .groupBy("anio") \
    .agg(_sum("cantidad").alias("total")) \
    .orderBy("anio") \
    .show(50)

# ============================================================
# 8. Top 10 tipos de armas/medios más usados
# ============================================================
print("=" * 60)
print("=== TOP 10 TIPOS DE ARMAS/MEDIOS ===")
df_clean.groupBy("armas_medios") \
    .agg(_sum("cantidad").alias("total")) \
    .orderBy(desc("total")) \
    .show(10, truncate=False)

# ============================================================
# 9. Top 10 municipios más afectados
# ============================================================
print("=" * 60)
print("=== TOP 10 MUNICIPIOS MÁS AFECTADOS ===")
df_clean.groupBy("municipio") \
    .agg(_sum("cantidad").alias("total")) \
    .orderBy(desc("total")) \
    .show(10, truncate=False)

# ============================================================
# 10. Análisis de estacionalidad: Incidentes por Año y Mes
# ============================================================
print("=" * 60)
print("=== INCIDENTES POR AÑO Y MES ===")
estacionalidad = df_clean.withColumn("anio", year("fecha_hecho")) \
    .withColumn("mes", month("fecha_hecho")) \
    .groupBy("anio", "mes") \
    .agg(_sum("cantidad").alias("total")) \
    .orderBy("anio", "mes")

estacionalidad.show(60)

# ============================================================
# 11. Evolución del uso de armas por año
# ============================================================
print("=" * 60)
print("=== EVOLUCIÓN DEL USO DE ARMAS POR AÑO ===")
armas_por_anio = df_clean.withColumn("anio", year("fecha_hecho")) \
    .groupBy("anio", "armas_medios") \
    .agg(_sum("cantidad").alias("total")) \
    .orderBy("anio", desc("total"))

armas_por_anio.show(60)

# ============================================================
# 12. VISUALIZACIONES (4 gráficos)
    # •	Gráfico de Barras Horizontal (Top 10 Departamentos)
    # •	Gráfico de Línea (Evolución Anual de Incidentes)
    # •	Mapa de Calor (Incidentes por Año y Mes)
    # •	Gráfico de Barras Apiladas (Tipos de Armas por Año)
#     (Punto de la guía: "Visualizar los resultados en batch")
# ============================================================
print("=" * 60)
print("=== GENERANDO GRÁFICOS ===")

# ---------- Gráfico 1: Top 10 Departamentos (Barras Horizontales) ----------
top_dept = df_clean.groupBy("departamento") \
    .agg(_sum("cantidad").alias("total")) \
    .orderBy(desc("total")) \
    .limit(10) \
    .toPandas()

plt.figure(figsize=(10, 6))
plt.barh(top_dept["departamento"], top_dept["total"], color="steelblue")
plt.xlabel("Total de incidentes")
plt.ylabel("Departamento")
plt.title("Top 10 Departamentos - Delito de Terrorismo (Colombia)")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("top_departamentos.png", dpi=100)
plt.close()
print("Gráfico 1 guardado: top_departamentos.png")

# ---------- Gráfico 2: Evolución Anual de Incidentes (Línea) ----------
por_anio = df_clean.withColumn("anio", year("fecha_hecho")) \
    .groupBy("anio") \
    .agg(_sum("cantidad").alias("total")) \
    .orderBy("anio") \
    .toPandas()

plt.figure(figsize=(10, 6))
plt.plot(por_anio["anio"].to_numpy(),
         por_anio["total"].to_numpy(),
         marker="o", color="firebrick", linewidth=2)
plt.xlabel("Año")
plt.ylabel("Total de incidentes")
plt.title("Evolución de Incidentes por Año - Terrorismo (Colombia)")
plt.grid(True, linestyle="--", alpha=0.6)
plt.tight_layout()
plt.savefig("incidentes_por_anio.png", dpi=100)
plt.close()
print("Gráfico 2 guardado: incidentes_por_anio.png")

# ---------- Gráfico 3: Mapa de Calor (Año vs Mes) ----------
# Pivotear los datos: filas = años, columnas = meses
estac_pd = estacionalidad.toPandas()
pivot_estac = estac_pd.pivot(index="anio", columns="mes", values="total").fillna(0)

# Asegurar que estén los 12 meses
for m in range(1, 13):
    if m not in pivot_estac.columns:
        pivot_estac[m] = 0
pivot_estac = pivot_estac[sorted(pivot_estac.columns)]

fig, ax = plt.subplots(figsize=(12, 7))
im = ax.imshow(pivot_estac.values, cmap="YlOrRd", aspect="auto")

# Etiquetas de los ejes
ax.set_xticks(np.arange(len(pivot_estac.columns)))
ax.set_xticklabels(["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                    "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"])
ax.set_yticks(np.arange(len(pivot_estac.index)))
ax.set_yticklabels(pivot_estac.index)

# Anotar los valores dentro de cada celda
for i in range(len(pivot_estac.index)):
    for j in range(len(pivot_estac.columns)):
        valor = int(pivot_estac.values[i, j])
        if valor > 0:
            ax.text(j, i, str(valor), ha="center", va="center",
                    color="black", fontsize=8)

# Barra de color y títulos
cbar = plt.colorbar(im, ax=ax)
cbar.set_label("Total de incidentes")
ax.set_xlabel("Mes")
ax.set_ylabel("Año")
ax.set_title("Mapa de Calor: Incidentes de Terrorismo por Año y Mes")
plt.tight_layout()
plt.savefig("mapa_calor_anio_mes.png", dpi=100)
plt.close()
print("Gráfico 3 guardado: mapa_calor_anio_mes.png")

# ---------- Gráfico 4: Barras Apiladas (Top 5 Armas por Año) ----------
# Obtener las 5 armas más usadas
top_armas = df_clean.groupBy("armas_medios") \
    .agg(_sum("cantidad").alias("total")) \
    .orderBy(desc("total")) \
    .limit(5) \
    .select("armas_medios") \
    .rdd.flatMap(lambda x: x).collect()

# Filtrar solo esas 5 armas
armas_filtrado = armas_por_anio.filter(col("armas_medios").isin(top_armas))
pivot_armas = armas_filtrado.toPandas().pivot(
    index="anio", columns="armas_medios", values="total"
).fillna(0)

# Gráfico de barras apiladas
fig, ax = plt.subplots(figsize=(12, 7))
pivot_armas.plot(kind="bar", stacked=True, ax=ax, colormap="tab10", width=0.75)
ax.set_xlabel("Año")
ax.set_ylabel("Total de Incidentes")
ax.set_title("Evolución del Uso de los Top 5 Tipos de Armas por Año")
ax.legend(title="Tipo de Arma/Medio", bbox_to_anchor=(1.02, 1), loc="upper left")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("evolucion_armas_por_anio.png", dpi=100)
plt.close()
print("Gráfico 4 guardado: evolucion_armas_por_anio.png")

# ============================================================
# 13. Cerrar Spark
# ============================================================
spark.stop()
print("=" * 60)
print("=== PROCESO FINALIZADO ===")
print("Gráficos generados:")
print("  1. top_departamentos.png")
print("  2. incidentes_por_anio.png")
print("  3. mapa_calor_anio_mes.png")
print("  4. evolucion_armas_por_anio.png")