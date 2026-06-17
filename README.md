# Inventario Flexy ETL

Pipeline ETL en Python para procesar reportes Excel del WMS Flexy y generar un Data Warehouse local en Parquet, listo para consumo en Power BI.

---

## Objetivo

El proyecto transforma snapshots diarios de inventario en un modelo tipo estrella:

- `fact_inventario/` como tabla de hechos particionada por `fecha_corte`.
- `fact_inventario_actual.parquet` como vista del último snapshot disponible.
- `fact_snapshot_control.parquet` como resumen histórico por fecha de corte.
- `dim_cliente.parquet`
- `dim_producto.parquet`
- `dim_fecha.parquet`
- `dim_ubicacion.parquet`

Cada fila del Excel representa 1 registro operativo. La tabla final `fact_inventario` queda a nivel de `pallet_logico`, resuelto por ubicación física.

---

## Nuevas Características Integradas (Mejoras Recientes)

### 1. Integración con Lista Maestra de Productos
- **Mapeo Automático de SKUs**: El pipeline consume el archivo `LISTA MAESTRA DE PRODUCTOS.xlsx` (pestaña `verificada`) y asocia automáticamente el producto clasificado y el peso exacto (`Peso x UM`).
- **Manejo de SKUs Nulos (`SIN_SKU`)**: Los registros sin código en el Excel original ya no se eliminan; ahora se etiquetan como `SIN_SKU` y se catalogan como `"PRODUCTO SIN CLASIFICAR"` para no perder peso/tonelaje en Power BI.
- **Validación Permisiva (Advertencia)**: Si un SKU no está en la Lista Maestra de Productos, el script no se detiene; en su lugar, emite una advertencia en la consola y aplica una clasificación genérica (`OTROS` y peso de `1.0 KG`), manteniendo la continuidad del flujo.

### 2. Extracción y Desfase Temporal de Fechas
- **Fuente de Fecha Dinámica**: El parámetro `DATE_SOURCE` (en `config.py`) permite elegir si extraer la fecha desde la celda interna (`"content"`) o desde el nombre del archivo (`"filename"`), dando soporte a formatos `DD-MM-YYYY` y `YYYY-MM-DD`.
- **Desfase de Corte**: El parámetro `DATE_SHIFT_DAYS` aplica un desfase (ej. `-1` día) para corregir la fecha operativa real, ya que el reporte descargado "hoy" representa el inventario al cierre del día anterior.

### 3. Formato y Presentación de Excels Limpios
- **Nueva Estructura de Nombres**: Los archivos Excel generados comienzan con la fecha para facilitar la ordenación:
  - Excel Limpio: `DD-MM-YYYY_inventario.xlsx` (ej. `15-06-2026_inventario.xlsx`).
  - Auditoría: `DD-MM-YYYY_auditoria_ocupacion.xlsx` (ej. `15-06-2026_auditoria_ocupacion.xlsx`).
- **Alineación Centrada**: Todas las columnas del Excel (incluyendo fechas y números) se centran de forma automática.
- **Sin Columnas Técnicas en Salida Limpia**: La columna técnica de auditoría `_SOURCE_ROW_NUM` se remueve del archivo limpio final para limpieza visual de cara al negocio, y se mantiene únicamente en las hojas de auditoría.

### 4. Reporte Resumen en Consola
Al terminar la corrida, el ETL imprime un reporte en formato de tabla ASCII con el resumen de la ejecución del pipeline:
- Nombre del archivo y su fecha de corte calculada.
- Estado final (`PROCESADO`, `ERROR`).
- Cantidad de pallets lógicos, consolidaciones aplicadas y SKUs faltantes en catálogo.

---

## Flujo General

```text
Excel Flexy
  -> ORIGINAL/
  -> Python ETL (main.py)
  -> PROCESADOS/Excel/ (Limpio centrado: DD-MM-YYYY_inventario.xlsx)
  -> DW/fact_inventario/fecha_corte=YYYY-MM-DD/data.parquet
  -> DW/fact_inventario_actual.parquet
  -> DW/fact_snapshot_control.parquet
  -> DW/dim_*.parquet
  -> Power BI (Actualización directa)
```

---

## Estructura del Proyecto

```text
Inventario_Flexy_ETL/
|-- main.py
|-- config.py
|-- requirements.txt
|-- README.md
|-- .gitignore
|-- modules/
|   |-- control.py
|   |-- dimensiones.py
|   |-- extract.py
|   |-- file_manager.py
|   |-- load.py
|   |-- master_catalog.py      <-- Carga y lectura del catálogo maestro
|   |-- parquet_io.py
|   |-- snapshot.py
|   |-- transform.py
|   |-- ubicaciones.py
|   `-- utils.py
|-- tests/
|   |-- test_data_quality_runner.py
|   |-- test_master_catalog.py  <-- Pruebas de integración del catálogo
|   `-- test_historico.py
`-- venv/
```

---

## Responsabilidad de Módulos

- `main.py`: Orquestación del pipeline, logging, impresión de reporte resumen y regeneración del DW.
- `modules/master_catalog.py`: Realiza copia temporal de seguridad de la Lista Maestra de Productos para evitar bloqueos por uso en Google Drive y carga los datos de SKUs.
- `modules/data_quality/rules_input.py`: Reglas de calidad sobre el Excel crudo, extracción de fecha corte con desfases, y chequeo de negativos.
- `modules/data_quality/runner.py`: Validación permisiva de SKUs y advertencias en logs.
- `modules/transform.py`: Mapea los SKUs a la Lista Maestra y realiza el saneamiento de nulos.
- `modules/load.py`: Exportación de Excels limpios (formateo centrado y eliminación de columnas técnicas) y auditorías.

---

## Ejecución (Terminal Bash / Git Bash)

### Instalar Requisitos:
```bash
./venv/Scripts/python.exe -m pip install -r requirements.txt
```

### Ejecutar Pipeline (Archivos nuevos):
```bash
./venv/Scripts/python.exe main.py
```

### Reprocesar Histórico (Modo forzado):
```bash
./venv/Scripts/python.exe main.py --force
```

### Ejecutar Suite de Pruebas Unitarias:
```bash
./venv/Scripts/python.exe -m unittest discover -s tests
```

---

## Integración con Power BI

El archivo de Power BI (`Reporte Flexy v4 - ETL Python.pbix`) apunta al directorio `DW/` para consumir los archivos Parquet optimizados.

### Medida de Fecha del Título
El título de Power BI debe consumir directamente el campo sin restarle días manualmente, ya que el ETL en Python se encarga de aplicar la fecha operativa real:
```DAX
Fecha Inventario = 
"Inventario al " &
FORMAT(MAX(dim_fecha[fecha]), "[$-es-PE]dd ""de"" mmmm, yyyy")
```
