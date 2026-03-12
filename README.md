# Inventario Flexy ETL

Pipeline ETL en Python para procesar reportes de inventario del sistema WMS **Flexy**, generando un **Data Warehouse local** con tabla de hechos particionada y tablas de dimensiones en formato Parquet, listo para consumo en **Power BI**.

---

## Por qué esta arquitectura

| Problema del enfoque anterior | Solución actual |
|-------------------------------|-----------------|
| Histórico gigante reconstruido cada ejecución | Snapshots particionados por fecha de corte |
| Reescribir millones de filas cada día | Solo se escribe la partición del día |
| Corregir un día requiere reprocesar todo | Se reemplaza solo la partición afectada |
| CSV no escala con volumen | Parquet con compresión Snappy |

---

## Arquitectura del sistema

```
Flexy (WMS)
    ↓
Descarga manual de Excel (.xlsx)
    ↓
Carpeta ORIGINAL/ (datos crudos, cualquier nombre)
    ↓
Python ETL (main.py)
    ↓
┌─────────────────────────────────────────────┐
│  PROCESADOS/                                │
│    └── Excel/inventario_DD-MM-YYYY.xlsx     │  ← Revisión humana
│    └── control_procesados.csv               │  ← Registro de procesamiento
│                                             │
│  DW/                                        │
│    ├── fact_inventario/                      │
│    │   ├── fecha_corte=2026-03-05/          │
│    │   │   └── data.parquet                 │  ← Snapshot diario
│    │   ├── fecha_corte=2026-03-07/          │
│    │   │   └── data.parquet                 │
│    │   └── ...                              │
│    ├── dim_cliente.parquet                   │
│    ├── dim_producto.parquet                  │
│    ├── dim_fecha.parquet                     │
│    └── dim_ubicacion.parquet                 │
└─────────────────────────────────────────────┘
    ↓
Power BI consume DW/ (parquet)
```

**Separación de responsabilidades:**

| Herramienta | Función |
|-------------|---------|
| Python | ETL: limpieza, transformación, generación de DW |
| Power BI | Modelado de datos y visualización |
| Excel | Revisión humana de datos procesados |

---

## Requisitos

- **Python 3.10+**
- Sistema operativo: Windows
- Dependencias (en `requirements.txt`):

| Librería | Versión | Uso |
|----------|---------|-----|
| pandas | 3.0.1 | Manipulación de datos |
| openpyxl | 3.1.5 | Lectura de archivos Excel |
| xlsxwriter | 3.2.9 | Escritura de archivos Excel |
| pyarrow | 23.0.1 | Lectura/escritura de archivos Parquet |

---

## Instalación

### 1. Crear entorno virtual

```powershell
cd "D:\AG Chavin\Proyecto Flexy\Python\Inventario_Flexy_ETL"
python -m venv venv
```

### 2. Instalar dependencias

```powershell
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

---

## Estructura del proyecto

```
Proyecto Flexy/
├── Python/
│   └── Inventario_Flexy_ETL/
│       ├── venv/                        # Entorno virtual
│       ├── modules/
│       │   ├── __init__.py
│       │   ├── file_manager.py          # Escaneo de carpeta ORIGINAL
│       │   ├── extract.py               # Lectura Excel, validaciones, extracción de fecha
│       │   ├── transform.py             # Lógica de transformación ETL
│       │   ├── snapshot.py              # Construcción del fact snapshot con keys
│       │   ├── dimensiones.py           # Generación de tablas de dimensiones
│       │   ├── load.py                  # Exportación Excel y escritura de particiones
│       │   ├── parquet_io.py            # Escritura genérica a Parquet
│       │   ├── control.py               # Gestión de control_procesados.csv
│       │   └── utils.py                 # Funciones auxiliares
│       ├── config.py                    # Rutas y configuración central
│       ├── main.py                      # Orquestador principal
│       ├── requirements.txt
│       └── README.md
│
└── Reporte/
    ├── ORIGINAL/                        # Excel crudos descargados de Flexy
    │
    ├── PROCESADOS/
    │   ├── control_procesados.csv       # Registro de archivos procesados
    │   └── Excel/                       # Excel limpios para revisión humana
    │       └── inventario_11-03-2026.xlsx
    │
    ├── DW/                              # Data Warehouse local
    │   ├── fact_inventario/             # Fact table particionada
    │   │   └── fecha_corte=2026-03-11/
    │   │       └── data.parquet
    │   ├── dim_cliente.parquet
    │   ├── dim_producto.parquet
    │   ├── dim_fecha.parquet
    │   └── dim_ubicacion.parquet
    │
    └── LOGS/
        └── etl.log
```

---

## Uso

### Ejecución normal (solo archivos nuevos)

```powershell
.\venv\Scripts\python.exe main.py
```

### Reprocesar todo desde cero

```powershell
.\venv\Scripts\python.exe main.py --force
```

### Flujo de trabajo diario

1. Descargar el reporte Excel desde Flexy
2. Guardarlo en `Reporte/ORIGINAL/` (cualquier nombre `.xlsx`)
3. Ejecutar `.\venv\Scripts\python.exe main.py`
4. Revisar la salida en terminal
5. Refrescar datos en Power BI

---

## Pipeline ETL — Paso a paso

```
Excel original
    │
    ├── 1. Leer Excel (openpyxl)
    ├── 2. Validar columnas esperadas
    ├── 3. Extraer fecha de corte desde "Fecha Actualización"
    ├── 4. Validar que no haya valores negativos
    ├── 5. Transformar datos (17 columnas finales)
    ├── 6. Guardar Excel limpio en PROCESADOS/Excel/
    ├── 7. Construir fact snapshot con surrogate keys
    ├── 8. Escribir partición fecha_corte=YYYY-MM-DD/data.parquet
    ├── 9. Reconstruir dimensiones desde todas las particiones
    └── 10. Registrar en control_procesados.csv
```

---

## Extracción de fecha

La fecha de corte **no depende del nombre del archivo**. Se extrae automáticamente de la primera fila válida de la columna `Fecha Actualización` del Excel.

Esto permite que los archivos en `ORIGINAL/` tengan cualquier nombre (ej: `reporte.xlsx`, `inventario_marzo.xlsx`, etc.).

Los archivos de salida se nombran con la fecha extraída: `inventario_DD-MM-YYYY.xlsx`.

---

## Transformaciones aplicadas

### Columnas de entrada (Excel Flexy)

```
Fecha Actualización, Empresa, Almacén, Ubicación, Código,
Cantidad, Presentación, Lote, Fecha Caducidad, Fecha Fabricación, Producto
```

### Transformaciones

| # | Transformación | Detalle |
|---|----------------|---------|
| 1 | División de Ubicación | `03,16,03,04` → CÁMARA, RACK, NIVEL, POSICIÓN |
| 2 | Mapeo de cámara | `01` → CÁMARA 01, `RECEPCION` → RECEPCIÓN |
| 3 | Normalización de almacén | CHAVIN CASMA DISPONIBLE → CHAVIN |
| 4 | Estado del producto | Según almacén original: DISPONIBLE / REEMPAQUE |
| 5 | Clasificación de producto | Desde el nombre: MANGO, PALTA, FRESA, PIÑA, MARACUYÁ, GRANADA, OTROS |
| 6 | Tipo de producción | ORGÁNICO / CONVENCIONAL (detectado desde el nombre) |
| 7 | Limpieza de presentación | Se extrae el detalle sin nombre de producto ni tipo |
| 8 | Toneladas | Presentación (peso) / 1000, redondeado a 2 decimales |
| 9 | Empresa → Cliente | Renombrado de columna |
| 10 | Cantidad → Cantidad Cajas | Renombrado de columna |
| 11 | Fecha Corte | Extraída de la columna "Fecha Actualización" |
| 12 | Filtrado | Se eliminan registros donde Código es nulo |
| 13 | Columnas MAYÚSCULAS | Todas las columnas finales en mayúsculas |

### Columnas de salida (17 columnas)

```
FECHA CORTE, CLIENTE, ALMACÉN, ESTADO PRODUCTO, CÁMARA, RACK, NIVEL,
POSICIÓN, CÓDIGO, CANTIDAD CAJAS, TONELADAS, LOTE, FECHA FABRICACIÓN,
FECHA CADUCIDAD, PRODUCTO, PRESENTACIÓN, TIPO PRODUCCIÓN
```

---

## Data Warehouse

### Fact table: `fact_inventario/`

Particionada por `fecha_corte=YYYY-MM-DD`. Cada partición contiene un snapshot completo del inventario en esa fecha.

Columnas adicionales generadas por el snapshot:

| Columna | Descripción |
|---------|-------------|
| `fecha_key` | Clave de fecha (YYYYMMDD) |
| `cliente_key` | Clave de cliente (nombre normalizado) |
| `producto_key` | Clave de producto (código normalizado) |
| `ubicacion_key` | Clave compuesta: `CAM01-R016-N03-P04` |
| `almacen_grupo` | CHAVIN / EXTERNOS |
| `tipo_ubicacion` | POSICION / RECEPCION / EXTERNO / SIN_UBICACION |
| `pallets` | 1 por fila (para conteo) |
| `source_file` | Archivo Excel origen |
| `source_row_num` | Fila original en el Excel |
| `snapshot_row_id` | Hash SHA-1 único por fila+fecha |

### Dimensiones

| Archivo | Contenido | Fuente |
|---------|-----------|--------|
| `dim_cliente.parquet` | Clientes únicos | Extraída del fact |
| `dim_producto.parquet` | Código, producto, presentación, tipo producción | Extraída del fact |
| `dim_fecha.parquet` | Año, mes, trimestre, semana, día | Extraída del fact |
| `dim_ubicacion.parquet` | Todas las posiciones estructurales (4 cámaras) | Generada desde configuración fija |

### Capacidad estructural (dim_ubicacion)

| Cámara | Racks | Niveles | Posiciones | Total |
|--------|-------|---------|------------|-------|
| CÁMARA 01 | 10 | 5 | 15 | 750 |
| CÁMARA 02 | 20 | 3 | 4 | 240 |
| CÁMARA 03 | 20 | 3 | 4 | 240 |
| CÁMARA 04 | 13 | 11 | 3 | 429 |
| **Total** | | | | **1,659** |

---

## Validaciones

### Validación de columnas

Se verifica que el Excel contenga las 11 columnas esperadas. Si falta alguna, el archivo se rechaza.

### Validación de valores negativos

Se revisan Cantidad, Presentación, Rack, Nivel y Posición. Si hay negativos:

- El archivo **no se procesa**
- Se muestra un reporte detallado fila por fila
- Se registra como `ERROR_NEGATIVOS` en el control

```
======================================================================
  ❌ ARCHIVO BLOQUEADO: reporte.xlsx
  📊 Total valores negativos encontrados: 2
======================================================================
  ⚠ Cantidad: 1 valor(es) negativo(s)
  ⚠ Presentación: 1 valor(es) negativo(s)

  📋 DETALLE POR FILA:
     → Fila 308  |  Cantidad = -54.0  |  Código: PT-PAL-049  |  Producto: PALTA...
     → Fila 308  |  Presentación = -540.0  |  Código: PT-PAL-049  |  Producto: PALTA...

  💡 Corrige estos valores en el Excel original y vuelve a ejecutar.
======================================================================
```

### Validación de fecha

Si la columna "Fecha Actualización" no contiene fechas válidas, el archivo se rechaza.

---

## Archivo de control

`PROCESADOS/control_procesados.csv` registra cada ejecución:

| Columna | Descripción |
|---------|-------------|
| archivo_original | Nombre del archivo Excel |
| fecha_archivo | Fecha extraída de "Fecha Actualización" (YYYY-MM-DD) |
| fecha_procesamiento | Timestamp del procesamiento |
| estado | `PROCESADO` · `ERROR` · `ERROR_NEGATIVOS` |
| archivo_excel_salida | Excel generado |
| archivo_csv_salida | (vacío — ya no se genera CSV individual) |
| observacion | Detalle del resultado + ruta de partición |

---

## Integración con Power BI

Power BI se conecta a la carpeta `DW/`:

```
Reporte/DW/
├── fact_inventario/          ← Carpeta particionada (Parquet)
├── dim_cliente.parquet
├── dim_producto.parquet
├── dim_fecha.parquet
└── dim_ubicacion.parquet
```

### Configuración en Power BI

1. **Obtener datos** → Parquet
2. Conectar cada archivo/carpeta de dimensiones y fact
3. Crear relaciones en el modelo estrella

### Métricas disponibles

| Métrica | Columna |
|---------|---------|
| Toneladas | TONELADAS |
| Cantidad de cajas | CANTIDAD CAJAS |
| Pallets | pallets |

### Análisis posibles

- Inventario por producto, almacén o cámara
- Evolución histórica por fecha de corte
- Ocupación de posiciones (fact vs dim_ubicacion)
- Distribución por tipo de producción (ORGÁNICO / CONVENCIONAL)
- Estado del producto (DISPONIBLE / REEMPAQUE)

---

## Módulos

| Módulo | Responsabilidad |
|--------|-----------------|
| `config.py` | Rutas, columnas esperadas y finales |
| `main.py` | Orquestador del pipeline ETL |
| `modules/file_manager.py` | Escaneo de archivos en ORIGINAL/ |
| `modules/extract.py` | Lectura Excel, validaciones, extracción de fecha |
| `modules/transform.py` | Transformación de datos (17 columnas finales) |
| `modules/snapshot.py` | Construcción del fact snapshot con surrogate keys |
| `modules/dimensiones.py` | Generación de tablas de dimensiones |
| `modules/load.py` | Escritura de Excel y particiones Parquet |
| `modules/parquet_io.py` | Escritura genérica a Parquet (pyarrow + snappy) |
| `modules/control.py` | Gestión de control_procesados.csv |
| `modules/utils.py` | Funciones auxiliares: directorios, fechas, nombres |

---

## Solución de problemas

| Problema | Causa | Solución |
|----------|-------|----------|
| `ModuleNotFoundError: pyarrow` | pyarrow no instalado | `.\venv\Scripts\python.exe -m pip install -r requirements.txt` |
| Error al escribir Excel | Archivo abierto en Excel | Cerrar el archivo y re-ejecutar |
| Sin fecha detectada | "Fecha Actualización" sin datos válidos | Verificar que el Excel es un reporte Flexy válido |
| Archivo bloqueado por negativos | Valores negativos en el original | Corregir valores en el Excel y re-ejecutar |
| `[SKIP] Ya procesado` | Archivo ya registrado en control | Usar `--force` para reprocesar |
| Warning de header/footer | openpyxl no puede parsear encabezado | Es solo un warning, no afecta el procesamiento |
