# Inventario Flexy ETL

Pipeline ETL en Python para procesar reportes de inventario descargados del sistema WMS **Flexy**.

Automatiza la limpieza, transformación y consolidación de datos de inventario, generando archivos listos para análisis en **Power BI** y revisión en **Excel**.

---

## Arquitectura del sistema

```
Flexy (WMS)
    ↓
Descarga manual de Excel (.xlsx)
    ↓
Carpeta ORIGINAL (datos crudos)
    ↓
Python ETL (main.py)
    ↓
Excel limpio + CSV diario
    ↓
CSV histórico consolidado
    ↓
Power BI consume inventario_historico.csv
```

**Separación de responsabilidades:**

| Herramienta | Función |
|-------------|---------|
| Python | Procesamiento, limpieza y transformación de datos |
| Power BI | Modelado de datos y visualización |
| Excel | Revisión humana de datos procesados |

---

## Requisitos

- **Python 3.10+**
- Sistema operativo: Windows
- Librerías (incluidas en `requirements.txt`):

| Librería | Versión | Uso |
|----------|---------|-----|
| pandas | 3.0.1 | Manipulación de datos |
| openpyxl | 3.1.5 | Lectura de archivos Excel |
| xlsxwriter | 3.2.9 | Escritura de archivos Excel |

---

## Instalación

### 1. Crear entorno virtual

```bash
cd "D:\AG Chavin\Proyecto Flexy\Python\Inventario_Flexy_ETL"
python -m venv venv
```

### 2. Instalar dependencias

```bash
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

---

## Estructura del proyecto

```
AG Chavin/
└── Proyecto Flexy/
    ├── Python/
    │   └── Inventario_Flexy_ETL/
    │       ├── venv/                    # Entorno virtual
    │       ├── modules/
    │       │   ├── __init__.py
    │       │   ├── file_manager.py      # Escaneo y validación de archivos
    │       │   ├── extract.py           # Lectura de Excel y validaciones
    │       │   ├── transform.py         # Lógica de transformación ETL
    │       │   ├── load.py              # Exportación y reconstrucción histórico
    │       │   ├── control.py           # Gestión de control_procesados.csv
    │       │   └── utils.py             # Funciones auxiliares
    │       ├── logs/
    │       ├── config.py                # Rutas y configuración
    │       ├── main.py                  # Orquestador principal
    │       ├── requirements.txt
    │       └── README.md
    │
    └── Reporte/
        ├── ORIGINAL/                    # Excel crudos descargados de Flexy
        │   ├── 2026-03-05.xlsx
        │   └── 2026-03-07.xlsx
        │
        └── ETL/
            ├── control_procesados.csv   # Registro de archivos procesados
            ├── Excel/                   # Excel limpios por día
            │   └── inventario_05-03-2026.xlsx
            └── CSV/
                ├── inventario_05-03-2026.csv
                └── inventario_historico.csv  ← Power BI consume este archivo
```

---

## Uso

### Ejecución normal (solo archivos nuevos)

```bash
.\venv\Scripts\python.exe main.py
```

### Reprocesar todo desde cero

```bash
.\venv\Scripts\python.exe main.py --force
```

### Flujo de trabajo diario

1. Descargar el reporte Excel desde Flexy
2. Guardarlo en la carpeta `Reporte/ORIGINAL/` con formato `yyyy-mm-dd.xlsx`
3. Ejecutar `.\venv\Scripts\python.exe main.py`
4. Revisar la salida en terminal
5. Refrescar datos en Power BI

---

## Convención de nombres de archivos

| Tipo | Formato | Ejemplo |
|------|---------|---------|
| Archivo original | `yyyy-mm-dd.xlsx` | `2026-03-05.xlsx` |
| Excel procesado | `inventario_dd-mm-yyyy.xlsx` | `inventario_05-03-2026.xlsx` |
| CSV diario | `inventario_dd-mm-yyyy.csv` | `inventario_05-03-2026.csv` |
| Histórico | `inventario_historico.csv` | — |

---

## Estrategia ETL

### Procesamiento incremental

- Python detecta archivos nuevos en `ORIGINAL/`
- Consulta `control_procesados.csv` para saber cuáles ya fueron procesados
- Solo procesa los archivos pendientes
- Con `--force` reprocesa todo sin borrar archivos manualmente

### Reconstrucción del histórico

Después de procesar archivos nuevos, Python reconstruye `inventario_historico.csv` uniendo todos los CSV diarios limpios. Usa un archivo temporal (`inventario_historico_temp.csv`) antes de reemplazar el final, evitando corrupciones si Power BI está leyendo.

---

## Transformaciones aplicadas

### Columnas de entrada (Excel original de Flexy)

```
Fecha Actualización, Empresa, Almacén, Ubicación, Código,
Cantidad, Presentación, Lote, Fecha Caducidad, Fecha Fabricación, Producto
```

### Transformaciones

| Transformación | Detalle |
|----------------|---------|
| División de Ubicación | `03,16,03,04` → CÁMARA, RACK, NIVEL, POSICIÓN |
| Mapeo de cámara | `01` → CÁMARA 01, `RECEPCION` → RECEPCIÓN |
| Normalización de almacén | CHAVIN CASMA DISPONIBLE → CHAVIN |
| Estado del producto | Según almacén original: DISPONIBLE / REEMPAQUE |
| Clasificación de producto | Detectado desde el nombre: MANGO, PALTA, FRESA, PIÑA, MARACUYÁ, GRANADA, OTROS |
| Tipo de producción | ORGÁNICO / CONVENCIONAL (detectado desde el nombre) |
| Limpieza de presentación | Se extrae el detalle del producto sin el nombre ni tipo producción |
| Toneladas | Presentación (peso) / 1000, redondeado a 2 decimales |
| Empresa → Cliente | Renombrado de columna |
| Cantidad → Cantidad Cajas | Renombrado de columna |
| Fecha Corte | Extraída del nombre del archivo original |
| Filtrado | Se eliminan registros donde Código es nulo |
| Duplicados | Se eliminan filas duplicadas |

### Columnas de salida (dataset final)

```
FECHA CORTE, CLIENTE, ALMACÉN, ESTADO PRODUCTO, CÁMARA, RACK, NIVEL,
POSICIÓN, CÓDIGO, CANTIDAD CAJAS, TONELADAS, LOTE, FECHA FABRICACIÓN,
FECHA CADUCIDAD, PRODUCTO, PRESENTACIÓN, TIPO PRODUCCIÓN
```

---

## Validaciones

### Validación de columnas

Antes de procesar, se verifica que el Excel contenga todas las columnas esperadas. Si falta alguna, el archivo se rechaza con un mensaje de error.

### Validación de valores negativos

Se revisan las columnas numéricas (Cantidad, Presentación, Rack, Nivel, Posición) antes de procesar. Si se detectan valores negativos:

- El archivo **no se procesa**
- Se muestra un reporte detallado con fila, columna, valor, código y producto
- Se registra como `ERROR_NEGATIVOS` en el control
- Se debe corregir el Excel original y volver a ejecutar

Ejemplo de salida:

```
======================================================================
  ❌ ARCHIVO BLOQUEADO: 2026-03-05.xlsx
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

### Validación de nombre de archivo

Los archivos en `ORIGINAL/` deben seguir el formato `yyyy-mm-dd.xlsx`. Los archivos temporales de Excel (`~$*.xlsx`) se ignoran automáticamente.

---

## Archivo de control

El archivo `control_procesados.csv` registra el estado de cada archivo procesado.

| Columna | Descripción |
|---------|-------------|
| archivo_original | Nombre del archivo Excel original |
| fecha_archivo | Fecha extraída del nombre (yyyy-mm-dd) |
| fecha_procesamiento | Fecha y hora del procesamiento |
| estado | PROCESADO, ERROR, ERROR_NEGATIVOS |
| archivo_excel_salida | Nombre del Excel generado |
| archivo_csv_salida | Nombre del CSV generado |
| observacion | Detalle del resultado |

---

## Integración con Power BI

Power BI se conecta únicamente a:

```
Reporte/ETL/CSV/inventario_historico.csv
```

### Configuración en Power BI

1. **Obtener datos** → Texto/CSV
2. Seleccionar `inventario_historico.csv`
3. Configurar como fuente de datos

### Métricas disponibles

| Métrica | Columna |
|---------|---------|
| Toneladas | TONELADAS |
| Cantidad de cajas | CANTIDAD CAJAS |

### Análisis posibles

- Inventario actual por producto, almacén o cámara
- Evolución histórica del inventario por fecha
- Comparación entre fechas usando slicer de FECHA CORTE
- Distribución por tipo de producción (ORGÁNICO / CONVENCIONAL)
- Estado del producto (DISPONIBLE / REEMPAQUE)

---

## Módulos

| Módulo | Responsabilidad |
|--------|-----------------|
| `config.py` | Rutas de carpetas, columnas esperadas y finales |
| `main.py` | Orquestador del pipeline ETL |
| `modules/file_manager.py` | Escaneo de carpeta ORIGINAL, validación de archivos |
| `modules/extract.py` | Lectura de Excel, validación de columnas y negativos |
| `modules/transform.py` | Toda la lógica de transformación de datos |
| `modules/load.py` | Exportación a Excel/CSV y reconstrucción del histórico |
| `modules/control.py` | Gestión del archivo de control de procesamiento |
| `modules/utils.py` | Funciones auxiliares: directorios, fechas, nombres |

---

## Solución de problemas

| Problema | Causa | Solución |
|----------|-------|----------|
| "Comando no encontrado" | Usar bash en vez de PowerShell | Usar `.\venv\Scripts\python.exe` en PowerShell |
| Error al escribir Excel | Archivo abierto en Excel | Cerrar el archivo Excel y re-ejecutar |
| Archivo ignorado | Nombre no cumple `yyyy-mm-dd.xlsx` | Renombrar correctamente |
| Archivo bloqueado por negativos | Valores negativos en Excel original | Corregir valores en el Excel y re-ejecutar |
| `[SKIP] Ya procesado` | Archivo ya está en control | Usar `--force` para reprocesar |
