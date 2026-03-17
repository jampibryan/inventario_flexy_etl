# Inventario Flexy ETL

Pipeline ETL en Python para procesar reportes Excel del WMS Flexy y generar un Data Warehouse local en Parquet, listo para consumo en Power BI.

## Objetivo

El proyecto transforma snapshots diarios de inventario en un modelo tipo estrella:

- `fact_inventario/` como tabla de hechos particionada por `fecha_corte`
- `dim_cliente.parquet`
- `dim_producto.parquet`
- `dim_fecha.parquet`
- `dim_ubicacion.parquet`

Cada fila del Excel representa 1 pallet.

## Beneficios de la arquitectura

- Solo se reescribe la particion del dia procesado.
- El historico queda acumulado por fecha de corte.
- Power BI consume parquet, no Excels crudos.
- La capacidad estructural se controla desde `dim_ubicacion`.
- La integridad entre fact y dim de ubicacion queda validada automaticamente.

## Flujo general

```text
Excel Flexy
  -> ORIGINAL/
  -> Python ETL (main.py)
  -> PROCESADOS/Excel/
  -> DW/fact_inventario/fecha_corte=YYYY-MM-DD/data.parquet
  -> DW/dim_*.parquet
  -> Power BI
```

## Estructura del proyecto

```text
Inventario_Flexy_ETL/
|-- main.py
|-- config.py
|-- requirements.txt
|-- README.md
|-- modules/
|   |-- control.py
|   |-- dimensiones.py
|   |-- extract.py
|   |-- file_manager.py
|   |-- load.py
|   |-- parquet_io.py
|   |-- snapshot.py
|   |-- transform.py
|   |-- ubicaciones.py
|   `-- utils.py
`-- venv/
```

## Responsabilidad de modulos

- `main.py`: orquestacion del pipeline, logging y regeneracion del DW.
- `modules/extract.py`: lectura de Excel, validacion de columnas y fecha.
- `modules/transform.py`: limpieza y transformacion del Excel a layout estandar.
- `modules/snapshot.py`: construccion del fact snapshot y claves tecnicas.
- `modules/ubicaciones.py`: normalizacion de camara, construccion de `ubicacion_key` y validaciones de integridad.
- `modules/dimensiones.py`: generacion de dimensiones.
- `modules/load.py`: escritura de Excel de revision, particiones parquet y saneamiento historico.
- `modules/control.py`: control de archivos procesados.
- `modules/parquet_io.py`: escritura parquet.

## Requisitos

- Python 3.10+
- Windows
- Dependencias de `requirements.txt`

Instalacion:

```powershell
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Ejecucion

Proceso normal:

```powershell
.\venv\Scripts\python.exe main.py
```

Reprocesar archivos ya registrados:

```powershell
.\venv\Scripts\python.exe main.py --force
```

Flujo diario:

1. Descargar el Excel desde Flexy.
2. Guardarlo en `Reporte/ORIGINAL/`.
3. Ejecutar `main.py`.
4. Revisar `PROCESADOS/Excel/` y `LOGS/etl.log`.
5. Refrescar Power BI.

## Pipeline ETL

1. Leer el Excel original.
2. Validar columnas esperadas.
3. Extraer `fecha_corte` desde `Fecha Actualizacion`.
4. Validar que no existan valores negativos.
5. Transformar el layout del Excel.
6. Guardar Excel limpio para revision humana.
7. Construir `fact_inventario`.
8. Validar integridad de ubicaciones antes de cargar.
9. Escribir la particion diaria parquet.
10. Sanear particiones historicas si tienen ubicaciones invalidas.
11. Regenerar dimensiones.
12. Registrar el resultado en `control_procesados.csv`.

## Transformaciones principales

Entradas esperadas del Excel:

```text
Fecha Actualizacion, Empresa, Almacen, Ubicacion, Codigo,
Cantidad, Presentacion, Lote, Fecha Caducidad, Fecha Fabricacion, Producto
```

Transformaciones relevantes:

- `Ubicacion` se divide en `CAMARA`, `RACK`, `NIVEL`, `POSICION`.
- `CAMARA` se normaliza con una unica logica compartida.
- `Almacen` se clasifica en `CHAVIN`, `ACUAPESCA`, `EMERGENT COLD` u original.
- `Estado Producto` se deriva desde el almacen original.
- `Producto`, `Clasificacion`, `Variedad` y `Calidad` se derivan desde el texto del producto.
- `Toneladas` = `Presentacion / 1000`.
- Se eliminan filas con `Codigo` nulo.
- El layout final queda en mayusculas.

Columnas finales del layout transformado:

```text
FECHA CORTE, CLIENTE, ALMACEN, ESTADO PRODUCTO, CAMARA, RACK, NIVEL,
POSICION, CODIGO, CANTIDAD CAJAS, TONELADAS, LOTE, FECHA FABRICACION,
FECHA CADUCIDAD, PRODUCTO, VARIEDAD, CLASIFICACION, CALIDAD,
TIPO DE CORTE, PRESENTACION
```

## Fact table: `fact_inventario/`

Cada particion representa un snapshot completo del inventario para una fecha.

Campos tecnicos principales:

- `fecha_key`: `YYYYMMDD`
- `cliente_key`
- `producto_key`
- `ubicacion_key`
- `ubicacion_inventario`
- `almacen_grupo`
- `tipo_almacen`
- `tipo_ubicacion`
- `pallets`
- `source_file`
- `source_row_num`
- `snapshot_row_id`

## Dimension de ubicacion

La relacion principal en Power BI es:

```text
dim_ubicacion[ubicacion_key] -> fact_inventario[ubicacion_key]
```

Formato oficial de la clave:

```text
CAM##-R###-N##-P##
```

Ejemplo:

```text
CAM01-R016-N03-P04
```

`dim_ubicacion` se genera desde `CAPACITY_CONFIG` en `modules/dimensiones.py`.

## Capacidad estructural actual

| Camara | Racks | Niveles | Posiciones | Total |
|---|---:|---:|---:|---:|
| CAMARA 01 | 10 | 5 | 14 | 700 |
| CAMARA 02 | 20 | 3 | 4 | 240 |
| CAMARA 03 | 20 | 3 | 4 | 240 |
| CAMARA 04 | 13 | 11 | 3 | 429 |
| Total |  |  |  | 1609 |

Nota:

- `CAMARA 04` existe en la dimension estructural.
- `es_operativa` permite distinguir capacidad estructural de capacidad operativa.

## Regla de clasificacion de ubicacion

Un pallet puede quedar como:

- `POSICION`
- `RECEPCION`
- `EXTERNO`
- `SIN_UBICACION`

La regla actual es:

- `EXTERNO` si el almacen no es `CHAVIN`
- `RECEPCION` si la camara es `RECEPCION`
- `POSICION` solo si la `ubicacion_key` existe en `dim_ubicacion`
- `SIN_UBICACION` en cualquier otro caso

## Mejora clave: integridad entre fact y dim_ubicacion

Se reforzo la logica de ubicaciones para evitar el problema clasico de Power BI con la categoria `(En blanco)` por claves huerfanas.

Ahora el ETL hace lo siguiente:

- fact y dimension usan exactamente el mismo constructor de `ubicacion_key`
- la camara se normaliza de forma consistente
- se validan ceros a la izquierda, prefijos y conversion numerica
- una fila no queda como `POSICION` si su clave no existe en `dim_ubicacion`
- las claves invalidas se reclasifican a `SIN_UBICACION`
- `ubicacion_key` queda nula cuando no existe match estructural
- se auditan las diferencias entre claves del fact y la dimension
- tambien se sanean particiones historicas ya guardadas

## Validaciones automaticas de ubicacion

Antes y durante la carga, el pipeline:

- cuenta pallets con ubicacion candidata sin match en `dim_ubicacion`
- lista las claves invalidas del fact
- muestra detalle de filas afectadas en log
- reescribe particiones historicas si traen ubicaciones inconsistentes

Mensajes esperados en log:

- `OK | sin pallets POSICION fuera de dim_ubicacion`
- o un warning con detalle de claves reclasificadas a `SIN_UBICACION`

## Dimensiones generadas

| Archivo | Contenido |
|---|---|
| `dim_cliente.parquet` | Clientes unicos |
| `dim_producto.parquet` | Codigo, producto, variedad, clasificacion, calidad, tipo_corte, presentacion |
| `dim_fecha.parquet` | Fecha, anio, mes, trimestre, semana y dia |
| `dim_ubicacion.parquet` | Posiciones estructurales configuradas |

## Integracion con Power BI

Power BI debe conectarse a:

```text
DW/
|-- fact_inventario/
|-- dim_cliente.parquet
|-- dim_producto.parquet
|-- dim_fecha.parquet
`-- dim_ubicacion.parquet
```

Analisis habilitados:

- pallets por camara
- ocupacion estructural
- capacidad vs ocupacion
- historico por fecha de corte
- inventario por producto, cliente o almacen

## Validaciones generales

El ETL tambien valida:

- columnas esperadas
- fechas validas
- valores negativos en campos numericos

Si encuentra negativos:

- el archivo no se procesa
- se registra `ERROR_NEGATIVOS`
- se deja trazabilidad en control y log

## Archivo de control

`PROCESADOS/control_procesados.csv` registra:

- archivo original
- fecha del archivo
- fecha de procesamiento
- estado
- archivo de salida
- observacion

Estados habituales:

- `PROCESADO`
- `ERROR`
- `ERROR_NEGATIVOS`

## Solucion de problemas

- `ModuleNotFoundError: pyarrow`: instalar dependencias.
- Archivo bloqueado al escribir Excel: cerrar el archivo abierto.
- No se detecta fecha: revisar `Fecha Actualizacion`.
- Archivo bloqueado por negativos: corregir el Excel original.
- `[SKIP] Ya procesado`: usar `--force` si deseas reprocesar.
- Visual `(En blanco)` en Power BI: ejecutar el ETL actualizado para sanear particiones historicas y refrescar el modelo.

## Resultado esperado

Con la version actual:

- no debe existir ningun pallet en `POSICION` sin match en `dim_ubicacion`
- debe desaparecer `(En blanco)` por mismatch de `ubicacion_key`
- el conteo de pallets por camara debe cuadrar con las posiciones estructurales validas
