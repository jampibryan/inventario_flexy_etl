# Medidas Power BI

Usa `fact_inventario` como tabla principal.

## Medidas base

```DAX
Pallets Logicos =
SUM ( fact_inventario[pallets] )
```

```DAX
Ubicaciones Ocupadas =
SUM ( fact_inventario[ubicacion_ocupada_flag] )
```

```DAX
Pallets Consolidados =
SUM ( fact_inventario[pallet_consolidado_flag] )
```

```DAX
Ubicaciones con Conflicto =
CALCULATE (
    DISTINCTCOUNT ( fact_inventario[ubicacion_key] ),
    fact_inventario[conflicto_flag] = 1
)
```

```DAX
Ubicaciones con Sobrecapacidad =
CALCULATE (
    DISTINCTCOUNT ( fact_inventario_auditoria[ubicacion_key_candidata] ),
    fact_inventario_auditoria[sobrecapacidad_flag] = 1
)
```

```DAX
Cajas Inventario Limpio =
SUM ( fact_inventario[CANTIDAD CAJAS] )
```

## Recomendaciones

- Usa `fact_inventario_auditoria` para auditoria y calidad de dato.
- No cuentes filas del Excel como pallets.
- Para ocupacion estructural usa `Ubicaciones Ocupadas` contra `dim_ubicacion`.
- Para conflicto y sobrecapacidad usa la auditoria o los flags de la fact limpia segun el analisis.
