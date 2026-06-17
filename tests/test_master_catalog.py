import unittest
import pandas as pd
from modules.master_catalog import get_master_catalog_lookup
from modules.transform import transform_inventory

class MasterCatalogTests(unittest.TestCase):
    def test_get_master_catalog_lookup_converts_correctly(self) -> None:
        raw_df = pd.DataFrame([
            {
                "TIPO DE PRODUCTO": "PT-PAL-001",
                "MATERIA PRIMA": "PALTA",
                "Peso x UM": 10.0,
                "Descripcion Corta": "PALTA IQF SLICES CJ x 10KG",
                "Descripcion Larga": "Cajas de palta congelada / 10kg",
                "UM": "CAJ"
            },
            {
                "TIPO DE PRODUCTO": "PT-MGO-002",
                "MATERIA PRIMA": "MANGO",
                "Peso x UM": 12.5,
                "Descripcion Corta": "MANGO IQF DICES CJ x 12.5KG",
                "UM": "CAJ"
            }
        ])
        
        lookup = get_master_catalog_lookup(raw_df)
        self.assertEqual(len(lookup), 2)
        self.assertIn("PT-PAL-001", lookup)
        self.assertEqual(lookup["PT-PAL-001"]["producto"], "PALTA")
        self.assertEqual(lookup["PT-PAL-001"]["peso"], 10.0)
        self.assertEqual(lookup["PT-MGO-002"]["peso"], 12.5)

    def test_transform_inventory_maps_catalog_properties(self) -> None:
        # Simular DataFrame de entrada diario que coincide con EXPECTED_COLUMNS
        df_raw = pd.DataFrame([
            {
                "Fecha Actualización": "2026-04-28",
                "Empresa": "CLIENTE_A",
                "Almacén": "CHAVIN CASMA DISPONIBLE",
                "Ubicación": "CÁMARA 01,1,1,1",
                "Código": "PT-PAL-001",
                "Cantidad": 10,
                "Presentación": 1.0,
                "Lote": "LOTE1",
                "Fecha Caducidad": "2026-06-01",
                "Fecha Fabricación": "2026-03-01",
                "Producto": "TEXTO_DE_PRODUCTO_RAW",
            },
            {
                "Fecha Actualización": "2026-04-28",
                "Empresa": "CLIENTE_B",
                "Almacén": "CHAVIN CASMA DISPONIBLE",
                "Ubicación": "CÁMARA 01,1,1,2",
                "Código": "", # Código vacío, debería mapearse a SIN_SKU
                "Cantidad": 10,
                "Presentación": 1.0,
                "Lote": "LOTE2",
                "Fecha Caducidad": "2026-06-01",
                "Fecha Fabricación": "2026-03-01",
                "Producto": "TEXTO_PRODUCTO_SIN_SKU",
            }
        ])
        
        catalog_lookup = {
            "PT-PAL-001": {
                "producto": "PALTA",
                "peso": 8.4, # Peso diferente al del Excel diario
                "descripcion_corta": "PALTA CONGELADA HASS",
                "descripcion_larga": "PALTA HASS",
                "um": "CAJ"
            }
        }
        
        df_clean = transform_inventory(df_raw, "2026-04-28", catalog_lookup=catalog_lookup)
        
        # Verificar las propiedades mapeadas de la primera fila desde el catálogo
        row_0 = df_clean.iloc[0]
        self.assertEqual(row_0["CÓDIGO"], "PT-PAL-001")
        self.assertEqual(row_0["PRODUCTO"], "PALTA")
        self.assertEqual(row_0["TONELADAS"], 0.08) # 10 * 8.4 / 1000 = 0.084 -> redondeo a 2 decimales = 0.08

        # Verificar que la segunda fila (SKU vacío) se mapee a SIN_SKU
        row_1 = df_clean.iloc[1]
        self.assertEqual(row_1["CÓDIGO"], "SIN_SKU")
        self.assertEqual(row_1["PRODUCTO"], "PRODUCTO SIN CLASIFICAR")
        self.assertEqual(row_1["TONELADAS"], 0.01) # Cálculo: 10 * 1.0 / 1000 = 0.01

if __name__ == "__main__":
    unittest.main()
