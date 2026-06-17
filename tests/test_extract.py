import unittest

import pandas as pd

from modules.extract import validate_sku_catalog_consistency


class ExtractValidationTests(unittest.TestCase):
    def test_validate_sku_catalog_consistency_ok_con_descripcion_unica(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "CÓDIGO": "PT-PAL-019",
                    "PRODUCTO": "PALTA",
                    "PRESENTACIÓN": "IQF DICES 15X15 MM 20 BLS X 500GR CJ X 10KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "_SOURCE_ROW_NUM": 12,
                },
                {
                    "CÓDIGO": "PT-PAL-019",
                    "PRODUCTO": "PALTA",
                    "PRESENTACIÓN": "IQF DICES 15X15 MM 20 BLS X 500GR CJ X 10KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "_SOURCE_ROW_NUM": 13,
                },
            ]
        )

        ok, message = validate_sku_catalog_consistency(df, "demo.xlsx")

        self.assertTrue(ok)
        self.assertEqual(message, "")

    def test_validate_sku_catalog_consistency_advierte_sku_con_dos_presentaciones(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "CÓDIGO": "PT-PAL-019",
                    "PRODUCTO": "PALTA",
                    "PRESENTACIÓN": "IQF DICES 15X15 MM 20 BLS X 500GR CJ X 10KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "_SOURCE_ROW_NUM": 12,
                },
                {
                    "CÓDIGO": "PT-PAL-019",
                    "PRODUCTO": "PALTA",
                    "PRESENTACIÓN": "IQF DICES 15X15 MM 6 BLS X1 KG CAJA X6KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "_SOURCE_ROW_NUM": 18,
                },
            ]
        )

        ok, message = validate_sku_catalog_consistency(df, "demo.xlsx")

        self.assertTrue(ok)
        self.assertIn("[VALIDACION_SKU] demo.xlsx | advertencia", message)
        self.assertIn("PT-PAL-019", message)
        self.assertIn("20 BLS X 500GR", message)
        self.assertIn("6 BLS X1 KG", message)
        self.assertIn("filas_excel=12", message)
        self.assertIn("filas_excel=18", message)

    def test_validate_sku_catalog_consistency_bloquea_sku_con_producto_distinto(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "CÓDIGO": "PT-PAL-019",
                    "PRODUCTO": "PALTA",
                    "PRESENTACIÓN": "IQF DICES 15X15 MM 20 BLS X 500GR CJ X 10KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "_SOURCE_ROW_NUM": 12,
                },
                {
                    "CÓDIGO": "PT-PAL-019",
                    "PRODUCTO": "MANGO",
                    "PRESENTACIÓN": "IQF DICES 15X15 MM 20 BLS X 500GR CJ X 10KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "_SOURCE_ROW_NUM": 18,
                },
            ]
        )

        ok, message = validate_sku_catalog_consistency(df, "demo.xlsx")

        self.assertFalse(ok)
        self.assertIn("[VALIDACION_SKU] demo.xlsx | bloqueado", message)
        self.assertIn("PT-PAL-019", message)
        self.assertIn("PALTA", message)
        self.assertIn("MANGO", message)

    def test_validate_sku_catalog_consistency_advierte_conflicto_de_presentacion_con_historico(self) -> None:
        current_df = pd.DataFrame(
            [
                {
                    "CÓDIGO": "PT-FRE-039",
                    "PRODUCTO": "FRESA",
                    "PRESENTACIÓN": "ENTERA MEDIANA GRADO A IQF 25MM-35MM 6BLS X 05 LBS CJ X 13.61KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "_SOURCE_ROW_NUM": 25,
                },
            ]
        )
        historical_df = pd.DataFrame(
            [
                {
                    "CÓDIGO": "PT-FRE-039",
                    "PRODUCTO": "FRESA",
                    "PRESENTACIÓN": "ENTERA GRADO A IQF 24BLS X 1LB CJ X 24LBS",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "FECHA CORTE": "2026-03-20",
                },
            ]
        )

        ok, message = validate_sku_catalog_consistency(
            current_df,
            "demo.xlsx",
            historical_df=historical_df,
        )

        self.assertTrue(ok)
        self.assertIn("PT-FRE-039", message)
        self.assertIn("actual:", message)
        self.assertIn("historico:", message)
        self.assertIn("2026-03-20", message)


if __name__ == "__main__":
    unittest.main()
