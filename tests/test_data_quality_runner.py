import unittest

import pandas as pd

from modules.data_quality.runner import (
    run_file_metadata_checks,
    run_input_quality_checks,
    run_transformed_quality_checks,
)


def _base_raw_row(**overrides: object) -> dict[str, object]:
    row = {
        "Fecha Actualización": "2026-04-28",
        "Empresa": "CLI",
        "Almacén": "CHAVIN CASMA DISPONIBLE",
        "Ubicación": "CÁMARA 01,1,1,1",
        "Código": "PT-PAL-019",
        "Cantidad": 10,
        "Presentación": 1,
        "Lote": "L1",
        "Fecha Caducidad": "2026-06-01",
        "Fecha Fabricación": "2026-03-01",
        "Producto": "PALTA HASS IQF DICES 15X15 MM 20 BLS X 500GR CJ X 10KG",
    }
    row.update(overrides)
    return row


class DataQualityRunnerTests(unittest.TestCase):
    def test_run_file_metadata_checks_detecta_faltantes(self) -> None:
        df = pd.DataFrame([{"Empresa": "CLI"}])

        result = run_file_metadata_checks(df)

        self.assertFalse(result.ok)
        self.assertEqual(result.blocking_issue.status_code, "ERROR_COLUMNAS")



    def test_run_file_metadata_checks_extrae_fecha_content(self) -> None:
        df = pd.DataFrame([_base_raw_row()])
        result = run_file_metadata_checks(df)
        self.assertTrue(result.ok)
        self.assertEqual(result.file_date, "2026-04-27") # Desplazado por -1 desde 2026-04-28

    def test_run_file_metadata_checks_extrae_fecha_filename(self) -> None:
        df = pd.DataFrame([_base_raw_row()])
        result = run_file_metadata_checks(df, filename="2026-05-10_Stock a la Fecha.xlsx")
        self.assertTrue(result.ok)
        self.assertEqual(result.file_date, "2026-05-09") # Desplazado por -1 desde 2026-05-10

    def test_run_input_quality_checks_conserva_warning_y_dataframe_filtrado(self) -> None:
        df = pd.DataFrame(
            [
                _base_raw_row(**{"Cantidad": 10, "Presentación": 5, "Ubicación": "CÁMARA 01,1,1,1"}),
                _base_raw_row(**{"Cantidad": -10, "Presentación": -5, "Ubicación": "CÁMARA 01,1,1,1"}),
            ]
        )

        result = run_input_quality_checks(df, "demo.xlsx")

        self.assertTrue(result.ok)
        self.assertEqual(len(result.warning_messages), 1)
        self.assertEqual(len(result.dataframe), 0)

    def test_run_transformed_quality_checks_advierte_presentacion_distinta_en_historico(self) -> None:
        current_df = pd.DataFrame(
            [
                {
                    "CÓDIGO": "PT-FRE-039",
                    "PRODUCTO": "FRESA",
                    "PRESENTACIÓN": "ENTERA MEDIANA GRADO A IQF 25MM-35MM 6BLS X 05 LBS CJ X 13.61KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "_SOURCE_ROW_NUM": 25,
                }
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
                }
            ]
        )

        result = run_transformed_quality_checks(
            current_df,
            "demo.xlsx",
            historical_df=historical_df,
        )

        self.assertTrue(result.ok)
        self.assertEqual(len(result.warning_messages), 1)
        self.assertIn("PT-FRE-039", result.warning_messages[0])

    def test_run_transformed_quality_checks_bloquea_producto_distinto(self) -> None:
        current_df = pd.DataFrame(
            [
                {
                    "CÓDIGO": "PT-FRE-039",
                    "PRODUCTO": "FRESA",
                    "PRESENTACIÓN": "ENTERA MEDIANA GRADO A IQF 25MM-35MM 6BLS X 05 LBS CJ X 13.61KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "_SOURCE_ROW_NUM": 25,
                }
            ]
        )
        historical_df = pd.DataFrame(
            [
                {
                    "CÓDIGO": "PT-FRE-039",
                    "PRODUCTO": "FRAMBUESA",
                    "PRESENTACIÓN": "ENTERA MEDIANA GRADO A IQF 25MM-35MM 6BLS X 05 LBS CJ X 13.61KG",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                    "FECHA CORTE": "2026-03-20",
                }
            ]
        )

        result = run_transformed_quality_checks(
            current_df,
            "demo.xlsx",
            historical_df=historical_df,
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.blocking_issue.status_code, "ERROR_SKU_CATALOGO")
        self.assertIn("PT-FRE-039", result.blocking_issue.message)
        self.assertIn("FRAMBUESA", result.blocking_issue.message)

    def test_run_transformed_quality_checks_advierte_sku_no_catalogado(self) -> None:
        current_df = pd.DataFrame(
            [
                {
                    "CÓDIGO": "PT-PAL-999",
                    "PRODUCTO": "PALTA",
                    "PRESENTACIÓN": "DESCONOCIDO",
                    "CLASIFICACIÓN": "CONVENCIONAL",
                }
            ]
        )
        catalog_lookup = {
            "PT-PAL-001": {"producto": "PALTA", "peso": 10.0}
        }
        result = run_transformed_quality_checks(
            current_df,
            "demo.xlsx",
            catalog_lookup=catalog_lookup,
        )
        self.assertTrue(result.ok)
        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].status_code, "WARN_SKU_NO_CATALOGADO")
        self.assertIn("PT-PAL-999", result.issues[0].message)


if __name__ == "__main__":
    unittest.main()
