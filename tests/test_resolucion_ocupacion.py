import unittest

import pandas as pd

from modules.dimensiones import build_dim_producto, build_dim_ubicacion
from modules.resolucion_ocupacion import resolve_location_occupancy
from modules.snapshot import build_fact_snapshot


def _base_row(**overrides: object) -> dict[str, object]:
    row = {
        "FECHA CORTE": "2026-03-17",
        "CLIENTE": "CLI",
        "ALMACÉN": "CHAVIN",
        "ESTADO PRODUCTO": "DISPONIBLE",
        "CÁMARA": "CÁMARA 01",
        "RACK": 1,
        "NIVEL": 1,
        "POSICIÓN": 1,
        "CÓDIGO": "A",
        "CANTIDAD CAJAS": 10,
        "TONELADAS": 0.10,
        "LOTE": "L1",
        "FECHA FABRICACIÓN": "2026-03-01",
        "FECHA CADUCIDAD": "2026-06-01",
        "PRODUCTO": "MANGO",
        "VARIEDAD": "EDWARD",
        "CLASIFICACIÓN": "CONVENCIONAL",
        "CALIDAD": None,
        "TIPO DE CORTE": None,
        "PRESENTACIÓN": "EDWARD 10 KG",
        "_SOURCE_ROW_NUM": 2,
    }
    row.update(overrides)
    return row


class ResolucionOcupacionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.valid_keys = set(build_dim_ubicacion()["ubicacion_key"])

    def test_consolida_mismo_producto_en_un_pallet_logico(self) -> None:
        df = pd.DataFrame(
            [
                _base_row(**{"CANTIDAD CAJAS": 76, "_SOURCE_ROW_NUM": 2}),
                _base_row(**{"CANTIDAD CAJAS": 24, "_SOURCE_ROW_NUM": 3}),
            ]
        )

        clean_df, audit_df, summary = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)

        self.assertEqual(summary["logical_pallets"], 1)
        self.assertEqual(summary["consolidated_pallets"], 1)
        self.assertEqual(int(clean_df.iloc[0]["CANTIDAD CAJAS"]), 100)
        self.assertEqual(clean_df.iloc[0]["tipo_registro_resuelto"], "PALLET_LOGICO_CONSOLIDADO")
        self.assertEqual(len(audit_df), 2)

    def test_misma_presentacion_y_fechas_cercanas_se_trata_como_un_pallet(self) -> None:
        df = pd.DataFrame(
            [
                _base_row(**{"POSICIÓN": 6, "CÓDIGO": "PT-MGO-049", "CANTIDAD CAJAS": 69, "_SOURCE_ROW_NUM": 11, "LOTE": "030126", "FECHA FABRICACIÓN": "2026-02-03", "PRESENTACIÓN": "MANGO KENT DEFORME IQF 20X20 MM A GRANEL CJ X 10KG"}),
                _base_row(**{"POSICIÓN": 6, "CÓDIGO": "PT-MGO-049", "CANTIDAD CAJAS": 51, "_SOURCE_ROW_NUM": 12, "LOTE": "03026", "FECHA FABRICACIÓN": "2026-01-30", "PRESENTACIÓN": "MANGO KENT DEFORME IQF 20X20 MM A GRANEL CJ X 10KG"}),
            ]
        )

        clean_df, _, summary = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)

        self.assertEqual(summary["logical_pallets"], 1)
        self.assertEqual(summary["consolidated_pallets"], 1)
        self.assertEqual(int(clean_df.iloc[0]["CANTIDAD CAJAS"]), 120)
        self.assertEqual(clean_df.iloc[0]["tipo_registro_resuelto"], "PALLET_REINGRESO_CONSOLIDADO")

    def test_misma_presentacion_con_fechas_lejanas_conserva_solo_el_mas_actual(self) -> None:
        df = pd.DataFrame(
            [
                _base_row(**{"POSICIÓN": 7, "CÓDIGO": "PT-MGO-049", "CANTIDAD CAJAS": 50, "_SOURCE_ROW_NUM": 13, "LOTE": "L2025", "FECHA FABRICACIÓN": "2025-02-03", "PRESENTACIÓN": "MANGO KENT DEFORME IQF 20X20 MM A GRANEL CJ X 10KG"}),
                _base_row(**{"POSICIÓN": 7, "CÓDIGO": "PT-MGO-049", "CANTIDAD CAJAS": 40, "_SOURCE_ROW_NUM": 14, "LOTE": "L2026", "FECHA FABRICACIÓN": "2026-01-30", "PRESENTACIÓN": "MANGO KENT DEFORME IQF 20X20 MM A GRANEL CJ X 10KG"}),
            ]
        )

        clean_df, audit_df, summary = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)

        self.assertEqual(summary["logical_pallets"], 1)
        self.assertEqual(clean_df.iloc[0]["LOTE"], "L2026")
        self.assertIn("DESCARTADO_CONFLICTO", audit_df["tipo_registro_resuelto"].tolist())

    def test_multipallet_valido_con_regla_explicita(self) -> None:
        df = pd.DataFrame(
            [
                _base_row(**{"POSICIÓN": 2, "CÓDIGO": "B", "PRODUCTO": "PALTA", "PRESENTACIÓN": "4 KG", "CANTIDAD CAJAS": 60, "_SOURCE_ROW_NUM": 4}),
                _base_row(**{"POSICIÓN": 2, "CÓDIGO": "C", "PRODUCTO": "FRESA", "PRESENTACIÓN": "2 KG", "CANTIDAD CAJAS": 30, "_SOURCE_ROW_NUM": 5}),
            ]
        )

        clean_df, _, summary = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)

        self.assertEqual(summary["logical_pallets"], 2)
        self.assertEqual(summary["multipallet_locations"], 1)
        self.assertTrue((clean_df["tipo_registro_resuelto"] == "MULTIPALLET_VALIDO").all())
        self.assertEqual(clean_df["regla_compatibilidad_ubicacion"].nunique(), 1)
        self.assertEqual(clean_df["regla_compatibilidad_ubicacion"].iloc[0], "SMALL_PALLETS_GENERIC")

    def test_coexistencia_valida_si_no_excede_capacidad(self) -> None:
        df = pd.DataFrame(
            [
                _base_row(**{"POSICIÓN": 3, "CÓDIGO": "B", "PRODUCTO": "PALTA", "PRESENTACIÓN": "4 KG", "CANTIDAD CAJAS": 70, "_SOURCE_ROW_NUM": 6, "FECHA FABRICACIÓN": "2026-03-10"}),
                _base_row(**{"POSICIÓN": 3, "CÓDIGO": "C", "PRODUCTO": "FRESA", "PRESENTACIÓN": "2 KG", "CANTIDAD CAJAS": 30, "_SOURCE_ROW_NUM": 7, "FECHA FABRICACIÓN": "2026-03-11"}),
            ]
        )

        clean_df, audit_df, summary = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)

        self.assertEqual(summary["logical_pallets"], 2)
        self.assertEqual(summary["multipallet_locations"], 1)
        self.assertTrue((clean_df["tipo_registro_resuelto"] == "MULTIPALLET_VALIDO").all())
        self.assertIn("CAPACIDAD_UBICACION", audit_df["regla_compatibilidad_ubicacion"].tolist())

    def test_si_excede_capacidad_se_conserva_el_mas_reciente(self) -> None:
        df = pd.DataFrame(
            [
                _base_row(**{"POSICIÓN": 4, "CÓDIGO": "B", "PRODUCTO": "PALTA", "PRESENTACIÓN": "4 KG", "CANTIDAD CAJAS": 80, "_SOURCE_ROW_NUM": 8, "FECHA FABRICACIÓN": "2026-03-10"}),
                _base_row(**{"POSICIÓN": 4, "CÓDIGO": "C", "PRODUCTO": "FRESA", "PRESENTACIÓN": "2 KG", "CANTIDAD CAJAS": 70, "_SOURCE_ROW_NUM": 9, "FECHA FABRICACIÓN": "2026-03-11"}),
            ]
        )

        clean_df, audit_df, summary = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)

        self.assertEqual(summary["logical_pallets"], 1)
        self.assertEqual(summary["conflict_locations"], 1)
        self.assertEqual(clean_df.iloc[0]["tipo_registro_resuelto"], "CONFLICTO_RESUELTO_MAS_RECIENTE")
        self.assertEqual(clean_df.iloc[0]["CÓDIGO"], "C")
        self.assertIn("DESCARTADO_CONFLICTO", audit_df["tipo_registro_resuelto"].tolist())

    def test_sobrecapacidad_unitaria_sale_de_fact_limpia(self) -> None:
        df = pd.DataFrame(
            [
                _base_row(**{"POSICIÓN": 5, "CÓDIGO": "B", "PRODUCTO": "PALTA", "PRESENTACIÓN": "4 KG", "CANTIDAD CAJAS": 130, "_SOURCE_ROW_NUM": 10}),
            ]
        )

        clean_df, audit_df, summary = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)

        self.assertTrue(clean_df.empty)
        self.assertEqual(summary["overcapacity_locations"], 1)
        self.assertTrue((audit_df["tipo_registro_resuelto"] == "ERROR_SOBRECAPACIDAD").all())

    def test_snapshot_y_dim_producto_exponen_campos_nuevos(self) -> None:
        df = pd.DataFrame([_base_row(**{"CANTIDAD CAJAS": 50})])
        clean_df, _, _ = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)
        fact_df, _ = build_fact_snapshot(clean_df, "demo.xlsx", self.valid_keys)
        dim_producto = build_dim_producto(fact_df)

        self.assertIn("pallet_logico_id", fact_df.columns)
        self.assertIn("ubicacion_ocupada_flag", fact_df.columns)
        self.assertIn("max_cajas_configuradas", dim_producto.columns)
        self.assertIn("regla_capacidad", dim_producto.columns)


if __name__ == "__main__":
    unittest.main()
