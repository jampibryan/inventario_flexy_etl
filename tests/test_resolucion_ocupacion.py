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
                _base_row(CANTIDAD_CAJAS=76, **{"CANTIDAD CAJAS": 76, "_SOURCE_ROW_NUM": 2}),
                _base_row(CANTIDAD_CAJAS=24, **{"CANTIDAD CAJAS": 24, "_SOURCE_ROW_NUM": 3, "LOTE": "L2", "FECHA FABRICACIÓN": "2026-03-02"}),
            ]
        )

        clean_df, audit_df, summary = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)

        self.assertEqual(summary["logical_pallets"], 1)
        self.assertEqual(summary["consolidated_pallets"], 1)
        self.assertEqual(int(clean_df.iloc[0]["CANTIDAD CAJAS"]), 100)
        self.assertEqual(clean_df.iloc[0]["tipo_registro_resuelto"], "PALLET_LOGICO_CONSOLIDADO")
        self.assertEqual(len(audit_df), 2)

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

    def test_conflicto_si_no_hay_regla_compatible(self) -> None:
        df = pd.DataFrame(
            [
                _base_row(**{"POSICIÓN": 3, "CÓDIGO": "B", "PRODUCTO": "PALTA", "PRESENTACIÓN": "4 KG", "CANTIDAD CAJAS": 70, "_SOURCE_ROW_NUM": 6, "FECHA FABRICACIÓN": "2026-03-10"}),
                _base_row(**{"POSICIÓN": 3, "CÓDIGO": "C", "PRODUCTO": "FRESA", "PRESENTACIÓN": "2 KG", "CANTIDAD CAJAS": 30, "_SOURCE_ROW_NUM": 7, "FECHA FABRICACIÓN": "2026-03-11"}),
            ]
        )

        clean_df, audit_df, summary = resolve_location_occupancy(df, "demo.xlsx", self.valid_keys)

        self.assertEqual(summary["logical_pallets"], 1)
        self.assertEqual(summary["conflict_locations"], 1)
        self.assertEqual(clean_df.iloc[0]["tipo_registro_resuelto"], "CONFLICTO_RESUELTO_MAS_RECIENTE")
        self.assertIn("SIN_REGLA_COMPATIBILIDAD", audit_df["regla_compatibilidad_ubicacion"].tolist())
        self.assertIn("DESCARTADO_CONFLICTO", audit_df["tipo_registro_resuelto"].tolist())

    def test_sobrecapacidad_sale_de_fact_limpia(self) -> None:
        df = pd.DataFrame(
            [
                _base_row(**{"POSICIÓN": 4, "CÓDIGO": "B", "PRODUCTO": "PALTA", "PRESENTACIÓN": "4 KG", "CANTIDAD CAJAS": 80, "_SOURCE_ROW_NUM": 8}),
                _base_row(**{"POSICIÓN": 4, "CÓDIGO": "C", "PRODUCTO": "FRESA", "PRESENTACIÓN": "2 KG", "CANTIDAD CAJAS": 70, "_SOURCE_ROW_NUM": 9}),
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
