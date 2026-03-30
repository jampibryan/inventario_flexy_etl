import shutil
import unittest
from pathlib import Path

import pandas as pd

from modules.dimensiones import build_dim_fecha
from modules.historico import build_fact_actual, build_snapshot_control
from modules.parquet_io import save_parquet


class HistoricoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base_dir = Path("tests_tmp_historico")
        self.fact_dir = self.base_dir / "fact_inventario"
        self.audit_dir = self.base_dir / "fact_inventario_auditoria"
        self.fact_dir.mkdir(parents=True, exist_ok=True)
        self.audit_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        if self.base_dir.exists():
            shutil.rmtree(self.base_dir)

    def test_dim_fecha_construye_calendario_continuo_y_flags_snapshot(self) -> None:
        fact_df = pd.DataFrame({"FECHA CORTE": ["2026-03-20", "2026-03-22"]})

        dim_fecha = build_dim_fecha(fact_df)

        self.assertEqual(len(dim_fecha), 3)
        self.assertEqual(dim_fecha["es_fecha_snapshot_flag"].tolist(), [1, 0, 1])
        self.assertEqual(dim_fecha["es_ultimo_snapshot_flag"].tolist(), [0, 0, 1])

    def test_build_fact_actual_se_queda_con_ultimo_snapshot(self) -> None:
        fact_df = pd.DataFrame(
            {
                "FECHA CORTE": ["2026-03-20", "2026-03-20", "2026-03-21"],
                "pallets": [1, 1, 1],
            }
        )

        fact_actual = build_fact_actual(fact_df)

        self.assertEqual(len(fact_actual), 1)
        self.assertEqual(str(fact_actual.iloc[0]["FECHA CORTE"]), "2026-03-21")
        self.assertEqual(int(fact_actual.iloc[0]["es_ultimo_snapshot_flag"]), 1)

    def test_snapshot_control_resume_metricas_e_integridad(self) -> None:
        fact_df = pd.DataFrame(
            {
                "FECHA CORTE": ["2026-03-20", "2026-03-20"],
                "pallets": [1, 1],
                "ubicacion_ocupada_flag": [1, 1],
                "ubicacion_key": ["CAM01-R001-N01-P01", "CAM01-R001-N01-P02"],
                "TONELADAS": [1.25, 1.75],
                "cliente_key": ["A", "B"],
                "producto_key": ["X", "Y"],
                "source_file": ["demo.xlsx", "demo.xlsx"],
                "snapshot_row_id": ["id-1", "id-2"],
            }
        )
        audit_df = pd.DataFrame(
            {
                "FECHA CORTE": ["2026-03-20", "2026-03-20"],
                "conflicto_flag": [1, 0],
                "sobrecapacidad_flag": [0, 1],
                "registro_vigente_flag": [1, 0],
            }
        )

        fact_path = self.fact_dir / "fecha_corte=2026-03-20" / "data.parquet"
        audit_path = self.audit_dir / "fecha_corte=2026-03-20" / "data.parquet"
        save_parquet(fact_df, fact_path)
        save_parquet(audit_df, audit_path)

        snapshot_control = build_snapshot_control(self.fact_dir, self.audit_dir)

        self.assertEqual(len(snapshot_control), 1)
        row = snapshot_control.iloc[0]
        self.assertEqual(str(row["fecha_corte"]), "2026-03-20")
        self.assertEqual(int(row["fact_rows"]), 2)
        self.assertEqual(int(row["pallets_logicos"]), 2)
        self.assertEqual(int(row["ubicaciones_ocupadas"]), 2)
        self.assertEqual(int(row["ubicaciones_distintas"]), 2)
        self.assertEqual(float(row["toneladas_total"]), 3.0)
        self.assertEqual(int(row["audit_rows"]), 2)
        self.assertEqual(int(row["conflictos_auditoria"]), 1)
        self.assertEqual(int(row["sobrecapacidad_auditoria"]), 1)
        self.assertEqual(int(row["descartados_auditoria"]), 1)
        self.assertEqual(int(row["partition_integrity_ok"]), 1)
        self.assertEqual(int(row["es_ultimo_snapshot_flag"]), 1)


if __name__ == "__main__":
    unittest.main()
