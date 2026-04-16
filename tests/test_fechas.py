import unittest

import pandas as pd

from modules.fechas import normalize_date_text, parse_datetime_series, parse_datetime_value


class FechasTests(unittest.TestCase):
    def test_normalize_date_text_zero_pads_day_and_month(self) -> None:
        self.assertEqual(normalize_date_text("2/9/2027"), "02/09/2027")
        self.assertEqual(normalize_date_text("2/09/2027 "), "02/09/2027")
        self.assertEqual(normalize_date_text("2/9/2027 8:05"), "02/09/2027 8:05")

    def test_parse_datetime_value_accepts_single_digit_day(self) -> None:
        parsed = parse_datetime_value("2/09/2027 ")
        self.assertEqual(parsed, pd.Timestamp("2027-09-02"))

    def test_parse_datetime_series_handles_mixed_date_formats(self) -> None:
        series = pd.Series(["29/03/2026 23:00", "2/09/2027 ", "14/02/2028"])
        parsed = parse_datetime_series(series)

        self.assertEqual(parsed.iloc[0], pd.Timestamp("2026-03-29 23:00:00"))
        self.assertEqual(parsed.iloc[1], pd.Timestamp("2027-09-02"))
        self.assertEqual(parsed.iloc[2], pd.Timestamp("2028-02-14"))


if __name__ == "__main__":
    unittest.main()
