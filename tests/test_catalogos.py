import unittest

from config import BOX_CAPACITY_RULES, MULTIPALLET_COMPATIBILITY_RULES


class CatalogosTests(unittest.TestCase):
    def test_box_capacity_rules_cargadas(self) -> None:
        self.assertGreater(len(BOX_CAPACITY_RULES), 0)
        for rule in BOX_CAPACITY_RULES:
            self.assertIn("max_boxes", rule)
            self.assertIn("rule_name", rule)

    def test_multipallet_rules_cargadas(self) -> None:
        self.assertGreater(len(MULTIPALLET_COMPATIBILITY_RULES), 0)
        for rule in MULTIPALLET_COMPATIBILITY_RULES:
            self.assertIn("rule_name", rule)
            self.assertIn("max_logical_pallets", rule)
            self.assertIn("max_total_boxes", rule)


if __name__ == "__main__":
    unittest.main()
