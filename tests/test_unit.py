"""Unit tests — no API calls required."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from component import _batched


class TestBatched(unittest.TestCase):
    def test_exact_multiple(self):
        result = list(_batched(range(6), 3))
        self.assertEqual(result, [[0, 1, 2], [3, 4, 5]])

    def test_partial_last_batch(self):
        result = list(_batched(range(5), 3))
        self.assertEqual(result, [[0, 1, 2], [3, 4]])

    def test_empty_input(self):
        self.assertEqual(list(_batched([], 100)), [])

    def test_single_element(self):
        self.assertEqual(list(_batched([42], 10)), [[42]])

    def test_batch_larger_than_input(self):
        result = list(_batched(range(3), 100))
        self.assertEqual(result, [[0, 1, 2]])


if __name__ == "__main__":
    unittest.main()
