from YearSet import YearSet
import unittest


class TestYearSet(unittest.TestCase):

    def test_year_set(self):
        ys = YearSet(
            set_number=1,
            start_year=1,
            end_year=10)
        self.assertTrue(ys.set_number == 1)
        self.assertTrue(ys.set_start_year == 1)
        self.assertTrue(ys.set_end_year == 10)
        self.assertTrue(ys.length == 10)

if __name__ == '__main__':
    unittest.main()
