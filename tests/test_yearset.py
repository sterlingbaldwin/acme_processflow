import os, sys
import unittest

if sys.path[0] != '.':
    sys.path.insert(0, os.path.abspath('.'))

from lib.YearSet import YearSet, SetStatus


class TestYearSet(unittest.TestCase):

    def test_year_set(self):
        ys = YearSet(
            set_number=1,
            start_year=1,
            end_year=10)
        self.assertTrue(ys.set_number == 1)
        self.assertTrue(ys.set_start_year == 1)
        self.assertTrue(ys.start_year == 1)
        self.assertTrue(ys.set_end_year == 10)
        self.assertTrue(ys.end_year == 10)
        self.assertTrue(ys.length == 10)
        self.assertEqual(ys.status, SetStatus.NO_DATA)

        ys.set_number = 99
        self.assertEqual(ys.set_number, 99)

        new_start_year = 101
        ys.set_start_year = new_start_year
        self.assertEqual(ys.set_start_year, new_start_year)

        new_end_year = 110
        ys.set_end_year = new_end_year
        self.assertEqual(ys.set_end_year, new_end_year)
        self.assertEqual(ys.length, new_end_year - new_start_year + 1)

        ys.status = SetStatus.COMPLETED
        self.assertEqual(ys.status, SetStatus.COMPLETED)

        for i in range(5):
            ys.add_job(i)
        for i in range(5):
            self.assertTrue(i in ys.jobs)
        
        test_string = str(ys)
        self.assertTrue('start_year: 1' in test_string)


if __name__ == '__main__':
    unittest.main()
