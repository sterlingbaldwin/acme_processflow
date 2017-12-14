import unittest
from lib.mailer import Mailer


class TestMailer(unittest.TestCase):

    def test_send_mail_valid(self):
        m = Mailer(
            src='baldwin32@llnl.gov',
            dst='baldwin32@llnl.gov')
        ret = m.send(
            status='THIS IS A TEST',
            msg='THIS IS ONLY A TEST')
        self.assertTrue(ret)

    def test_send_mail_invalid(self):
        m = Mailer(
            src='xxyyzz',
            dst='xxyyzz')
        ret = m.send(
            status='THIS IS A TEST',
            msg='THIS IS ONLY A TEST')
        self.assertFalse(ret)


if __name__ == '__main__':
    unittest.main()
