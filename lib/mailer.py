import smtplib
import logging
from email.mime.text import MIMEText
from validate_email import validate_email

class Mailer(object):
    """
    A simple class for sending email
    """
    def __init__(self, src, dst):
        """
        Initialize the mailer with source = src and destination = dst

        Parameters:
            src (str): the source email address
            dst (str): the destination email address
        """
        self.src = src
        self.dst = dst
        self.smtp = None
        self.default_message = 'Your post processsing job has completed successfully'
        self.default_status = 'SUCCEESS'

    def validate(self):
        """
        Validate the source and destination addresses

        returns True if valid, False otherwise
        """
        if not validate_email(self.dst, check_mx=True, verify=True):
            msg = 'Invalid destination email address {}'.format(self.dst)
            logging.error(msg)
            return False
        elif not validate_email(self.src, check_mx=True, verify=True):
            msg = 'Invalid source email address {}'.format(self.src)
            logging.error(msg)
            return False
        else:
            return True

    def send(self, status=None, msg=None):
        """
        Send the email with contents = msg and subject line = status

        Parameters:
            msg (str): the contents of the email
            status (str): the subject line of the email

        returns True if succesful, False otherwise
        """
        if not self.validate():
            return False
        if not msg:
            msg = self.default_message
        if not status:
            status = self.default_status
        self.smtp = smtplib.SMTP('localhost')
        message = MIMEText(msg)
        message['Subject'] = status
        message['From'] = self.src
        message['To'] = self.dst

        self.smtp.sendmail(self.src, self.dst, message.as_string())
        self.smtp.quit()
        return True
