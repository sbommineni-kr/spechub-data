"""
Script Name: dl_email.py
Arguments: None
Description: Datalake Email module
Created: Dec 01, 2024
Author: Sudheer B
Version: 1.0
"""
from .dl_logger import DLLogger

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import os 

dl_log = DLLogger(__name__)

#override if environment var is passed 
if 'EMAIL_PASSWORD' in os.environ.keys():
    password = os.environ['EMAIL_PASSWORD']
else:
    password = None

class DLEmail:
    def __init__(self, user, password=password, smtp_server='smtp-mail.outlook.com', smtp_port=587):
        self.user = user
        self.password = password
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port

    def send_email(self, from_address, to_address, subject, body, attachment_path=None):
        msg = MIMEMultipart()
        msg['From'] = from_address
        msg['To'] = ", ".join(to_address)
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        if attachment_path:
            attachment = open(attachment_path, "rb")
            part = MIMEBase('application', 'octet-stream')
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', "attachment; filename= " + attachment_path)
            msg.attach(part)

        server = smtplib.SMTP(self.smtp_server, self.smtp_port)
        server.starttls()
        server.login(self.user, self.password)
        #print all email config before sending email , print from server object 
        dl_log.info(f"Sending email from {from_address} to {msg['To']} with subject {subject} and body {body}")
        server.sendmail(from_address, to_address, msg.as_string())
        dl_log.info("Email sent successfully")
        server.quit()