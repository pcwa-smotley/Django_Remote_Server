from django.core.mail import EmailMessage
from wx.settings import EMAIL_HOST_USER
import os, sys, platform

if "Linux" in platform.platform(terse=True):
    sys.path.append("/var/www/wx")
os.environ['DJANGO_SETTINGS_MODULE'] = 'wx.settings'

def send_mail(user_phone, user_email, email_text, email_subject):
    msg = EmailMessage(
        subject=email_subject,
        body=email_text,
        from_email=EMAIL_HOST_USER,
        bcc=[user_email, f"{user_phone}"],
    )
    #msg.attach_file(file_attachement1)
    msg.send(fail_silently=False)
    print("ALERT SENT")
    return


if __name__ == '__main__':
    send_mail(None, None)