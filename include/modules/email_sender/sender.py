import os 
import requests
from dotenv import load_dotenv
import json

load_dotenv()

def send_email_via_api(msg, **kwargs):
    url = "http://email_sender:5000/send"

    recipients = os.getenv("RECEIVER_EMAILS", "")
    receiver_emails = [email.strip() for email in recipients.split(",") if email.strip()]
    for receiver in receiver_emails:
        payload = {
            "name": os.getenv("EMAIL_NAME", "Air Flow User"),
            "email": os.getenv("SENDER_EMAIL"),
            "subject": os.getenv("EMAIL_SUBJ"),
            "message": msg,
            "receiver_email": receiver
        }
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            print("Email sent successfully:", response.json())
        except requests.exceptions.RequestException as e:
            print("Failed to send email:", e)

def task_failure_alert(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    logical_date = context.get('logical_date')
    msg = f"""
    Task Failed!
    dag_id: {dag_id}
    task_id: {task_id}
    logical_date: {logical_date}
    """

    send_email_via_api(msg)

