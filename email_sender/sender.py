import os 
import requests


def send_email_via_api(project_root, **kwargs):
    url = "http://email_sender:5000/send"


    with open(os.path.join(project_root, "include/Body_email_messege.txt"), "r") as f:
        body_message = f.read()
    recipients = os.getenv("RECEIVER_EMAILS", "")
    receiver_emails = [email.strip() for email in recipients.split(",") if email.strip()]
    for receiver in receiver_emails:
        payload = {
            "name": os.getenv("NAME", "Air Flow User"),
            "email": os.getenv("EMAIL"),
            "subject": os.getenv("SUBJECT"),
            "message": body_message,
            "receiver_email": receiver
        }
        try:
            response = requests.post(url, json=payload, timeout=10)
            response.raise_for_status()
            print("Email sent successfully:", response.json())
        except requests.exceptions.RequestException as e:
            print("Failed to send email:", e)