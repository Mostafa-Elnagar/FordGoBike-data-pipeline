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


def _summarize_airflow_dags(api_url='http://api-server:8080/api/v2/dags'):
    # try:
    response = requests.get(api_url)
    response.raise_for_status()
    dags = response.json().get('dags', [])

    summary = "Task Overview:\n"
    if not dags:
        summary += "No DAGs found.\n"
        return summary

    summary += f"{'DAG ID':<20} | {'Schedule':<20} | {'Is Paused':<10} | {'Next Run':<25} | {'Execution Date':<25} | {'Latest Run Status':<20}\n"
    summary += '-' * 135 + '\n'

    for dag in dags:
        dag_id = dag.get('dag_id')
        schedule = dag.get('timetable_summary') or 'Manual Trigger'
        is_paused = dag.get('is_paused')
        next_run = dag.get('next_dagrun_logical_date') or 'No future run'

        # Fetch latest DAG run
        dag_runs_url = f"http://api-server:8080/api/v2/dags/{dag_id}/dagRuns?order_by=-execution_date&limit=1"
        run_response = requests.get(dag_runs_url)
        if run_response.status_code == 200 and run_response.json().get('dag_runs'):
            latest_run = run_response.json()['dag_runs'][0]
            latest_status = latest_run.get('state', 'Unknown')
            latest_date = latest_run.get('execution_date', 'Unknown')
        else:
            latest_status = 'No runs yet'
            latest_date = 'No runs yet'

        summary += f"{dag_id:<20} | {schedule:<20} | {str(is_paused):<10} | {next_run:<25} | {latest_date:<25} | {latest_status:<20}\n"

    return summary

    # except requests.exceptions.RequestException as e:
    #     return f"Error fetching DAGs: {e}"

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
    summary = _summarize_airflow_dags()
    msg += f"\n\nDAGs Overview:\n{summary}"

    send_email_via_api(msg)

def email_dags_overview():
    summary = _summarize_airflow_dags()
    send_email_via_api(summary)

