import os 
import requests
from dotenv import load_dotenv
import json

load_dotenv()

MODE = "deploy"

match MODE:
    case "dev":
        DAGS_ENDPOINT = 'http://localhost:8080/api/v2/dags'
        EMAIL_ENDPOINT = "http://localhost:5000/send"
    case "deploy":
        DAGS_ENDPOINT = 'http://api-server:8080/api/v2/dags'
        EMAIL_ENDPOINT = "http://email_sender:5000/send"

def send_email_via_api(msg, **kwargs):
    url = EMAIL_ENDPOINT

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


def _summarize_airflow_dags(api_url=DAGS_ENDPOINT):
    # try:
    response = requests.get(api_url)
    response.raise_for_status()
    dags = response.json().get('dags', [])

    summary = "Task Overview:\n"
    if not dags:
        summary += "No DAGs found.\n"
        return summary

    summary += f"{'DAG ID':<20} | {'Schedule':<20} | {'Is Paused':<10} | {'Next Run':<25} | {'Execution Start Date':<30} | {'Execution End Date':<30} | {'Status':<15}\n"
    summary += '-' * 165 + '\n'

    for dag in dags:
        dag_id = dag.get('dag_id')
        if dag_id != "report_dag":
            schedule = dag.get('timetable_summary') or 'Manual Trigger'
            is_paused = dag.get('is_paused')
            next_run = dag.get('next_dagrun_logical_date') or 'No future run'

            # Fetch latest DAG run https://airflow.apache.org/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}
            dag_runs_url = f"{DAGS_ENDPOINT}/{dag_id}/dagRuns"
            run_response = requests.get(dag_runs_url)
            
            if run_response.status_code == 200 and run_response.json().get('dag_runs'):
                latest_dag_run = run_response.json()["dag_runs"][-1]
                latest_status = latest_dag_run.get('state', 'Unknown')
                latest_start_date = latest_dag_run.get('start_date', 'Unknown')
                latest_end_date = latest_dag_run.get('end_date', 'Unknown')
            else:
                latest_status = 'No runs yet'
                latest_start_date = 'No runs yet'
                latest_end_date = 'No runs yet'

            summary += f"{dag_id:<20} | {schedule:<20} | {str(is_paused):<10} | {next_run:<25} | {latest_start_date:<30} | {latest_end_date:<30} | {latest_status:<15}\n"

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

if __name__ == "__main__":
    email_dags_overview()