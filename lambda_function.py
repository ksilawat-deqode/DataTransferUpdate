import json
import os
from time import sleep

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor


def lambda_handler(event, context):
    connection = psycopg2.connect(
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT"),
        database=os.environ.get("DB_NAME"),
    )

    cursor = connection.cursor(cursor_factory=RealDictCursor)

    secrets_client = boto3.client("secretsmanager")
    polling_function_arn = json.loads(
        secrets_client.get_secret_value(
            SecretId=os.environ.get("SECRETS"),
        ).get("SecretString")
    ).get("POLLING_FUNCTION_ARN")

    data_sync_client = \
        boto3.client("datasync", region_name=event["region_name"])
    status = data_sync_client.describe_task_execution(
        TaskExecutionArn=event["task_execution_arn"],
    ).get("Status")

    data_transfer_state_update_query = "UPDATE emr_job_details SET " \
                                       "data_transfer_state=%s WHERE id=%s"

    cursor.execute(
        data_transfer_state_update_query,
        (status, event["id"])
    )

    print(
        f"""{event["id"]}-> Polling jobstatus: {status}"""
    )
    if status in ("SUCCESS", "ERROR"):
        status = status if status != "ERROR" else "FAILED"

        print(
            f"""{event["id"]}-> Updating jobstatus: {status}"""
        )
        update_query = "UPDATE emr_job_details SET jobstatus=%s WHERE id=%s"
        cursor.execute(
            update_query,
            (status, event["id"])
        )
        connection.commit()
        cursor.close()
        return

    sleep_interval = int(os.environ.get("POLLING_INTERVAL"))

    print(f""""{event["id"]}-> Sleeping for {sleep_interval} seconds""")
    sleep(sleep_interval)

    payload = {
        "id": event["id"],
        "region_name": event["region_name"],
        "task_execution_arn": event["task_execution_arn"],
    }
    payload_bytes = json.dumps(payload).encode('utf-8')

    print(f"""{event["id"]}-> Performing recursive invocation""")
    boto3.client("lambda").invoke_async(
        FunctionName=polling_function_arn,
        InvokeArgs=payload_bytes,
    )
