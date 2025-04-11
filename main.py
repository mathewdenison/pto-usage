import os
import json
import logging
import base64

from google.cloud import pubsub_v1
from google.cloud import logging as cloud_logging

# Import shared code from your PyPI package.
# Note: The package name must use underscores. For example, if your distribution is "pto-common"
# then your import should be "pto_common_timesheet_mfdenison_hopkinsep".
from pto_common_timesheet_mfdenison_hopkinsep.models import PTO
from pto_common_timesheet_mfdenison_hopkinsep.utils.dashboard_events import build_dashboard_payload

# Initialize Google Cloud Logging
cloud_log_client = cloud_logging.Client()
cloud_log_client.setup_logging()

# Standard logger configuration
logger = logging.getLogger("pto_deduction_handler")
logger.setLevel(logging.INFO)

# Pub/Sub dashboard topic configuration
PROJECT_ID = os.environ.get("PROJECT_ID", "hopkinstimesheetproj")
DASHBOARD_TOPIC = f"projects/{PROJECT_ID}/topics/dashboard-queue"
publisher = pubsub_v1.PublisherClient()

def pto_deduction_handler(event, context):
    """
    Cloud Function triggered by a Pub/Sub message containing a PTO deduction request.

    Expected message payload (JSON):
      {
        "employee_id": <int>,
        "pto_hours": <int>
      }

    The function:
      - Decodes the base64-encoded Pub/Sub message.
      - Parses the JSON payload (double-parsing if needed).
      - Retrieves (or creates) the PTO record for the given employee_id.
      - If there isn’t enough balance, builds an error dashboard payload.
      - Otherwise, deducts the PTO hours, saves the record, and builds a refresh dashboard payload.
      - Publishes the dashboard payload to the dashboard Pub/Sub topic.
    """
    try:
        # Decode the Pub/Sub message.
        raw_data = base64.b64decode(event["data"]).decode("utf-8")
        logger.info(f"Raw message received: {raw_data}")

        # Decode the JSON payload. If the payload is a string, decode it again.
        data = json.loads(raw_data)
        if isinstance(data, str):
            data = json.loads(data)
        logger.info(f"Payload: {data}")

        # Retrieve required fields.
        employee_id = data["employee_id"]
        pto_hours = data["pto_hours"]

        # Retrieve or create the PTO record for the given employee.
        # (Make sure your shared model uses a method like Django's get_or_create or a similar utility.)
        pto, created = PTO.objects.get_or_create(
            employee_id=employee_id,
            defaults={"balance": 0}
        )
        if created:
            logger.info(f"Created new PTO record for employee_id {employee_id} with 0 balance.")

        # Check if there is enough balance.
        if pto.balance < pto_hours:
            msg = (
                f"Not enough PTO balance for employee_id {employee_id}. "
                f"Current balance: {pto.balance}, Requested deduction: {pto_hours}"
            )
            logger.warning(msg)
            dashboard_payload = build_dashboard_payload(employee_id, "pto_deducted", msg)
        else:
            # Deduct PTO and update the record.
            pto.balance -= pto_hours
            pto.save()
            msg = (
                f"PTO successfully deducted for employee_id {employee_id}. "
                f"New balance: {pto.balance}"
            )
            logger.info(msg)
            dashboard_payload = build_dashboard_payload(
                employee_id,
                "refresh_data",
                "Time log created, please refresh dashboard data."
            )

        # Publish the dashboard payload to the Pub/Sub dashboard topic.
        future = publisher.publish(
            DASHBOARD_TOPIC,
            json.dumps(dashboard_payload).encode("utf-8")
        )
        future.result()  # Optionally, wait for the publish to complete.
        logger.info("Published dashboard update.")

    except Exception as e:
        logger.exception(f"Failed to handle message: {str(e)}")
        # Reraise the exception so that Cloud Functions signals a failure and triggers a retry.
        raise
