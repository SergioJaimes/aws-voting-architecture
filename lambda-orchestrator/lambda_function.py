import json
import os
import uuid
import logging
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
events_client = boto3.client("events")

TABLE_NAME = os.environ["TABLE_NAME"]
EVENT_BUS_NAME = os.environ["EVENT_BUS_NAME"]
EVENT_SOURCE = os.environ.get("EVENT_SOURCE", "app.voting")
EVENT_DETAIL_TYPE = os.environ.get("EVENT_DETAIL_TYPE", "VotingCalculationRequested")

table = dynamodb.Table(TABLE_NAME)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_decimal(obj):
    """
    Convierte números float/int a Decimal para DynamoDB.
    """
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_decimal(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    return obj


def build_event_entry(request_id: str, replica: str, payload: dict) -> dict:
    detail = {
        "requestId": request_id,
        "replica": replica,
        "payload": payload,
        "createdAt": utc_now_iso()
    }

    return {
        "Source": EVENT_SOURCE,
        "DetailType": EVENT_DETAIL_TYPE,
        "Detail": json.dumps(detail),
        "EventBusName": EVENT_BUS_NAME
    }


def create_meta_if_not_exists(request_id: str, payload: dict) -> bool:
    """
    Crea el registro META solo si no existe.
    Retorna:
      - True si lo creó
      - False si ya existía
    """
    pk = f"REQ#{request_id}"
    item = {
        "PK": pk,
        "SK": "META",
        "requestId": request_id,
        "status": "PENDING",
        "expectedResults": 3,
        "receivedResults": 0,
        "createdAt": utc_now_iso(),
        "payload": to_decimal(payload)
    }

    try:
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)"
        )
        logger.info("META creado para requestId=%s", request_id)
        return True

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ConditionalCheckFailedException":
            logger.info("META ya existía para requestId=%s", request_id)
            return False
        raise


def publish_replica_events(request_id: str, payload: dict) -> dict:
    entries = [
        build_event_entry(request_id, "A", payload),
        build_event_entry(request_id, "B", payload),
        build_event_entry(request_id, "C", payload),
    ]

    response = events_client.put_events(Entries=entries)

    failed_count = response.get("FailedEntryCount", 0)
    entries_response = response.get("Entries", [])

    logger.info("PutEvents response: %s", json.dumps(response, default=str))

    if failed_count > 0:
        failed_entries = []
        for idx, entry in enumerate(entries_response):
            if "ErrorCode" in entry or "ErrorMessage" in entry:
                failed_entries.append({
                    "index": idx,
                    "replica": ["A", "B", "C"][idx],
                    "errorCode": entry.get("ErrorCode"),
                    "errorMessage": entry.get("ErrorMessage")
                })

        raise RuntimeError(f"Error publicando eventos en EventBridge: {failed_entries}")

    return response


def build_response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body, default=str)
    }


def lambda_handler(event, context):
    """
    Espera un evento con esta forma:

    {
      "requestId": "REQ-123",
      "payload": {
        "pedidoId": "P-100",
        "monto": 250000,
        "moneda": "COP"
      }
    }
    """

    logger.info("Evento recibido: %s", json.dumps(event))

    try:
        request_id = event.get("requestId") or str(uuid.uuid4())
        payload = event.get("payload")

        if not payload or not isinstance(payload, dict):
            return build_response(400, {
                "message": "El campo 'payload' es obligatorio y debe ser un objeto JSON."
            })

        created = create_meta_if_not_exists(request_id, payload)

        if not created:
            # Idempotencia: si ya existe la solicitud, no la recreamos ni redisparamos.
            return build_response(200, {
                "message": "La solicitud ya existía. No se reprocesó.",
                "requestId": request_id,
                "status": "ALREADY_EXISTS"
            })

        publish_replica_events(request_id, payload)

        return build_response(202, {
            "message": "Solicitud aceptada y eventos publicados.",
            "requestId": request_id,
            "status": "PENDING"
        })

    except Exception as e:
        logger.exception("Error procesando la solicitud")
        return build_response(500, {
            "message": "Error interno procesando la solicitud.",
            "error": str(e)
        })