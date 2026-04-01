import json
import os
import logging
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["TABLE_NAME"])
REPLICA_ID = os.environ["REPLICA_ID"]


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def to_decimal(obj):
    if isinstance(obj, dict):
        return {k: to_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_decimal(v) for v in obj]
    if isinstance(obj, float):
        return Decimal(str(obj))
    return obj


def business_calculation(payload: dict) -> dict:
    """
    Simulación del cálculo.
    Aquí luego metemos tu lógica real.
    """
    monto = payload.get("monto", 0)

    if not isinstance(monto, (int, float)):
        raise ValueError("El campo 'monto' debe ser numérico")

    valor_calculado = monto * 0.98  # ejemplo: aplicar 2% de ajuste

    return {
        "decision": "APPROVED",
        "value": round(valor_calculado, 2)
    }


def lambda_handler(event, context):
    """
    Evento esperado desde EventBridge:

    {
      "version": "0",
      "id": "...",
      "detail-type": "VotingCalculationRequested",
      "source": "app.voting",
      "account": "...",
      "time": "...",
      "region": "...",
      "resources": [],
      "detail": {
        "requestId": "REQ-123",
        "replica": "A",
        "payload": {
          "pedidoId": "P-100",
          "monto": 250000,
          "moneda": "COP"
        }
      }
    }
    """

    logger.info("Evento recibido: %s", json.dumps(event))

    try:
        detail = event.get("detail", {})
        request_id = detail.get("requestId")
        replica = detail.get("replica")
        payload = detail.get("payload")

        if not request_id:
            raise ValueError("Falta requestId en detail")
        if not payload:
            raise ValueError("Falta payload en detail")
        if replica != REPLICA_ID:
            raise ValueError(f"Replica inválida. Esperada={REPLICA_ID}, recibida={replica}")

        result = business_calculation(payload)

        item = {
            "PK": f"REQ#{request_id}",
            "SK": f"RESULT#{REPLICA_ID}",
            "requestId": request_id,
            "replica": REPLICA_ID,
            "status": "OK",
            "result": to_decimal(result),
            "createdAt": utc_now_iso()
        }

        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)"
        )

        logger.info("Resultado guardado para requestId=%s replica=%s", request_id, REPLICA_ID)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Resultado calculado y almacenado",
                "requestId": request_id,
                "replica": REPLICA_ID
            })
        }

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code == "ConditionalCheckFailedException":
            logger.info("Resultado ya existía para requestId=%s replica=%s", request_id, REPLICA_ID)
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Resultado ya existía",
                    "requestId": request_id,
                    "replica": REPLICA_ID
                })
            }

        logger.exception("Error de DynamoDB")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error de DynamoDB",
                "error": str(e)
            })
        }

    except Exception as e:
        logger.exception("Error procesando cálculo")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error procesando cálculo",
                "error": str(e)
            })
        }