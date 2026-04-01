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


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def decimal_to_native(obj):
    if isinstance(obj, list):
        return [decimal_to_native(v) for v in obj]
    if isinstance(obj, dict):
        return {k: decimal_to_native(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    return obj


def get_item(request_id: str, sk: str):
    response = table.get_item(
        Key={
            "PK": f"REQ#{request_id}",
            "SK": sk
        }
    )
    return response.get("Item")


def normalize_result(item):
    if not item:
        return None

    result = decimal_to_native(item.get("result", {}))
    status = item.get("status", "UNKNOWN")

    return {
        "status": status,
        "decision": result.get("decision"),
        "value": result.get("value")
    }


def vote_results(result_a, result_b, result_c):
    a = normalize_result(result_a)
    b = normalize_result(result_b)
    c = normalize_result(result_c)

    base_output = {
        "replicaAResult": a,
        "replicaBResult": b,
        "replicaCResult": c
    }

    if not a or not b or not c:
        return {
            **base_output,
            "voteStatus": "INCOMPLETE",
            "finalResult": None,
            "divergentReplica": None
        }

    if a == b == c:
        return {
            **base_output,
            "voteStatus": "3_OF_3",
            "finalResult": a,
            "divergentReplica": None
        }

    if a == b:
        return {
            **base_output,
            "voteStatus": "2_OF_3",
            "finalResult": a,
            "divergentReplica": "C"
        }

    if a == c:
        return {
            **base_output,
            "voteStatus": "2_OF_3",
            "finalResult": a,
            "divergentReplica": "B"
        }

    if b == c:
        return {
            **base_output,
            "voteStatus": "2_OF_3",
            "finalResult": b,
            "divergentReplica": "A"
        }

    return {
        **base_output,
        "voteStatus": "NO_CONSENSUS",
        "finalResult": None,
        "divergentReplica": "ALL"
    }


def final_already_exists(request_id: str) -> bool:
    item = get_item(request_id, "FINAL")
    return item is not None


def save_final_result(request_id: str, voting_output: dict):
    item = {
        "PK": f"REQ#{request_id}",
        "SK": "FINAL",
        "requestId": request_id,
        "voteStatus": voting_output["voteStatus"],
        "finalResult": voting_output["finalResult"],
        "divergentReplica": voting_output["divergentReplica"],
        "replicaAResult": voting_output.get("replicaAResult"),
        "replicaBResult": voting_output.get("replicaBResult"),
        "replicaCResult": voting_output.get("replicaCResult"),
        "createdAt": utc_now_iso()
    }

    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(PK) AND attribute_not_exists(SK)"
    )
    return item


def extract_request_ids_from_stream_event(event):
    request_ids = set()

    for record in event.get("Records", []):
        if record.get("eventName") not in ["INSERT", "MODIFY"]:
            continue

        dynamodb_record = record.get("dynamodb", {})
        new_image = dynamodb_record.get("NewImage", {})

        pk_attr = new_image.get("PK", {})
        sk_attr = new_image.get("SK", {})

        pk = pk_attr.get("S")
        sk = sk_attr.get("S")

        if not pk or not sk:
            continue

        if not pk.startswith("REQ#"):
            continue

        # Solo reaccionamos a resultados parciales
        if sk in ["RESULT#A", "RESULT#B", "RESULT#C"]:
            request_id = pk.replace("REQ#", "", 1)
            request_ids.add(request_id)

    return list(request_ids)


def process_request_id(request_id: str):
    if final_already_exists(request_id):
        logger.info("FINAL ya existe para requestId=%s", request_id)
        return {
            "requestId": request_id,
            "status": "FINAL_ALREADY_EXISTS"
        }

    result_a = get_item(request_id, "RESULT#A")
    result_b = get_item(request_id, "RESULT#B")
    result_c = get_item(request_id, "RESULT#C")

    voting_output = vote_results(result_a, result_b, result_c)

    if voting_output["voteStatus"] == "INCOMPLETE":
        logger.info("Aún no están los 3 resultados para requestId=%s", request_id)
        return {
            "requestId": request_id,
            "status": "INCOMPLETE"
        }

    saved = save_final_result(request_id, voting_output)

    logger.info("Resultado final guardado: %s", json.dumps(decimal_to_native(saved)))

    return {
    "requestId": request_id,
    "status": "FINAL_SAVED",
    "voteStatus": voting_output["voteStatus"],
    "divergentReplica": voting_output["divergentReplica"],
    "finalResult": voting_output["finalResult"],
    "replicaAResult": voting_output.get("replicaAResult"),
    "replicaBResult": voting_output.get("replicaBResult"),
    "replicaCResult": voting_output.get("replicaCResult")
}


def lambda_handler(event, context):
    logger.info("Evento recibido: %s", json.dumps(event))

    try:
        # Caso 1: invocación manual
        if "requestId" in event:
            result = process_request_id(event["requestId"])
            return {
                "statusCode": 200,
                "body": json.dumps(result)
            }

        # Caso 2: evento desde DynamoDB Streams
        if "Records" in event:
            request_ids = extract_request_ids_from_stream_event(event)

            results = []
            for request_id in request_ids:
                results.append(process_request_id(request_id))

            return {
                "statusCode": 200,
                "body": json.dumps(results)
            }

        raise ValueError("Evento no soportado")

    except ClientError as e:
        logger.exception("Error DynamoDB")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error de DynamoDB",
                "error": str(e)
            })
        }

    except Exception as e:
        logger.exception("Error en voter")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Error procesando votación",
                "error": str(e)
            })
        }