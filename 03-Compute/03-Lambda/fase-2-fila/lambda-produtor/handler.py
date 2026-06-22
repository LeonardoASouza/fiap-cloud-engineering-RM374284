import os
import json
import boto3

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

logger = Logger(service="pedeja-produtor")
metrics = Metrics(namespace="PedeJa", service="pedeja-produtor")
tracer = Tracer(service="pedeja-produtor")

sqs = boto3.client("sqs")
QUEUE_URL = os.environ["QUEUE_URL"]


@logger.inject_lambda_context
@metrics.log_metrics
@tracer.capture_lambda_handler
def handler(event, context):
    # Produtor faz UMA coisa so: validar minimamente e ENFILEIRAR.
    # Nao grava no S3 -> responde em milissegundos -> aguenta rajada (Black Friday).
    pedido = json.loads(event.get("body") or "{}")

    sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(pedido, ensure_ascii=False),
    )

    metrics.add_metric(name="pedidos_enfileirados", unit=MetricUnit.Count, value=1)
    logger.info("pedido enfileirado", pedido_id=pedido.get("pedido_id"))

    # 202 Accepted: "recebi e vou processar", nao "ja processei".
    return {
        "statusCode": 202,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"status": "enfileirado", "pedido_id": pedido.get("pedido_id")}, ensure_ascii=False),
    }
