import os
import json
import base64
import boto3

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

logger = Logger(service="pedeja-datalake")
metrics = Metrics(namespace="PedeJa", service="pedeja-datalake")
tracer = Tracer(service="pedeja-datalake")

s3 = boto3.client("s3")
BUCKET = os.environ["BUCKET_DATA_LAKE"]


@logger.inject_lambda_context
@metrics.log_metrics
@tracer.capture_lambda_handler
def handler(event, context):
    # Consumidor 1 (de N): grava no data lake. Le o MESMO stream que o agregador.
    # No Kinesis o dado vem em base64 dentro de Records[].kinesis.data.
    registros = event.get("Records", [])
    for r in registros:
        pedido = json.loads(base64.b64decode(r["kinesis"]["data"]))
        dt = pedido["event_time"][:10]
        key = f"pedidos/dt={dt}/{pedido['pedido_id']}.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=json.dumps(pedido, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
        metrics.add_metric(name="pedidos_no_datalake", unit=MetricUnit.Count, value=1)
        logger.info("gravado no data lake", pedido_id=pedido["pedido_id"], s3_key=key)

    return {"gravados": len(registros)}
