import json
import base64

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

logger = Logger(service="pedeja-faturamento")
metrics = Metrics(namespace="PedeJa", service="pedeja-faturamento")
tracer = Tracer(service="pedeja-faturamento")


@logger.inject_lambda_context
@metrics.log_metrics
@tracer.capture_lambda_handler
def handler(event, context):
    # Consumidor 2 (de N): agrega faturamento por cidade. Le o MESMO stream que o
    # data lake, de forma INDEPENDENTE -- e isso que a fila SQS nao permite.
    # Nao grava no S3: so calcula metrica de negocio em tempo (quase) real.
    registros = event.get("Records", [])
    por_cidade = {}
    for r in registros:
        pedido = json.loads(base64.b64decode(r["kinesis"]["data"]))
        por_cidade.setdefault(pedido["cidade"], 0.0)
        por_cidade[pedido["cidade"]] += pedido["valor"]

    for cidade, total in por_cidade.items():
        metrics.add_dimension(name="cidade", value=cidade)
        metrics.add_metric(name="faturamento_tempo_real", unit="None", value=total)
        metrics.add_metric(name="pedidos_agregados", unit=MetricUnit.Count, value=1)
        logger.info("faturamento agregado", cidade=cidade, total=round(total, 2))
        metrics.flush_metrics(raise_on_empty_metrics=False)  # 1 conjunto EMF por cidade

    return {"cidades_agregadas": len(por_cidade)}
