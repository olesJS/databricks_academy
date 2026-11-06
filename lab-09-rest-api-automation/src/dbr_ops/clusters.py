import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import DataSecurityMode

from dbr_ops.models import ClusterSpec
from dbr_ops.client import get_identity

log = logging.getLogger(__name__)


def create_cluster(w: WorkspaceClient, spec: ClusterSpec) -> str:
    spark_version = spec.spark_version or w.clusters.select_spark_version(latest=True, long_term_support=True)
    node_type_id = spec.node_type_id or w.clusters.select_node_type(local_disk=True)

    extra = {}
    if spec.num_workers == 0:
        extra["spark_conf"] = {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]"
        }
        extra["custom_tags"] = {
            "ResourceClass": "SingleNode"
        }

    log.info(
        "Creating cluster %r (spark=%s, node=%s, workers=%d); waiting for RUNNING...",
        spec.name, spark_version, node_type_id, spec.num_workers,
    )
    cluster_details = w.clusters.create_and_wait(
        cluster_name=spec.name,
        spark_version=spark_version,
        node_type_id=node_type_id,
        num_workers=spec.num_workers,
        autotermination_minutes=spec.autotermination_minutes,
        data_security_mode=DataSecurityMode.SINGLE_USER,
        single_user_name=get_identity(w).user_name,
        **extra
    )

    log.info("Cluster %s is RUNNING", cluster_details.cluster_id)
    return cluster_details.cluster_id


def delete_cluster(w: WorkspaceClient, cluster_id: str):
    log.info("Permanently deleting cluster %s", cluster_id)
    w.clusters.permanent_delete(cluster_id=cluster_id)
    log.info("Cluster %s deleted", cluster_id)
