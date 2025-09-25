"""
Kubernetes client for SparkApplication operations.
"""

from kubernetes import client, config
from fastapi import HTTPException
from datetime import datetime
from typing import Dict, List, Optional, Any
import os


class SparkApplicationClient:
    """Client for managing SparkApplications in Kubernetes."""

    def __init__(self, namespace: str = "asgard"):
        self.namespace = namespace
        self._setup_kubernetes_client()

    def _setup_kubernetes_client(self):
        """Initialize Kubernetes client."""
        try:
            config.load_incluster_config()
        except Exception:
            try:
                config.load_kube_config()
            except Exception as e:
                raise HTTPException(500, f"Kube config error: {e}")

    def create_spark_application(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Create a SparkApplication in Kubernetes."""
        co = client.CustomObjectsApi()

        try:
            response = co.create_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                body=spec,
            )
            return response

        except client.ApiException as e:
            error_detail = f"SparkApplication create failed: {e.reason}"
            if e.body:
                try:
                    import json

                    error_body = json.loads(e.body)
                    error_detail += f" - {error_body.get('message', e.body)}"
                except:
                    error_detail += f" - {e.body}"
            raise HTTPException(500, error_detail)
        except Exception as e:
            error_detail = f"Unexpected error creating SparkApplication: {str(e)}"
            raise HTTPException(500, error_detail)

    def get_spark_application(self, name: str) -> Dict[str, Any]:
        """Get a SparkApplication by name."""
        co = client.CustomObjectsApi()

        try:
            spark_app = co.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=name,
            )
            return spark_app

        except client.ApiException as e:
            if e.status == 404:
                raise HTTPException(404, f"SparkApplication {name} not found")
            raise HTTPException(500, f"Failed to get SparkApplication: {e.reason}")
        except Exception as e:
            raise HTTPException(500, f"Unexpected error getting SparkApplication: {str(e)}")

    def list_spark_applications(
        self, limit: int = 20, label_selector: str = ""
    ) -> List[Dict[str, Any]]:
        """List SparkApplications."""
        co = client.CustomObjectsApi()

        try:
            spark_apps = co.list_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                label_selector=label_selector,
                limit=limit,
            )
            return spark_apps.get("items", [])

        except client.ApiException as e:
            raise HTTPException(500, f"Failed to list SparkApplications: {e.reason}")
        except Exception as e:
            raise HTTPException(500, f"Unexpected error listing SparkApplications: {str(e)}")

    def get_pod_logs(self, pod_name: str, container: str = "spark-kubernetes-driver") -> str:
        """Get logs from a pod."""
        v1 = client.CoreV1Api()

        try:
            logs = v1.read_namespaced_pod_log(
                name=pod_name, namespace=self.namespace, container=container
            )
            return logs

        except client.ApiException as e:
            if e.status == 404:
                return "Driver pod not found or logs not available yet"
            else:
                return f"Error getting logs: {e.reason}"

    def get_events(self, object_name: str) -> List[Dict[str, Any]]:
        """Get Kubernetes events for an object."""
        v1 = client.CoreV1Api()

        try:
            events = v1.list_namespaced_event(
                namespace=self.namespace, field_selector=f"involvedObject.name={object_name}"
            )

            return [
                {
                    "time": event.first_timestamp.isoformat() if event.first_timestamp else "",
                    "type": event.type,
                    "reason": event.reason,
                    "message": event.message,
                    "object": f"{event.involved_object.kind}/{object_name}",
                }
                for event in events.items
            ]

        except client.ApiException as e:
            return []
        except Exception as e:
            return []


class SparkApplicationFactory:
    """Factory for creating SparkApplication specifications."""

    @staticmethod
    def create_sql_execution_spec(
        name: str,
        namespace: str,
        sql: str,
        sources: List[str],
        destination: str,
        write_mode: str = "overwrite",
        driver_cores: int = 1,
        driver_memory: str = "512m",
        executor_cores: int = 1,
        executor_instances: int = 1,
        executor_memory: str = "512m",
        spark_image: str = "637423187518.dkr.ecr.eu-north-1.amazonaws.com/spark-custom:latest",
        service_account: str = "spark-sa",
        s3_secret_name: str = "s3-credentials",
    ) -> Dict[str, Any]:
        """Create a SparkApplication spec for SQL execution."""

        import json

        # Use custom Spark image with S3A support
        if not spark_image:
            spark_image = os.getenv("SPARK_IMAGE")

        return {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "labels": {"app": "asgard-transform", "type": "sql-transformation"},
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": spark_image,
                "imagePullPolicy": "Always",
                "imagePullSecrets": ["ecr-secret"],
                "mainApplicationFile": "local:///opt/spark/sql_transform.py",
                "sparkVersion": "3.4.0",
                "restartPolicy": {"type": "Never"},
                "driver": {
                    "cores": driver_cores,
                    "memory": driver_memory,
                    "serviceAccount": service_account,
                    "volumeMounts": [{"name": "job-code", "mountPath": "/opt/spark/app"}],
                    "envFrom": [{"secretRef": {"name": s3_secret_name}}],
                    "env": [
                        {
                            "name": "AWS_SECRET_KEY",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": s3_secret_name,
                                    "key": "AWS_SECRET_ACCESS_KEY",
                                }
                            },
                        },
                        {"name": "SQL_QUERY", "value": sql},
                        {"name": "SOURCE_PATHS", "value": json.dumps(sources)},
                        {"name": "DESTINATION_PATH", "value": destination},
                        {"name": "WRITE_MODE", "value": write_mode},
                    ],
                    "volumeMounts": [],
                },
                "executor": {
                    "cores": executor_cores,
                    "instances": executor_instances,
                    "memory": executor_memory,
                    "volumeMounts": [{"name": "job-code", "mountPath": "/opt/spark/app"}],
                    "envFrom": [{"secretRef": {"name": s3_secret_name}}],
                    "env": [
                        {
                            "name": "AWS_SECRET_KEY",
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": s3_secret_name,
                                    "key": "AWS_SECRET_ACCESS_KEY",
                                }
                            },
                        },
                        {"name": "SQL_QUERY", "value": sql},
                        {"name": "SOURCE_PATHS", "value": json.dumps(sources)},
                        {"name": "DESTINATION_PATH", "value": destination},
                        {"name": "WRITE_MODE", "value": write_mode},
                    ],
                },
                "sparkConf": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                    "spark.hadoop.fs.s3a.access.key":  os.getenv("AWS_ACCESS_KEY_ID"),
                    "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
                    "spark.hadoop.fs.s3a.endpoint.region": "eu-north-1",
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.fast.upload": "true",
                    "spark.hadoop.fs.s3a.multipart.size": "67108864",
                    "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
                    "spark.hadoop.fs.s3a.path.style.access": "false",
                    "spark.driver.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
                    "spark.executor.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
                    # Pass our parameters via spark configuration
                    "spark.sql.transform.query": sql,
                    "spark.sql.transform.sources": json.dumps(sources),
                    "spark.sql.transform.destination": destination,
                    "spark.sql.transform.writeMode": write_mode,
                },
                "volumes": [],
            },
        }


def get_spark_client() -> SparkApplicationClient:
    """Dependency function to get SparkApplicationClient."""
    namespace = os.getenv("SPARK_NAMESPACE", "asgard")
    return SparkApplicationClient(namespace=namespace)
