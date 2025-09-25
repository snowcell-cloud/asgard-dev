"""
Service layer for data transformation operations.
"""

from typing import Dict, List, Any, Optional
from datetime import datetime
import uuid
import os

from fastapi import HTTPException

from app.data_transformation.client import SparkApplicationClient, SparkApplicationFactory
from app.data_transformation.schemas import TransformReq
from app.airbyte.client import AirbyteClient, AirbyteClientError
from fastapi import HTTPException


class TransformationService:
    """Service for managing data transformation jobs."""
    
    def __init__(self, spark_client: SparkApplicationClient, airbyte_client: AirbyteClient):
        self.spark_client = spark_client
        self.airbyte_client = airbyte_client
    
    async def _resolve_workspace_id(self, workspace_name: str | None = None) -> str:
        """Resolve workspace ID from name or get default workspace."""
        try:
            workspaces = await self.airbyte_client.list_workspaces()
            
            if not workspaces:
                raise HTTPException(status_code=404, detail="No workspaces found")
            
            if workspace_name:
                # Find workspace by name
                for workspace in workspaces:
                    if workspace.get("name") == workspace_name:
                        return workspace["workspaceId"]
                raise HTTPException(status_code=404, detail=f"Workspace '{workspace_name}' not found")
            
            # Return first workspace as default
            return workspaces[0]["workspaceId"]
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error resolving workspace: {str(e)}")
    
    async def _get_s3_destinations(self) -> List[Dict[str, Any]]:
        """Fetch S3 destinations from Airbyte."""
        try:
            workspace_id = await self._resolve_workspace_id()
            destinations = await self.airbyte_client.list_destinations(workspace_id)
            
            # Filter for S3 destinations only
            s3_destinations = []
            for dest in destinations:
                if dest.get("name", "").lower() == "s3":
                    s3_destinations.append(dest)
            
            if not s3_destinations:
                raise HTTPException(
                    status_code=404, 
                    detail="No S3 destinations found in Airbyte. Please register an S3 sink first using /sink endpoint."
                )
            
            return s3_destinations
        except AirbyteClientError as e:
            raise HTTPException(status_code=e.status_code, detail=f"Airbyte error: {e.message}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error fetching S3 destinations: {str(e)}")
    
    async def submit_transformation(self, req: TransformReq) -> Dict[str, Any]:
        """Submit a new transformation job."""
        
        # Generate unique run ID first
        run_id = uuid.uuid4().hex[:8]
        name = f"sql-exec-{run_id}"
        
        # Try to get S3 configuration from Airbyte, fallback to environment variables
        try:
            s3_destinations = await self._get_s3_destinations()
            source_dest = s3_destinations[0]
            dest_config = source_dest.get("configuration", {})
            source_bucket = dest_config.get("s3_bucket_name")
        except Exception as e:
            print(f"Warning: Could not get S3 config from Airbyte: {e}")
            # Fallback to environment variable or default
            source_bucket = os.getenv("S3_BUCKET_NAME", "your-default-bucket")
            if source_bucket == "your-default-bucket":
                raise HTTPException(
                    status_code=500,
                    detail="S3 bucket configuration missing. Set S3_BUCKET_NAME environment variable or configure Airbyte S3 destination."
                )
        
        if not source_bucket:
            raise HTTPException(
                status_code=500,
                detail="S3 destination configuration is missing bucket name"
            )
        
        # Construct source and destination paths
        # Use custom source path if provided, otherwise use bronze layer
        if req.source_path:
            # Use the specific S3 path provided by the user
            if req.source_path.startswith("s3://"):
                source_s3_path = req.source_path.replace("s3://", "s3a://", 1)
            elif req.source_path.startswith("s3a://"):
                source_s3_path = req.source_path
            else:
                source_s3_path = f"s3a://{req.source_path}"
        else:
            # Default: read from bronze layer
            source_s3_path = f"s3a://{source_bucket}/bronze/"
            
        destination_s3_path = f"s3a://{source_bucket}/silver/{run_id}/"
        
        # Sources list for Spark
        sources = [source_s3_path]
        destination = destination_s3_path
        
        print(f"Creating Spark job - Source: {source_s3_path}, Destination: {destination_s3_path}")
        
        # Create SparkApplication spec
        spark_spec = SparkApplicationFactory.create_sql_execution_spec(
            name=name,
            namespace=self.spark_client.namespace,
            sql=req.sql,
            sources=sources,
            destination=destination,
            write_mode=req.write_mode,
            driver_cores=req.driver_cores,
            driver_memory=req.driver_memory,
            executor_cores=req.executor_cores,
            executor_instances=req.executor_instances,
            executor_memory=req.executor_memory,
            spark_image="637423187518.dkr.ecr.eu-north-1.amazonaws.com/spark-custom:latest"
        )
        
        try:
            # Create the SparkApplication
            response = self.spark_client.create_spark_application(spark_spec)
            print(f"SparkApplication {name} created successfully in namespace {self.spark_client.namespace}")
            
            return {
                "run_id": run_id, 
                "spark_application": name, 
                "status": "submitted",
                "source": source_s3_path,
                "destination": destination_s3_path,
                "sql": req.sql,
                "message": f"Transformation submitted. Source: {source_s3_path} -> Destination: {destination_s3_path}",
                "namespace": self.spark_client.namespace,
                "spark_response": response.get("metadata", {}).get("name", name)
            }
        except Exception as e:
            print(f"Error creating SparkApplication: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create Spark job: {str(e)}"
            )
    
    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get the status of a transformation job."""
        name = f"sql-exec-{run_id}"
        
        spark_app = self.spark_client.get_spark_application(name)
        
        status = spark_app.get("status", {})
        spec = spark_app.get("spec", {})
        metadata = spark_app.get("metadata", {})
        
        # Extract key information
        app_state = status.get("applicationState", {})
        driver_info = status.get("driverInfo", {})
        executor_infos = status.get("executorInfos", [])
        
        return {
            "run_id": run_id,
            "spark_application": name,
            "namespace": self.spark_client.namespace,
            "state": app_state.get("state", "UNKNOWN"),
            "error_message": app_state.get("errorMessage"),
            "driver_pod": driver_info.get("podName"),
            "driver_state": driver_info.get("webUIAddress"),
            "executor_count": len(executor_infos),
            "creation_time": metadata.get("creationTimestamp"),
            "spark_version": spec.get("sparkVersion"),
            "image": spec.get("image"),
            "full_status": status
        }
    
    def get_job_logs(self, run_id: str) -> Dict[str, Any]:
        """Get logs from the transformation job driver pod."""
        name = f"sql-exec-{run_id}"
        
        spark_app = self.spark_client.get_spark_application(name)
        
        status = spark_app.get("status", {})
        driver_info = status.get("driverInfo", {})
        driver_pod = driver_info.get("podName")
        
        if not driver_pod:
            return {
                "run_id": run_id,
                "message": "Driver pod not found or not started yet",
                "logs": ""
            }
        
        logs = self.spark_client.get_pod_logs(driver_pod)
        
        return {
            "run_id": run_id,
            "driver_pod": driver_pod,
            "logs": logs
        }
    
    def get_job_events(self, run_id: str) -> Dict[str, Any]:
        """Get Kubernetes events related to the transformation job."""
        name = f"sql-exec-{run_id}"
        
        # Get SparkApplication events
        spark_events = self.spark_client.get_events(name)
        
        # Get driver pod events if it exists
        driver_events = []
        try:
            spark_app = self.spark_client.get_spark_application(name)
            status = spark_app.get("status", {})
            driver_info = status.get("driverInfo", {})
            driver_pod = driver_info.get("podName")
            
            if driver_pod:
                driver_events = self.spark_client.get_events(driver_pod)
        except:
            pass
        
        all_events = spark_events + driver_events
        all_events.sort(key=lambda x: x["time"])
        
        return {
            "run_id": run_id,
            "events": all_events
        }
    
    def get_job_metrics(self, run_id: str) -> Dict[str, Any]:
        """Get metrics and resource usage for the transformation job."""
        name = f"sql-exec-{run_id}"
        
        spark_app = self.spark_client.get_spark_application(name)
        
        status = spark_app.get("status", {})
        spec = spark_app.get("spec", {})
        metadata = spark_app.get("metadata", {})
        
        # Calculate durations
        creation_time = metadata.get("creationTimestamp")
        start_time = status.get("lastSubmissionAttemptTime")
        end_time = status.get("terminationTime")
        
        duration_seconds = None
        if start_time and end_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                duration_seconds = (end_dt - start_dt).total_seconds()
            except:
                pass
        
        # Get resource specifications
        driver_spec = spec.get("driver", {})
        executor_spec = spec.get("executor", {})
        
        return {
            "run_id": run_id,
            "spark_application": name,
            "timing": {
                "created_at": creation_time,
                "started_at": start_time,
                "finished_at": end_time,
                "duration_seconds": duration_seconds
            },
            "resource_specs": {
                "driver": {
                    "cores": driver_spec.get("cores"),
                    "memory": driver_spec.get("memory")
                },
                "executor": {
                    "cores": executor_spec.get("cores"),
                    "instances": executor_spec.get("instances"),
                    "memory": executor_spec.get("memory")
                }
            },
            "spark_info": {
                "application_id": status.get("sparkApplicationId"),
                "spark_version": spec.get("sparkVersion"),
                "image": spec.get("image"),
                "submission_attempts": status.get("submissionAttempts", 0),
                "execution_attempts": status.get("executionAttempts", 0)
            },
            "ui_access": {
                "web_ui_address": status.get("driverInfo", {}).get("webUIAddress"),
                "web_ui_service": status.get("driverInfo", {}).get("webUIServiceName")
            }
        }
    
    def list_jobs(self, limit: int = 20, status_filter: str = None) -> Dict[str, Any]:
        """List recent transformation jobs with their status."""
        spark_apps = self.spark_client.list_spark_applications(limit=limit)
        
        jobs = []
        for app in spark_apps:
            metadata = app.get("metadata", {})
            status = app.get("status", {})
            spec = app.get("spec", {})
            
            app_name = metadata.get("name", "")
            if not app_name.startswith("sql-exec-"):
                continue
                
            run_id = app_name.replace("sql-exec-", "")
            app_state = status.get("applicationState", {})
            state = app_state.get("state", "UNKNOWN")
            
            # Apply status filter if provided
            if status_filter and state.lower() != status_filter.lower():
                continue
            
            job_info = {
                "run_id": run_id,
                "spark_application": app_name,
                "state": state,
                "created_at": metadata.get("creationTimestamp"),
                "started_at": status.get("lastSubmissionAttemptTime"),
                "finished_at": status.get("terminationTime"),
                "spark_version": spec.get("sparkVersion"),
                "submission_attempts": status.get("submissionAttempts", 0),
                "driver_pod": status.get("driverInfo", {}).get("podName"),
                "web_ui": status.get("driverInfo", {}).get("webUIAddress")
            }
            
            if app_state.get("errorMessage"):
                error_msg = app_state.get("errorMessage", "")
                job_info["error_message"] = error_msg[:200] + "..." if len(error_msg) > 200 else error_msg
            
            jobs.append(job_info)
        
        # Sort by creation time (newest first)
        jobs.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        
        return {
            "jobs": jobs,
            "total_count": len(jobs),
            "filter_applied": status_filter
        }
