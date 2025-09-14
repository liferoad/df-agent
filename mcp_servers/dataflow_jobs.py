#!/usr/bin/env python3
"""
MCP Server for Google Cloud Dataflow operations.
This server provides tools to interact with Google Cloud Dataflow via gcloud CLI.
"""

import asyncio
import json
import os
import subprocess
from typing import Any, Dict, List

import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.types import Tool

# Create MCP server instance
server = Server("dataflow-mcp-server")


def get_gcloud_timeout() -> int:
    """
    Get the gcloud command timeout from environment variable.
    Defaults to 300 seconds if not set.
    """
    return int(os.getenv("GCLOUD_TIMEOUT", "300"))


@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    """
    List available tools for Dataflow operations.
    """
    return [
        Tool(
            name="check_dataflow_job_status",
            description="Check the status of a specific Google Cloud Dataflow job",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The Dataflow job ID to check",
                    },
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID (required)",
                    },
                    "region": {
                        "type": "string",
                        "description": "Google Cloud region (defaults to us-central1)",
                        "default": "us-central1",
                    },
                },
                "required": ["job_id", "project_id"],
            },
        ),
        Tool(
            name="list_dataflow_jobs",
            description="List Google Cloud Dataflow jobs with optional filtering",
            inputSchema={
                "type": "object",
                "properties": {
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID (required)",
                    },
                    "region": {
                        "type": "string",
                        "description": "Google Cloud region (defaults to us-central1)",
                        "default": "us-central1",
                    },
                    "status": {
                        "type": "string",
                        "description": (
                            "Job status filter (active/terminated/failed/all)"
                        ),
                        "enum": ["active", "terminated", "failed", "all"],
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of jobs to return (default: 50)",
                        "default": 50,
                    },
                },
                "required": ["project_id"],
            },
        ),
        Tool(
            name="get_dataflow_job_logs",
            description="Get logs for a specific Dataflow job",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The Dataflow job ID to get logs for",
                    },
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID (required)",
                    },
                    "region": {
                        "type": "string",
                        "description": "Google Cloud region (defaults to us-central1)",
                        "default": "us-central1",
                    },
                    "severity": {
                        "type": "string",
                        "description": "Minimum log severity level",
                        "enum": ["DEBUG", "INFO", "WARNING", "ERROR"],
                        "default": "INFO",
                    },
                },
                "required": ["job_id", "project_id"],
            },
        ),
        Tool(
            name="cancel_dataflow_job",
            description="Cancel a running Google Cloud Dataflow job",
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The Dataflow job ID to cancel",
                    },
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID (required)",
                    },
                    "region": {
                        "type": "string",
                        "description": "Google Cloud region (defaults to us-central1)",
                        "default": "us-central1",
                    },
                },
                "required": ["job_id", "project_id"],
            },
        ),
        Tool(
            name="drain_dataflow_job",
            description=(
                "Drain a streaming Google Cloud Dataflow job (graceful shutdown)"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "job_id": {
                        "type": "string",
                        "description": "The Dataflow job ID to drain",
                    },
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud project ID (required)",
                    },
                    "region": {
                        "type": "string",
                        "description": "Google Cloud region (defaults to us-central1)",
                        "default": "us-central1",
                    },
                },
                "required": ["job_id", "project_id"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: Dict[str, Any]
) -> List[types.TextContent]:
    """
    Handle tool calls for Dataflow operations.
    """
    try:
        if name == "check_dataflow_job_status":
            return await check_dataflow_job_status(arguments)
        elif name == "list_dataflow_jobs":
            return await list_dataflow_jobs(arguments)
        elif name == "get_dataflow_job_logs":
            return await get_dataflow_job_logs(arguments)
        elif name == "cancel_dataflow_job":
            return await cancel_dataflow_job(arguments)
        elif name == "drain_dataflow_job":
            return await drain_dataflow_job(arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")
    except Exception as e:
        return [
            types.TextContent(type="text", text=f"Error executing {name}: {str(e)}")
        ]


async def check_dataflow_job_status(
    arguments: Dict[str, Any],
) -> List[types.TextContent]:
    """
    Check the status of a specific Dataflow job.
    """
    job_id = arguments["job_id"]
    project_id = arguments["project_id"]  # Now required
    region = arguments.get("region", "us-central1")  # Default to us-central1

    # Build gcloud command with required project_id and region
    cmd = [
        "gcloud",
        "dataflow",
        "jobs",
        "describe",
        job_id,
        "--format=json",
        "--project",
        project_id,
        "--region",
        region,
    ]

    try:
        # Execute gcloud command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=get_gcloud_timeout(),
        )
        job_data = json.loads(result.stdout)

        # Extract key information
        job_info = {
            "job_id": job_data.get("id", "N/A"),
            "name": job_data.get("name", "N/A"),
            "state": job_data.get("currentState", "UNKNOWN"),
            "type": job_data.get("type", "N/A"),
            "create_time": job_data.get("createTime", "N/A"),
            "start_time": job_data.get("startTime", "N/A"),
            "end_time": job_data.get("endTime", "N/A"),
            "location": job_data.get("location", "N/A"),
        }

        # Check for error information if job failed
        error_info = ""
        if job_info["state"] in ["JOB_STATE_FAILED", "FAILED"]:
            if "stageStates" in job_data:
                for stage in job_data["stageStates"]:
                    if stage.get("executionState") == "JOB_STATE_FAILED":
                        error_info += f"\nStage '{stage.get('name', 'Unknown')}' failed"

            # Look for error messages in job metadata
            if "environment" in job_data and "userAgent" in job_data["environment"]:
                error_info += f"\nUser Agent: {job_data['environment']['userAgent']}"

        # Format response
        response = f"""Dataflow Job Status Report:

Job ID: {job_info['job_id']}
Job Name: {job_info['name']}
Current State: {job_info['state']}
Job Type: {job_info['type']}
Location: {job_info['location']}
Created: {job_info['create_time']}
Started: {job_info['start_time']}
Ended: {job_info['end_time']}
"""

        if error_info:
            response += f"\nError Information:{error_info}"

        # Add raw JSON for detailed analysis
        response += f"\n\nRaw Job Data (JSON):\n{json.dumps(job_data, indent=2)}"

        return [types.TextContent(type="text", text=response)]

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to get job status for {job_id}:\n{e.stderr}"
        return [types.TextContent(type="text", text=error_msg)]
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse gcloud response: {str(e)}"
        return [types.TextContent(type="text", text=error_msg)]


async def list_dataflow_jobs(arguments: Dict[str, Any]) -> List[types.TextContent]:
    """
    List Dataflow jobs with optional filtering.
    """
    project_id = arguments["project_id"]  # Now required
    region = arguments.get("region", "us-central1")  # Default to us-central1
    status = arguments.get("status", "all")
    limit = arguments.get("limit", 50)

    # Build gcloud command with required project_id and region
    cmd = [
        "gcloud",
        "dataflow",
        "jobs",
        "list",
        "--format=json",
        f"--limit={limit}",
        "--project",
        project_id,
        "--region",
        region,
    ]

    # Handle status filtering - use 'terminated' to get completed jobs,
    # then filter for failed ones
    filter_failed_jobs = False
    if status == "failed":
        cmd.extend(["--status", "terminated"])
        filter_failed_jobs = True
    elif status != "all":
        cmd.extend(["--status", status])

    try:
        # Execute gcloud command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=get_gcloud_timeout(),
        )
        jobs_data = json.loads(result.stdout)

        # Filter for failed jobs if requested
        if filter_failed_jobs:
            jobs_data = [
                job
                for job in jobs_data
                if job.get("state", "").upper() in ["JOB_STATE_FAILED", "FAILED"]
            ]

        if not jobs_data:
            status_msg = "failed" if filter_failed_jobs else status
            return [
                types.TextContent(
                    type="text",
                    text=f"No Dataflow jobs found with status '{status_msg}'.",
                )
            ]

        # Format job list
        status_display = "failed" if filter_failed_jobs else status
        response = (
            f"Dataflow Jobs (status: {status_display}, showing up to {limit} jobs):\n\n"
        )

        # Format job list
        response = f"Dataflow Jobs (showing up to {limit} jobs):\n\n"

        for job in jobs_data:
            job_id = job.get("id", "N/A")
            name = job.get("name", "N/A")
            state = job.get("state", "UNKNOWN")
            job_type = job.get("type", "N/A")
            create_time = job.get("createTime", "N/A")

            response += f"• Job ID: {job_id}\n"
            response += f"  Name: {name}\n"
            response += f"  State: {state}\n"
            response += f"  Type: {job_type}\n"
            response += f"  Created: {create_time}\n\n"

        return [types.TextContent(type="text", text=response)]

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to list Dataflow jobs:\n{e.stderr}"
        return [types.TextContent(type="text", text=error_msg)]
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse gcloud response: {str(e)}"
        return [types.TextContent(type="text", text=error_msg)]


async def get_dataflow_job_logs(arguments: Dict[str, Any]) -> List[types.TextContent]:
    """
    Get logs for a specific Dataflow job.
    """
    job_id = arguments["job_id"]
    project_id = arguments["project_id"]  # Now required
    severity = arguments.get("severity", "INFO")

    # Build gcloud command for logs with required project_id
    # Note: gcloud logging doesn't use --region parameter
    # Include severity in the filter query instead of as a separate parameter
    cmd = [
        "gcloud",
        "logging",
        "read",
        (
            f'resource.type="dataflow_step" AND '
            f'resource.labels.job_id="{job_id}" AND '
            f"severity>={severity}"
        ),
        "--format=json",
        "--limit=50",
        "--project",
        project_id,
    ]

    try:
        # Execute gcloud command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=get_gcloud_timeout(),
        )
        logs_data = json.loads(result.stdout)

        if not logs_data:
            return [
                types.TextContent(
                    type="text",
                    text=(
                        f"No logs found for job {job_id} "
                        f"with severity {severity} or higher."
                    ),
                )
            ]

        # Format logs
        response = f"Dataflow Job Logs for {job_id} (severity: {severity}+):\n\n"

        for log_entry in logs_data:
            timestamp = log_entry.get("timestamp", "N/A")
            severity_level = log_entry.get("severity", "UNKNOWN")
            message = log_entry.get(
                "textPayload",
                log_entry.get("jsonPayload", {}).get("message", "No message"),
            )

            response += f"[{timestamp}] {severity_level}: {message}\n"

        return [types.TextContent(type="text", text=response)]

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to get logs for job {job_id}:\n{e.stderr}"
        return [types.TextContent(type="text", text=error_msg)]
    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse gcloud response: {str(e)}"
        return [types.TextContent(type="text", text=error_msg)]


async def cancel_dataflow_job(arguments: Dict[str, Any]) -> List[types.TextContent]:
    """
    Cancel a running Dataflow job.
    """
    job_id = arguments["job_id"]
    project_id = arguments["project_id"]  # Now required
    region = arguments.get("region", "us-central1")  # Default to us-central1

    # Build gcloud command with required project_id and region
    cmd = [
        "gcloud",
        "dataflow",
        "jobs",
        "cancel",
        job_id,
        "--project",
        project_id,
        "--region",
        region,
    ]

    try:
        # Execute gcloud command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=get_gcloud_timeout(),
        )

        # Format response
        response = f"""Dataflow Job Cancellation Request:

Job ID: {job_id}
Project: {project_id}
Region: {region}
Status: Cancellation request submitted successfully

Note: The job may take some time to fully cancel. You can check the job status using
the check_dataflow_job_status tool to monitor the cancellation progress.

Command Output:
{result.stdout}"""

        return [types.TextContent(type="text", text=response)]

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to cancel job {job_id}:\n{e.stderr}"
        return [types.TextContent(type="text", text=error_msg)]


async def drain_dataflow_job(arguments: Dict[str, Any]) -> List[types.TextContent]:
    """
    Drain a streaming Dataflow job (graceful shutdown).
    """
    job_id = arguments["job_id"]
    project_id = arguments["project_id"]  # Now required
    region = arguments.get("region", "us-central1")  # Default to us-central1

    # Build gcloud command with required project_id and region
    cmd = [
        "gcloud",
        "dataflow",
        "jobs",
        "drain",
        job_id,
        "--project",
        project_id,
        "--region",
        region,
    ]

    try:
        # Execute gcloud command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            timeout=get_gcloud_timeout(),
        )

        # Format response
        response = f"""Dataflow Job Drain Request:

Job ID: {job_id}
Project: {project_id}
Region: {region}
Status: Drain request submitted successfully

Note: Draining allows the streaming job to finish processing current data and
stop gracefully. This is different from canceling, which stops the job immediately.
You can check the job status using the check_dataflow_job_status tool to
monitor the drain progress.

Command Output:
{result.stdout}"""

        return [types.TextContent(type="text", text=response)]

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to drain job {job_id}:\n{e.stderr}"
        return [types.TextContent(type="text", text=error_msg)]


async def main():
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="dataflow-mcp-server",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    import mcp.server.stdio

    asyncio.run(main())
