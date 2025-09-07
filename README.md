# Dataflow Agent - ADK MCP Implementation

This project implements a Google Agent Development Kit (ADK) agent that monitors Google Cloud Dataflow jobs using the Model Context Protocol (MCP). The agent can check job status, list jobs, and retrieve logs for failed jobs.

## Architecture

The implementation follows the ADK MCP integration pattern:

1. **MCP Server** (`mcp_servers/dataflow_jobs.py`): Wraps Google Cloud CLI commands for Dataflow operations
2. **ADK Agent** (`agent.py`): Uses MCPToolset to connect to the MCP server and provide intelligent job monitoring

## Features

- **Job Status Checking**: Get detailed status information for specific Dataflow jobs
- **Job Listing**: List active, terminated, or failed jobs with filtering options
- **Log Retrieval**: Fetch logs for failed jobs to identify error messages
- **Intelligent Analysis**: AI-powered analysis of job failures and troubleshooting suggestions

## Prerequisites

1. **Google Cloud SDK**: Install and configure gcloud CLI
   ```bash
   # Install Google Cloud SDK
   curl https://sdk.cloud.google.com | bash
   exec -l $SHELL

   # Authenticate and set project
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Python Environment**: Python 3.9 or higher
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **ADK Setup**: Follow the [ADK quickstart guide](https://google.github.io/adk-docs/quickstart/)

## Configuration

1. **Environment Variables**: Copy and configure the `.env` file:
   ```bash
   cp .env.example .env
   ```

   Update the following variables:
   ```env
   GOOGLE_CLOUD_PROJECT=your-project-id
   GOOGLE_CLOUD_REGION=us-central1
   GOOGLE_AI_API_KEY=your-google-ai-api-key
   ```

2. **Google Cloud Authentication**: Ensure you have proper authentication:
   ```bash
   # Option 1: Use gcloud default credentials
   gcloud auth application-default login

   # Option 2: Use service account key (set in .env)
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
   ```

## Usage

### Running with ADK Web

1. Start the ADK web interface:
   ```bash
   adk web
   ```

2. Navigate to the agent in your browser and interact with it using natural language:
   - "Check the status of job 2024-01-15_12_00_00-1234567890123456789"
   - "List all failed Dataflow jobs"
   - "Show me the logs for the failed job xyz-123"

### Direct Python Usage

```python
from dataflow_job_agent.agent import create_dataflow_agent

# Create the agent
agent = create_dataflow_agent()

# Example interaction
response = agent.run("Check the status of job 2024-01-15_12_00_00-1234567890123456789")
print(response)
```

## MCP Tools Available

The MCP server provides three main tools:

### 1. check_dataflow_job_status
- **Purpose**: Get detailed status of a specific job
- **Parameters**:
  - `job_id` (required): The Dataflow job ID
  - `project_id` (required): Google Cloud project ID
  - `region` (optional, default: us-central1): Google Cloud region

### 2. list_dataflow_jobs
- **Purpose**: List Dataflow jobs with filtering
- **Parameters**:
  - `project_id` (required): Google Cloud project ID
  - `region` (optional, default: us-central1): Google Cloud region
  - `status` (optional): Filter by status (active, terminated, failed, all)
  - `limit` (optional): Maximum number of jobs to return

### 3. get_dataflow_job_logs
- **Purpose**: Retrieve logs for a specific job
- **Parameters**:
  - `job_id` (required): The Dataflow job ID
  - `project_id` (required): Google Cloud project ID
  - `region` (optional, default: us-central1): Google Cloud region
  - `severity` (optional, default: INFO): Minimum log severity (DEBUG, INFO, WARNING, ERROR)

## Example Interactions

### Check Job Status
```
User: "What's the status of job 2024-01-15_12_00_00-1234567890123456789?"

Agent: I'll check the status of that Dataflow job for you.

[Agent uses check_dataflow_job_status tool]

Dataflow Job Status Report:

Job ID: 2024-01-15_12_00_00-1234567890123456789
Job Name: my-dataflow-pipeline
Current State: JOB_STATE_FAILED
Job Type: JOB_TYPE_BATCH
Location: us-central1
Created: 2024-01-15T12:00:00.000Z
Started: 2024-01-15T12:01:00.000Z
Ended: 2024-01-15T12:15:30.000Z

Error Information:
Stage 'Transform Data' failed

The job failed during the 'Transform Data' stage. Let me get the logs to provide more details about the failure.
```

### List Failed Jobs
```
User: "Show me all failed Dataflow jobs from today"

Agent: I'll list all failed Dataflow jobs for you.

[Agent uses list_dataflow_jobs tool with status='failed']

Dataflow Jobs (showing failed jobs):

• Job ID: 2024-01-15_12_00_00-1234567890123456789
  Name: my-dataflow-pipeline
  State: JOB_STATE_FAILED
  Type: JOB_TYPE_BATCH
  Created: 2024-01-15T12:00:00.000Z

• Job ID: 2024-01-15_10_30_00-9876543210987654321
  Name: data-processing-job
  State: JOB_STATE_FAILED
  Type: JOB_TYPE_STREAMING
  Created: 2024-01-15T10:30:00.000Z
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   ```bash
   gcloud auth application-default login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Permission Issues**:
   Ensure your account has the following IAM roles:
   - `roles/dataflow.viewer`
   - `roles/logging.viewer`
   - `roles/compute.viewer`

3. **MCP Server Connection Issues**:
   - Check that Python dependencies are installed
   - Verify the MCP server script path in `agent.py`
   - Check logs for detailed error messages

4. **Job Not Found**:
   - Verify the job ID is correct
   - Ensure you're checking the right project and region
   - Check if the job exists using `gcloud dataflow jobs list`

### Debug Mode

Enable debug logging by setting in your `.env` file:
```env
LOG_LEVEL=DEBUG
```

## Development

### Setup Development Environment

To set up the development environment with code formatting and linting:

```bash
# Run the setup script
./setup-dev.sh

# Or manually:
pip install -r requirements.txt
pre-commit install
pre-commit run --all-files
```

### Code Formatting

This project uses pre-commit hooks to ensure consistent code formatting:

- **Black**: Python code formatter
- **isort**: Import sorting
- **flake8**: Python linting
- **mypy**: Static type checking

**Manual formatting commands:**
```bash
black .                    # Format Python code
isort .                    # Sort imports
flake8 .                   # Lint Python code
mypy .                     # Type checking
pre-commit run --all-files # Run all hooks
```

### Project Structure
```
df-agent/
├── .env                                   # Environment configuration
├── requirements.txt                       # Python dependencies
├── README.md                              # This file
├── mcp_servers/
│   └── dataflow_jobs.py             # MCP server implementation (top-level)
└── dataflow_job_agent/
    └── agent.py                           # Main ADK agent
```

### Testing

Run tests with:
```bash
pytest tests/
```

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## References

- [Google Agent Development Kit Documentation](https://google.github.io/adk-docs/)
- [Model Context Protocol Specification](https://modelcontextprotocol.io/)
- [Google Cloud Dataflow Documentation](https://cloud.google.com/dataflow/docs)
- [ADK MCP Tools Guide](https://google.github.io/adk-docs/tools/mcp-tools/)
