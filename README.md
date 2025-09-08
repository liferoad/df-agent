# DF-Agent - Multi-Purpose ADK MCP Implementation

This project implements multiple Google Agent Development Kit (ADK) agents using the Model Context Protocol (MCP) for various data processing and pipeline management tasks.

## Available Agents

### 1. Dataflow Coordinator Agent (Multi-Agent System)
A sophisticated multi-agent coordinator that orchestrates the complete Dataflow pipeline lifecycle using the ADK agent hierarchy pattern <mcreference link="https://google.github.io/adk-docs/agents/multi-agents/#agent-hierarchy-parent-agent-sub-agents" index="0">0</mcreference>. This coordinator manages two specialized sub-agents:

- **BeamYAMLPipelineAgent**: Handles pipeline generation and validation
- **DataflowStatusAgent**: Manages job monitoring and troubleshooting

The coordinator provides intelligent task delegation, end-to-end workflow management, and coordinated responses across both pipeline development and job management domains.

### 2. Dataflow Job Management Agent
Monitors Google Cloud Dataflow jobs with capabilities to check job status, list jobs, and retrieve logs for failed jobs.

### 3. Beam YAML Pipeline Agent
Generates, validates, and manages Apache Beam YAML pipelines with comprehensive support for creating data processing pipelines using natural language descriptions.

## Architecture

The implementation follows multiple ADK patterns:

### Multi-Agent System Architecture
The **Dataflow Coordinator Agent** implements the ADK multi-agent system pattern <mcreference link="https://google.github.io/adk-docs/agents/multi-agents/#agent-hierarchy-parent-agent-sub-agents" index="0">0</mcreference>:

1. **Parent Agent** (`agents/dataflow_coordinator/agent.py`): Coordinates and delegates tasks
2. **Sub-Agents**: Specialized agents with distinct capabilities
   - `BeamYAMLPipelineAgent`: Pipeline generation and validation
   - `DataflowStatusAgent`: Job monitoring and management
3. **Agent Hierarchy**: Parent-child relationships enable structured task delegation and context sharing

### Traditional MCP Integration Pattern
Individual agents follow the ADK MCP integration pattern:

1. **MCP Servers**: Wrap external tools and services
   - `mcp_servers/dataflow_jobs.py`: Google Cloud CLI commands for Dataflow operations
   - `mcp_servers/beam_yaml.py`: Beam YAML pipeline tools and validation
2. **ADK Agents**: Use McpToolset to connect to MCP servers and provide intelligent capabilities

## Features

### Dataflow Coordinator Agent (Multi-Agent System)
- **Intelligent Task Delegation**: Automatically routes requests to appropriate sub-agents based on context
- **End-to-End Workflow Management**: Coordinates complete pipeline lifecycle from generation to monitoring
- **Sequential & Parallel Coordination**: Manages complex workflows involving multiple agents
- **Cross-Domain Error Handling**: Coordinates troubleshooting across pipeline and job management domains
- **Unified Interface**: Single point of interaction for all Dataflow-related tasks
- **Agent Hierarchy Navigation**: Leverages ADK parent-child relationships for structured task management
- **Context Sharing**: Enables information flow between specialized sub-agents
- **Workflow Orchestration**: Supports both sequential pipeline development and parallel monitoring tasks

### Dataflow Job Management Agent
- **Job Status Checking**: Get detailed status information for specific Dataflow jobs
- **Job Listing**: List active, terminated, or failed jobs with filtering options
- **Log Retrieval**: Fetch logs for failed jobs to identify error messages
- **Intelligent Analysis**: AI-powered analysis of job failures and troubleshooting suggestions

### Beam YAML Pipeline Agent
- **Pipeline Generation**: Generate complete Beam YAML pipelines from natural language descriptions
- **Schema Management**: Look up input/output schemas for IO connectors with detailed documentation
- **Validation & Quality Assurance**: YAML syntax validation, pipeline structure validation, and error detection
- **Pipeline Submission**: Submit Beam YAML pipelines directly to Google Cloud Dataflow with comprehensive validation
- **Transform Documentation**: Comprehensive documentation for all Beam transforms with examples
- **Multi-Format Support**: Support for BigQuery, PubSub, CSV, Text, Parquet, JSON, and database connectors
- **Dry Run Validation**: Test pipeline configurations without actual submission to catch issues early

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

1. Start the ADK web interface from the agents directory:
   ```bash
   cd agents
   adk web
   ```

2. Navigate to the agent in your browser and interact with it using natural language:

   **Dataflow Coordinator Agent Examples:**
   - "I need to create a Beam YAML pipeline that reads from BigQuery and writes to PubSub, then monitor its execution"
   - "Generate a pipeline for real-time data processing and check if any similar jobs are currently running"
   - "My pipeline job failed, can you help troubleshoot and suggest improvements?"
   - "Create a batch processing pipeline and show me the status of all recent jobs"

   **Individual Agent Examples:**
   - "Check the status of job 2024-01-15_12_00_00-1234567890123456789"
   - "List all failed Dataflow jobs"
   - "Show me the logs for the failed job xyz-123"

### Direct Python Usage

#### Dataflow Coordinator Agent (Recommended)
```python
from agents.dataflow_coordinator.agent import create_dataflow_coordinator_agent

# Create the coordinator agent
coordinator = create_dataflow_coordinator_agent()

# The coordinator automatically delegates to appropriate sub-agents
# Pipeline generation requests → BeamYAMLPipelineAgent
# Job monitoring requests → DataflowStatusAgent
# Complex workflows → Coordinated execution

# Example: End-to-end workflow
response = coordinator.run(
    "Create a pipeline that processes streaming data from PubSub "
    "and then check if there are any similar jobs currently running"
)
print(response)
```

#### Dataflow Job Management Agent
```python
from agents.dataflow_job_management.agent import create_dataflow_agent

# Create the agent
agent = create_dataflow_agent()

# Example interaction
response = agent.run("Check the status of job 2024-01-15_12_00_00-1234567890123456789")
print(response)
```

#### Beam YAML Pipeline Agent
```python
from agents.beam_yaml_pipeline.agent import create_beam_yaml_agent

# Create the agent
agent = create_beam_yaml_agent()

# Example interactions
response = agent.run("Create a pipeline that reads from BigQuery, filters records where age > 18, and writes to PubSub")
print(response)

# Schema lookup
response = agent.run("Show me the schema for ReadFromBigQuery connector")
print(response)

# Pipeline validation
response = agent.run("Validate this YAML pipeline configuration: [YAML content]")
print(response)

# Pipeline submission to Dataflow
response = agent.run("Submit this pipeline to Dataflow with job name 'my-pipeline' in project 'my-gcp-project' using staging location 'gs://my-bucket/staging' and temp location 'gs://my-bucket/temp'")
print(response)

# Dry run validation before submission
response = agent.run("Do a dry run validation of this pipeline before submitting to Dataflow")
print(response)
```

## MCP Tools Available

### Dataflow Job Management Tools

The Dataflow MCP server provides three main tools:

#### 1. check_dataflow_job_status
- **Purpose**: Get detailed status of a specific job
- **Parameters**:
  - `job_id` (required): The Dataflow job ID
  - `project_id` (required): Google Cloud project ID
  - `region` (optional, default: us-central1): Google Cloud region

#### 2. list_dataflow_jobs
- **Purpose**: List Dataflow jobs with filtering
- **Parameters**:
  - `project_id` (required): Google Cloud project ID
  - `region` (optional, default: us-central1): Google Cloud region
  - `status` (optional): Filter by status (active, terminated, failed, all)
  - `limit` (optional): Maximum number of jobs to return

#### 3. get_dataflow_job_logs
- **Purpose**: Retrieve logs for a specific job
- **Parameters**:
  - `job_id` (required): The Dataflow job ID
  - `project_id` (required): Google Cloud project ID
  - `region` (optional, default: us-central1): Google Cloud region
  - `severity` (optional, default: INFO): Minimum log severity (DEBUG, INFO, WARNING, ERROR)

### Beam YAML Pipeline Tools

The Beam YAML MCP server provides six main tools:

#### 1. get_beam_yaml_transforms
- **Purpose**: List available Beam YAML transforms by category
- **Parameters**:
  - `category` (optional): Filter transforms by category (all, io, transform, ml, sql)

#### 2. get_transform_details
- **Purpose**: Get detailed documentation for a specific transform
- **Parameters**:
  - `transform_name` (required): Name of the transform to get details for

#### 3. validate_beam_yaml
- **Purpose**: Validate a Beam YAML pipeline configuration
- **Parameters**:
  - `yaml_content` (required): The YAML pipeline content to validate

#### 4. generate_beam_yaml_pipeline
- **Purpose**: Generate a Beam YAML pipeline based on requirements
- **Parameters**:
  - `description` (required): Natural language description of pipeline requirements
  - `source_type` (optional): Type of data source (BigQuery, PubSub, Text, CSV)
  - `sink_type` (optional): Type of data sink (BigQuery, PubSub, Text, CSV)
  - `transformations` (optional): List of transformations to apply

#### 5. get_io_connector_schema
- **Purpose**: Get input/output schema information for IO connectors
- **Parameters**:
  - `connector_name` (required): Name of the IO connector (e.g., ReadFromBigQuery, WriteToText)

#### 6. submit_dataflow_yaml_pipeline
- **Purpose**: Submit a Beam YAML pipeline to Google Cloud Dataflow using gcloud CLI
- **Parameters**:
  - `yaml_content` (required): The YAML pipeline content to submit
  - `job_name` (required): Name for the Dataflow job (must be unique)
  - `project_id` (required): Google Cloud project ID
  - `region` (optional, default: us-central1): Google Cloud region
  - `staging_location` (required): GCS bucket for staging files (gs://bucket-name/staging)
  - `temp_location` (required): GCS bucket for temporary files (gs://bucket-name/temp)
  - `service_account_email` (optional): Service account email for the job
  - `max_workers` (optional, default: 10): Maximum number of workers
  - `machine_type` (optional, default: n1-standard-1): Machine type for workers
  - `network` (optional): VPC network for the job
  - `subnetwork` (optional): VPC subnetwork for the job
  - `additional_experiments` (optional): Additional Dataflow experiments to enable
  - `dry_run` (optional, default: false): Validate the pipeline without actually submitting it

## Example Interactions

### Dataflow Job Management Agent

#### Check Job Status
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

#### List Failed Jobs
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

### Beam YAML Pipeline Agent

#### Pipeline Generation Request
```
User: "I need a pipeline that reads sales data from BigQuery, calculates daily totals by region, and writes the results to another BigQuery table."

Agent: I'll generate a complete Beam YAML pipeline for your sales data processing requirements.

[Agent uses generate_beam_yaml_pipeline tool]

Generated Beam YAML Pipeline:

```yaml
pipeline:
  transforms:
    - name: ReadSalesData
      type: ReadFromBigQuery
      config:
        table: "your-project:sales_dataset.daily_sales"
    - name: GroupByRegion
      type: Combine
      input: ReadSalesData
      config:
        group_by: ["region", "date"]
        combine:
          total_sales:
            sum: "sales_amount"
          transaction_count:
            count: "*"
    - name: WriteResults
      type: WriteToBigQuery
      input: GroupByRegion
      config:
        table: "your-project:analytics_dataset.regional_daily_totals"
        create_disposition: "CREATE_IF_NEEDED"
        write_disposition: "WRITE_APPEND"
```

📝 **Next Steps:**
1. Replace placeholder values (your-project, sales_dataset, etc.) with actual values
2. Customize transform configurations based on your specific requirements
3. Validate the pipeline using the validate_beam_yaml tool
4. Test the pipeline with a small dataset first
```

#### Schema Information Request
```
User: "What's the schema for ReadFromPubSub?"

Agent: I'll provide detailed schema information for the ReadFromPubSub connector.

[Agent uses get_io_connector_schema tool]

# Schema Information for ReadFromPubSub

## Output Schema
Row(data: bytes, attributes: map[string, string], timestamp: timestamp)

**Example Output:**
```
Row(data: b'message content', attributes: {'key': 'value'}, timestamp: 2023-01-01T00:00:00Z)
```

## Configuration Parameters
- **`topic`**: string (projects/project/topics/topic) - PubSub topic
- **`subscription`**: string (projects/project/subscriptions/sub) - PubSub subscription
- **`id_label`**: string (optional) - Attribute for deduplication
- **`timestamp_attribute`**: string (optional) - Attribute containing event timestamp

## Usage Tips
- Ensure PubSub API is enabled in your Google Cloud project
- Use subscriptions for ReadFromPubSub to ensure message delivery
- Consider setting `id_label` for exactly-once processing
```

#### Pipeline Validation
```
User: "Please validate this YAML pipeline configuration: [YAML content]"

Agent: I'll validate your Beam YAML pipeline configuration.

[Agent uses validate_beam_yaml tool]

✅ YAML pipeline validation passed!

Warnings:
- Transform 1 (ReadFromBigQuery) should specify either 'table' or 'query'
- Consider adding error handling transforms for production use

Recommendations:
- Add descriptive names to your transforms
- Include proper error handling and monitoring
- Test with a small dataset before full deployment
```

#### Transform Documentation Request
```
User: "Show me how to use the Filter transform"

Agent: I'll provide detailed documentation for the Filter transform.

[Agent uses get_transform_details tool]

Transform: Filter

Description: Filters elements based on a condition

Configuration:
- condition: Expression or callable to filter elements
- language: Language for the condition (python, javascript, etc.)

Example:
```yaml
type: Filter
input: InputData
config:
  condition: "element.age > 18"
  language: python
```
```

#### Pipeline Submission to Dataflow
```
User: "Submit this pipeline to Dataflow with job name 'sales-analysis-pipeline' in my project 'my-gcp-project'"

Agent: I'll submit your Beam YAML pipeline to Google Cloud Dataflow.

[Agent uses submit_dataflow_yaml_pipeline tool]

✅ Pipeline submitted successfully!

**Job Details:**
- Job Name: sales-analysis-pipeline
- Project: my-gcp-project
- Region: us-central1
- Staging Location: gs://my-bucket/staging
- Temp Location: gs://my-bucket/temp

**Monitoring:**
- Console: https://console.cloud.google.com/dataflow/jobs/us-central1/sales-analysis-pipeline?project=my-gcp-project
- CLI: `gcloud dataflow jobs describe sales-analysis-pipeline --region=us-central1 --project=my-gcp-project`
- Job ID: 2024-01-15_14_30_00-1234567890123456789

Your pipeline is now running on Dataflow. You can monitor its progress using the provided links.
```

#### Pipeline Dry Run Validation
```
User: "Can you do a dry run validation of this pipeline before submitting it?"

Agent: I'll perform a dry run validation of your pipeline to check for any issues before actual submission.

[Agent uses submit_dataflow_yaml_pipeline tool with dry_run=true]

✅ Pipeline validation successful!

**Job Details:**
- Job Name: sales-analysis-pipeline-dryrun
- Project: my-gcp-project
- Region: us-central1
- Staging Location: gs://my-bucket/staging
- Temp Location: gs://my-bucket/temp

The pipeline passed all validation checks. You can now submit it for actual execution by removing the dry-run flag.

**Validation Results:**
- YAML syntax: ✅ Valid
- Pipeline structure: ✅ Valid
- Resource requirements: ✅ Acceptable
- Permissions: ✅ Sufficient
- GCS locations: ✅ Accessible
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
   - `roles/dataflow.viewer` (for job monitoring)
   - `roles/dataflow.developer` (for pipeline submission)
   - `roles/logging.viewer`
   - `roles/compute.viewer`
   - `roles/storage.objectAdmin` (for GCS staging/temp locations)

3. **MCP Server Connection Issues**:
   - Check that Python dependencies are installed
   - Verify the MCP server script path in `agent.py`
   - Check logs for detailed error messages

4. **Job Not Found**:
   - Verify the job ID is correct
   - Ensure you're checking the right project and region
   - Check if the job exists using `gcloud dataflow jobs list`

5. **Pipeline Submission Issues**:
   - **gcloud CLI not found**: Install Google Cloud SDK and ensure it's in your PATH
   - **Authentication required**: Run `gcloud auth login` to authenticate
   - **Invalid GCS locations**: Ensure staging and temp locations start with `gs://` and buckets exist
   - **Job name conflicts**: Use unique job names or include timestamps
   - **Dataflow API not enabled**: Enable the Dataflow API in your Google Cloud project
   - **Invalid job name format**: Job names must start with lowercase letter, contain only lowercase letters, numbers, and hyphens
   - **Insufficient permissions**: Ensure service account has Dataflow Developer and Storage Object Admin roles
   - **Network/VPC issues**: Verify network and subnetwork configurations if specified
   - **Resource quotas**: Check if you have sufficient Dataflow job quotas in the target region

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
├── .env                                   # Environment variables and configuration
├── requirements.txt                       # Python package dependencies
├── README.md                              # Project documentation and setup guide
├── mcp_servers/
│   ├── dataflow_jobs.py                  # MCP server wrapping Google Cloud Dataflow CLI
│   └── beam_yaml.py                      # MCP server for Beam YAML pipeline operations
├── agents/
│   ├── dataflow_coordinator/
│   │   └── agent.py                       # ADK coordinator agent managing pipeline lifecycle
│   ├── dataflow_job_management/
│   │   └── agent.py                       # ADK agent with intelligent job monitoring
│   └── beam_yaml_pipeline/
│       └── agent.py                       # ADK agent for Beam YAML pipeline generation
└── tests/
    ├── test_dataflow_coordinator_agent.py # Test suite for coordinator agent functionality
    ├── test_job_agent.py                  # Test suite for Dataflow agent functionality
    └── test_beam_yaml_agent.py            # Test suite for Beam YAML agent functionality
```

### Testing

Run tests for all agents:
```bash
pytest tests/
```

Run tests for specific agents:
```bash
# Test Dataflow Coordinator Agent
pytest tests/test_dataflow_coordinator_agent.py -v

# Test Dataflow Job Management Agent
pytest tests/test_job_agent.py -v

# Test Beam YAML Pipeline Agent
pytest tests/test_beam_yaml_agent.py -v
```

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## References

- [Google Agent Development Kit Documentation](https://google.github.io/adk-docs/)
- [Model Context Protocol Specification](https://modelcontextprotocol.io/)
- [Google Cloud Dataflow Documentation](https://cloud.google.com/dataflow/docs)
- [ADK MCP Tools Guide](https://google.github.io/adk-docs/tools/mcp-tools/)
