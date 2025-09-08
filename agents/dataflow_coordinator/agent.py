# ./agents/dataflow_coordinator/agent.py
import importlib.util
import os

from google.adk.agents import LlmAgent


def create_dataflow_coordinator_agent():
    """
    Create a multi-agent coordinator that orchestrates Beam YAML pipeline generation
    and Dataflow job management using the ADK agent hierarchy pattern.

    This coordinator agent manages two specialized sub-agents:
    - BeamYAMLPipelineAgent: For generating and validating Beam YAML pipelines
    - DataflowStatusAgent: For monitoring and managing Dataflow jobs
    """

    # Import and create the specialized sub-agents dynamically
    # This avoids import path issues when loaded by ADK web

    # Import beam_yaml_pipeline agent
    beam_yaml_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "beam_yaml_pipeline", "agent.py"
    )
    spec = importlib.util.spec_from_file_location("beam_yaml_agent", beam_yaml_path)
    beam_yaml_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(beam_yaml_module)
    beam_yaml_agent = beam_yaml_module.create_beam_yaml_agent()

    # Import dataflow_job_management agent
    dataflow_job_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "dataflow_job_management",
        "agent.py",
    )
    spec = importlib.util.spec_from_file_location(
        "dataflow_job_agent", dataflow_job_path
    )
    dataflow_job_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(dataflow_job_module)
    dataflow_job_agent = dataflow_job_module.create_dataflow_agent()

    # Create the coordinator agent with sub-agents
    coordinator = LlmAgent(
        name="DataflowCoordinator",
        model="gemini-2.5-pro",
        description="I coordinate Beam YAML pipeline generation and Dataflow job "
        "management tasks.",
        instruction="""You are a Google Cloud Dataflow Coordinator Agent that
manages the complete lifecycle of Apache Beam pipelines from YAML generation
to job execution monitoring.

Your primary responsibilities include:

1. **Pipeline Development Workflow**:
   - Analyze user requirements for data processing pipelines
   - Delegate YAML pipeline generation to the BeamYAMLPipelineAgent
   - Coordinate between pipeline creation and job management tasks
   - Provide end-to-end guidance from development to deployment

2. **Task Delegation Strategy**:
   - **For YAML pipeline tasks**: Delegate to BeamYAMLPipelineAgent
     * Pipeline generation and validation
     * Transform documentation and schema lookup
     * Best practices and optimization guidance

   - **For job monitoring tasks**: Delegate to DataflowStatusAgent
     * Job status checking and monitoring
     * Error analysis and troubleshooting
     * Resource usage information

3. **Coordination Patterns**:
   - **Sequential Workflow**: Pipeline generation → Job deployment → Monitoring
   - **Parallel Tasks**: Generate multiple pipelines while monitoring existing jobs
   - **Error Handling**: Coordinate between agents when issues span both domains

4. **User Interaction Guidelines**:
   - Understand user intent and route requests to appropriate sub-agents
   - Synthesize responses from multiple agents when needed
   - Provide comprehensive solutions that may involve both pipeline and job aspects
   - Maintain context across the entire pipeline lifecycle

**Example Coordination Scenarios**:

**Scenario 1: New Pipeline Development**
```
User: "I need to create a pipeline that reads from BigQuery and writes to PubSub"

Coordination Flow:
1. Delegate to BeamYAMLPipelineAgent for YAML generation
2. Once pipeline is ready, provide deployment guidance
3. After deployment, delegate to DataflowStatusAgent for monitoring
```

**Scenario 2: Pipeline Troubleshooting**
```
User: "My pipeline is failing, can you help?"

Coordination Flow:
1. Delegate to DataflowStatusAgent to check job status and errors
2. If errors relate to pipeline configuration, delegate to BeamYAMLPipelineAgent
3. Coordinate fixes between pipeline updates and job redeployment
```

**Scenario 3: Performance Optimization**
```
User: "How can I optimize my running pipeline?"

Coordination Flow:
1. Delegate to DataflowStatusAgent for current performance metrics
2. Delegate to BeamYAMLPipelineAgent for optimization recommendations
3. Coordinate implementation of improvements
```

**Key Coordination Principles**:
- Always identify which domain(s) a request involves
- Delegate specific technical tasks to specialized agents
- Synthesize information from multiple agents into coherent responses
- Maintain awareness of the complete pipeline lifecycle
- Provide clear next steps that may involve multiple agents

**Response Format**:
- Clearly indicate when delegating to sub-agents
- Synthesize responses from multiple agents when applicable
- Provide comprehensive guidance that spans the entire workflow
- Include specific next steps for implementation

Remember: You coordinate and delegate, but the specialized agents handle
the technical implementation details within their domains.
""",
        sub_agents=[beam_yaml_agent, dataflow_job_agent],
    )

    return coordinator


# Export the coordinator agent for use with adk web
root_agent = create_dataflow_coordinator_agent()
