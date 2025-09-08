# ./adk_agent_samples/dataflow_agent/agent.py
import os

from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset


# Create the Dataflow MCP agent
def create_dataflow_agent():
    """
    Create an ADK agent that can check Google Cloud Dataflow job status via MCP server.
    """

    # Configure MCP connection to our custom Dataflow MCP server
    mcp_connection = StdioConnectionParams(
        server_params={
            "command": "python",
            "args": [
                os.path.join(
                    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                    "mcp_servers",
                    "dataflow_jobs.py",
                )
            ],
            "env": os.environ.copy(),
        },
        timeout=30,  # Set timeout to 30 seconds
    )

    # Create McpToolset that connects to our Dataflow MCP server
    dataflow_mcp_toolset = McpToolset(
        connection_params=mcp_connection,
        tool_filter=None,  # Use all tools from the MCP server
    )

    # Create the LLM agent with the MCP toolset
    agent = LlmAgent(
        name="DataflowStatusAgent",
        model="gemini-2.5-pro",
        instruction="""
You are a Google Cloud Dataflow monitoring agent. Your primary responsibility is to:

1. Check the status of Dataflow jobs using the provided MCP tools
2. Return detailed information about job status, including:
   - Job ID and name
   - Current state (RUNNING, SUCCEEDED, FAILED, etc.)
   - Start and end times
   - Error messages if the job failed
   - Resource usage information

3. For failed jobs, provide:
   - Clear error messages
   - Failure reasons
   - Suggestions for troubleshooting when possible

4. Present information in a clear, structured format that's easy to understand

5. When listing jobs:
   - The default limit is 50 jobs
   - If users need more results, suggest they can increase the limit parameter
   - Example: "If you need to see more jobs, I can increase the limit (e.g., limit=100)"
   - Always mention the current limit when showing job lists

Always use the MCP tools to interact with Google Cloud Dataflow CLI commands.
""",
        tools=[dataflow_mcp_toolset],
    )

    return agent


# Export the agent for use with adk web
root_agent = create_dataflow_agent()
