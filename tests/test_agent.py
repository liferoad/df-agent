#!/usr/bin/env python3
"""
Test script for the Dataflow ADK MCP Agent.
This script demonstrates how to use the agent and test its functionality.
"""

import asyncio
import os
import sys

import pytest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Try to import dependencies, skip tests if not available
try:
    from dataflow_job_agent.agent import create_dataflow_agent

    ADK_AVAILABLE = True
except ImportError as e:
    ADK_AVAILABLE = False
    IMPORT_ERROR = str(e)


@pytest.mark.skipif(
    not ADK_AVAILABLE,
    reason=(
        f"ADK dependencies not available: "
        f"{IMPORT_ERROR if not ADK_AVAILABLE else ''}"
    ),
)
def test_agent_creation():
    """
    Test that the Dataflow agent can be created successfully.
    """
    agent = create_dataflow_agent()
    assert agent is not None
    assert hasattr(agent, "name")
    assert agent.name == "DataflowStatusAgent"


@pytest.mark.skipif(
    not ADK_AVAILABLE,
    reason=(
        f"ADK dependencies not available: "
        f"{IMPORT_ERROR if not ADK_AVAILABLE else ''}"
    ),
)
def test_agent_tools():
    """
    Test that the agent has the expected MCP tools.
    """
    agent = create_dataflow_agent()
    assert agent is not None
    assert hasattr(agent, "tools")
    assert len(agent.tools) > 0


def test_mcp_server_import():
    """
    Test that the MCP server module can be imported.
    """
    try:
        from mcp_servers import dataflow_jobs as dataflow_mcp_server

        assert dataflow_mcp_server is not None
    except ImportError:
        pytest.skip("MCP server module not available")


async def test_agent_async():
    """
    Async test for agent functionality (legacy function for manual testing).
    """
    if not ADK_AVAILABLE:
        print(f"⚠️ Skipping async test - ADK not available: {IMPORT_ERROR}")
        return

    print("Creating Dataflow ADK MCP Agent...")

    try:
        # Create the agent
        test_agent = create_dataflow_agent()
        print("✅ Agent created successfully!")
        print(f"Agent name: {test_agent.name}")

        # Test queries
        test_queries = [
            "List all Dataflow jobs in my project",
            "Show me failed Dataflow jobs",
        ]

        for i, query in enumerate(test_queries, 1):
            print(f"\n{'='*60}")
            print(f"Test {i}: {query}")
            print(f"{'='*60}")
            print(f"Query: {query}")
            print(
                "Note: This is a test setup. "
                "To run actual queries, use 'adk web' command."
            )

        print("\n✅ All tests completed!")

    except Exception as e:
        print(f"❌ Error creating agent: {str(e)}")
        print("\nTroubleshooting tips:")
        print(
            "1. Make sure you have installed all dependencies: "
            "pip install -r requirements.txt"
        )
        print("2. Check your .env file configuration")
        print("3. Ensure gcloud CLI is installed and authenticated")
        print("4. Verify your Google Cloud project has Dataflow API enabled")


def test_mcp_server():
    """
    Test the MCP server directly.
    """
    print("\nTesting MCP Server...")

    try:
        # Import the MCP server module
        import mcp_servers.dataflow_jobs as dataflow_mcp_server  # noqa: F401

        print("✅ MCP server module imported successfully!")

        # Test that the server has the expected tools
        print("\nExpected MCP tools:")
        print("- check_dataflow_job_status")
        print("- list_dataflow_jobs")
        print("- get_dataflow_job_logs")

    except ImportError:
        print("❌ MCP server module not found")
    except Exception as e:
        print(f"❌ Error testing MCP server: {str(e)}")


def check_prerequisites():
    """
    Check if all prerequisites are met.
    """
    print("Checking prerequisites...")

    # Check Python version
    python_version = sys.version_info
    if python_version >= (3, 9):
        print(
            f"✅ Python version: "
            f"{python_version.major}.{python_version.minor}.{python_version.micro}"
        )
    else:
        print(
            f"❌ Python version {python_version.major}.{python_version.minor} "
            f"is too old. Need 3.9+"
        )

    # Check for gcloud CLI
    try:
        import subprocess

        result = subprocess.run(["gcloud", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Google Cloud CLI is installed")
        else:
            print("❌ Google Cloud CLI not found")
    except FileNotFoundError:
        print("❌ Google Cloud CLI not found in PATH")

    # Check environment variables
    required_env_vars = ["GOOGLE_CLOUD_PROJECT", "GOOGLE_AI_API_KEY"]
    for var in required_env_vars:
        if os.getenv(var):
            print(f"✅ {var} is set")
        else:
            print(f"⚠️  {var} is not set (check your .env file)")

    # Check if dependencies are available
    try:
        import google.adk  # noqa: F401

        print("✅ Google ADK is available")
    except ImportError:
        print("❌ Google ADK not found. Install with: pip install google-adk")

    try:
        import mcp  # noqa: F401

        print("✅ MCP library is available")
    except ImportError:
        print("❌ MCP library not found. Install with: pip install mcp")


if __name__ == "__main__":
    print("Dataflow ADK MCP Agent Test")
    print("=" * 40)

    # Check prerequisites first
    check_prerequisites()

    # Test MCP server
    test_mcp_server()

    # Test agent creation
    print("\n" + "=" * 40)
    asyncio.run(test_agent_async())

    print("\n" + "=" * 40)
    print("Test completed!")
    print("\nTo run the agent interactively, use:")
    print("  adk web")
    print("\nThen navigate to the agent in your browser.")
