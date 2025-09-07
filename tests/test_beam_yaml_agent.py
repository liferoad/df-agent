#!/usr/bin/env python3
"""
Test suite for the Beam YAML Pipeline Agent.
"""

import os
import sys

import pytest

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.beam_yaml_pipeline.agent import create_beam_yaml_agent  # noqa: E402


class TestBeamYAMLAgent:
    """Test cases for the Beam YAML Pipeline Agent."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.agent = create_beam_yaml_agent()

    def test_agent_creation(self):
        """Test that the agent can be created successfully."""
        assert self.agent is not None
        assert self.agent.name == "BeamYAMLPipelineAgent"
        assert self.agent.model == "gemini-2.5-pro"

    def test_agent_has_tools(self):
        """Test that the agent has the expected MCP tools."""
        assert len(self.agent.tools) > 0
        # The agent should have the Beam YAML MCP toolset
        toolset = self.agent.tools[0]
        assert toolset is not None

    def test_agent_instruction(self):
        """Test that the agent has proper instructions."""
        instruction = self.agent.instruction
        assert "Apache Beam YAML" in instruction
        assert "pipeline generation" in instruction
        assert "validation" in instruction
        assert "schema" in instruction

    def test_agent_capabilities(self):
        """Test that the agent instruction covers all required capabilities."""
        instruction = self.agent.instruction.lower()

        # Check for key capabilities mentioned in the requirements
        assert "generate" in instruction
        assert "validate" in instruction
        assert "schema" in instruction
        assert "bigquery" in instruction
        assert "pubsub" in instruction
        assert "transform" in instruction


class TestBeamYAMLMCPServer:
    """Test cases for the Beam YAML MCP Server functionality."""

    def test_mcp_server_path(self):
        """Test that the MCP server file exists."""
        mcp_server_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "mcp_servers",
            "beam_yaml.py",
        )
        assert os.path.exists(
            mcp_server_path
        ), f"MCP server file not found at {mcp_server_path}"

    def test_mcp_server_imports(self):
        """Test that the MCP server can be imported without errors."""
        try:
            # Add the mcp_servers directory to the path
            mcp_servers_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "mcp_servers",
            )
            sys.path.insert(0, mcp_servers_path)

            # Import the beam_yaml module
            import beam_yaml

            # Check that key functions exist
            assert hasattr(beam_yaml, "server")
            assert hasattr(beam_yaml, "handle_list_tools")
            assert hasattr(beam_yaml, "handle_call_tool")

        except ImportError as e:
            pytest.fail(f"Failed to import beam_yaml MCP server: {e}")


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
