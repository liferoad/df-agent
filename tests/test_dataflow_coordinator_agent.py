#!/usr/bin/env python3
"""
Test suite for the Dataflow Coordinator Agent.
"""

import os
import sys

import pytest

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.dataflow_coordinator.agent import (  # noqa: E402
    create_dataflow_coordinator_agent,
)


class TestDataflowCoordinatorAgent:
    """Test cases for the Dataflow Coordinator Agent."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.agent = create_dataflow_coordinator_agent()

    def test_agent_creation(self):
        """Test that the coordinator agent can be created successfully."""
        assert self.agent is not None
        assert self.agent.name == "DataflowCoordinator"
        assert self.agent.model == "gemini-2.5-pro"

    def test_agent_has_sub_agents(self):
        """Test that the coordinator agent has the expected sub-agents."""
        assert hasattr(self.agent, "sub_agents")
        assert len(self.agent.sub_agents) == 2

        # Check sub-agent names
        sub_agent_names = [agent.name for agent in self.agent.sub_agents]
        assert "BeamYAMLPipelineAgent" in sub_agent_names
        assert "DataflowStatusAgent" in sub_agent_names

    def test_sub_agents_have_parent(self):
        """Test that sub-agents have the coordinator as their parent."""
        for sub_agent in self.agent.sub_agents:
            assert hasattr(sub_agent, "parent_agent")
            assert sub_agent.parent_agent == self.agent

    def test_agent_instruction(self):
        """Test that the coordinator agent has proper instructions."""
        instruction = self.agent.instruction
        assert "Dataflow Coordinator" in instruction
        assert "coordinate" in instruction.lower()
        assert "delegate" in instruction.lower()
        assert "BeamYAMLPipelineAgent" in instruction
        assert "DataflowStatusAgent" in instruction

    def test_agent_capabilities(self):
        """Test that the agent instruction covers all required capabilities."""
        instruction = self.agent.instruction.lower()

        # Check for key coordination capabilities
        assert "pipeline development workflow" in instruction
        assert "task delegation" in instruction
        assert "coordination patterns" in instruction
        assert "sequential workflow" in instruction
        assert "parallel tasks" in instruction
        assert "error handling" in instruction

    def test_agent_description(self):
        """Test that the agent has a proper description."""
        description = self.agent.description
        assert description is not None
        assert "coordinate" in description.lower()
        assert "Beam YAML" in description
        assert "Dataflow job management" in description

    def test_sub_agent_specialization(self):
        """Test that sub-agents maintain their specialized capabilities."""
        beam_yaml_agent = None
        dataflow_job_agent = None

        for sub_agent in self.agent.sub_agents:
            if sub_agent.name == "BeamYAMLPipelineAgent":
                beam_yaml_agent = sub_agent
            elif sub_agent.name == "DataflowStatusAgent":
                dataflow_job_agent = sub_agent

        # Test Beam YAML agent capabilities
        assert beam_yaml_agent is not None
        assert len(beam_yaml_agent.tools) > 0
        assert "Apache Beam YAML" in beam_yaml_agent.instruction

        # Test Dataflow job agent capabilities
        assert dataflow_job_agent is not None
        assert len(dataflow_job_agent.tools) > 0
        assert "Dataflow monitoring" in dataflow_job_agent.instruction

    def test_coordination_scenarios_in_instruction(self):
        """Test that the instruction includes coordination scenarios."""
        instruction = self.agent.instruction

        # Check for example scenarios
        assert "New Pipeline Development" in instruction
        assert "Pipeline Troubleshooting" in instruction
        assert "Performance Optimization" in instruction

        # Check for coordination flows
        assert "Coordination Flow:" in instruction
        assert "Delegate to BeamYAMLPipelineAgent" in instruction
        assert "Delegate to DataflowStatusAgent" in instruction

    def test_agent_hierarchy_structure(self):
        """Test that the agent hierarchy follows ADK patterns."""
        # Test that coordinator is the root
        assert (
            not hasattr(self.agent, "parent_agent") or self.agent.parent_agent is None
        )

        # Test that sub-agents can find each other through the parent
        beam_yaml_agent = self.agent.find_agent("BeamYAMLPipelineAgent")
        dataflow_job_agent = self.agent.find_agent("DataflowStatusAgent")

        assert beam_yaml_agent is not None
        assert dataflow_job_agent is not None
        assert beam_yaml_agent != dataflow_job_agent


class TestCoordinatorAgentIntegration:
    """Integration tests for the Dataflow Coordinator Agent."""

    def test_coordinator_agent_file_structure(self):
        """Test that the coordinator agent files are properly structured."""
        coordinator_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "agents",
            "dataflow_coordinator",
        )

        # Check that directory exists
        assert os.path.exists(
            coordinator_dir
        ), f"Coordinator directory not found at {coordinator_dir}"

        # Check that agent.py exists
        agent_file = os.path.join(coordinator_dir, "agent.py")
        assert os.path.exists(agent_file), f"Agent file not found at {agent_file}"

        # Check that __init__.py exists
        init_file = os.path.join(coordinator_dir, "__init__.py")
        assert os.path.exists(init_file), f"Init file not found at {init_file}"

    def test_coordinator_imports(self):
        """Test that the coordinator agent can be imported without errors."""
        try:
            from agents.dataflow_coordinator import root_agent
            from agents.dataflow_coordinator.agent import (
                create_dataflow_coordinator_agent,
            )

            # Test that functions exist
            assert callable(create_dataflow_coordinator_agent)
            assert root_agent is not None

        except ImportError as e:
            pytest.fail(f"Failed to import dataflow coordinator agent: {e}")

    def test_sub_agent_dependencies(self):
        """Test that sub-agent dependencies are properly resolved."""
        try:
            # Test that we can import the sub-agents
            from agents.beam_yaml_pipeline.agent import create_beam_yaml_agent
            from agents.dataflow_job_management.agent import create_dataflow_agent

            # Test that they can be created
            beam_agent = create_beam_yaml_agent()
            job_agent = create_dataflow_agent()

            assert beam_agent is not None
            assert job_agent is not None

        except ImportError as e:
            pytest.fail(f"Failed to import sub-agent dependencies: {e}")


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
