#!/usr/bin/env python3
"""
Test suite for the Beam YAML Guide Agent.
This module tests the step-by-step pipeline creation functionality.
"""

import os
import sys
import unittest
from unittest.mock import patch

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from agents.beam_yaml_guide.agent import create_beam_yaml_guide_agent
except ImportError:
    # Fallback if path setup doesn't work
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from agents.beam_yaml_guide.agent import create_beam_yaml_guide_agent


class TestBeamYAMLGuideAgent(unittest.TestCase):
    """
    Test cases for the Beam YAML Guide Agent functionality.
    """

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.agent = create_beam_yaml_guide_agent()

    def test_agent_creation(self):
        """Test that the agent is created successfully."""
        self.assertIsNotNone(self.agent)
        self.assertEqual(self.agent.name, "BeamYAMLGuideAgent")
        self.assertEqual(self.agent.model, "gemini-2.5-pro")

    def test_agent_has_tools(self):
        """Test that the agent has the required MCP toolset."""
        self.assertTrue(hasattr(self.agent, "tools"))
        self.assertGreater(len(self.agent.tools), 0)

    def test_agent_instruction_content(self):
        """Test that the agent instruction contains key guidance elements."""
        instruction = self.agent.instruction

        # Check for key workflow phases
        self.assertIn("Phase 1: Requirements Gathering", instruction)
        self.assertIn("Phase 2: Data Source Configuration", instruction)
        self.assertIn("Phase 3: Data Transformation Design", instruction)
        self.assertIn("Phase 4: Output Destination Setup", instruction)
        self.assertIn("Phase 5: Pipeline Generation & Validation", instruction)

        # Check for interaction guidelines
        self.assertIn("INTERACTION GUIDELINES", instruction)
        self.assertIn("one focused question at a time", instruction)

        # Check for technical requirements
        self.assertIn("TECHNICAL REQUIREMENTS", instruction)
        self.assertIn("ReadFromBigQuery", instruction)
        self.assertIn("LogForTesting", instruction)

        # Check for validation requirements
        self.assertIn("Always include required parameters", instruction)
        self.assertIn("Validate field references", instruction)

    def test_agent_transform_validation(self):
        """Test that the agent instruction includes proper transform validation."""
        instruction = self.agent.instruction

        # Check that it specifies valid transforms
        self.assertIn("ReadFromBigQuery, WriteToBigQuery", instruction)
        self.assertIn("Filter, MapToFields, Combine", instruction)

        # Check that it warns against invalid transforms
        self.assertIn("LogForTesting (NOT 'Log')", instruction)

    def test_agent_yaml_generation_requirements(self):
        """Test that the agent instruction includes YAML generation standards."""
        instruction = self.agent.instruction

        # Check for YAML formatting requirements
        self.assertIn("YAML Generation Standards", instruction)
        self.assertIn("proper YAML formatting", instruction)
        self.assertIn("consistent indentation", instruction)
        self.assertIn("descriptive transform names", instruction)

    def test_agent_error_handling_guidance(self):
        """Test that the agent instruction includes error handling guidance."""
        instruction = self.agent.instruction

        # Check for error handling instructions
        self.assertIn("Error Handling", instruction)
        self.assertIn("Specific error descriptions", instruction)
        self.assertIn("Clear correction instructions", instruction)
        self.assertIn("Re-prompt for corrected information", instruction)

    def test_agent_example_interaction_flow(self):
        """Test that the agent instruction includes example interaction patterns."""
        instruction = self.agent.instruction

        # Check for example interaction flow
        self.assertIn("EXAMPLE INTERACTION FLOW", instruction)
        self.assertIn("Welcome! I'll help you create", instruction)
        self.assertIn("Great! Let's start with", instruction)

    def test_agent_configuration_validation(self):
        """Test that the agent instruction emphasizes configuration validation."""
        instruction = self.agent.instruction

        # Check for validation requirements
        self.assertIn("Configuration Validation", instruction)
        self.assertIn("Always include required parameters", instruction)
        self.assertIn("Validate field references", instruction)
        self.assertIn("Check parameter formats", instruction)
        self.assertIn("Ensure proper connectivity", instruction)

    @patch("os.path.join")
    def test_mcp_server_path_construction(self, mock_join):
        """Test that the MCP server path is constructed correctly."""
        # Mock the path construction
        mock_join.return_value = "/mocked/path/beam_yaml.py"

        # Create agent (this will trigger path construction)
        agent = create_beam_yaml_guide_agent()

        # Verify that os.path.join was called to construct the MCP server path
        self.assertTrue(mock_join.called)

        # Verify the agent was created successfully
        self.assertIsNotNone(agent)

    def test_agent_systematic_approach(self):
        """Test that the agent instruction emphasizes systematic approach."""
        instruction = self.agent.instruction

        # Check for systematic approach principles
        self.assertIn("KEY PRINCIPLES", instruction)
        self.assertIn("Systematic Approach", instruction)
        self.assertIn("Follow the phase-by-phase workflow", instruction)
        self.assertIn("User-Friendly", instruction)
        self.assertIn("Thorough Validation", instruction)

    def test_agent_response_format_guidance(self):
        """Test that the agent instruction includes response format guidance."""
        instruction = self.agent.instruction

        # Check for response format guidelines
        self.assertIn("RESPONSE FORMAT", instruction)
        self.assertIn("During Information Gathering", instruction)
        self.assertIn("For Generated YAML", instruction)
        self.assertIn("properly formatted code blocks", instruction)


class TestBeamYAMLGuideAgentIntegration(unittest.TestCase):
    """
    Integration tests for the Beam YAML Guide Agent with MCP server.
    """

    def setUp(self):
        """Set up test fixtures for integration tests."""
        self.agent = create_beam_yaml_guide_agent()

    def test_mcp_toolset_configuration(self):
        """Test that the MCP toolset is configured correctly."""
        # Check that the agent has tools configured
        self.assertTrue(hasattr(self.agent, "tools"))
        self.assertIsNotNone(self.agent.tools)

        # The tools should be a list containing the MCP toolset
        self.assertIsInstance(self.agent.tools, list)
        self.assertGreater(len(self.agent.tools), 0)

    def test_agent_timeout_configuration(self):
        """Test that the MCP connection timeout is configured appropriately."""
        # This is more of a structural test to ensure the agent creation
        # doesn't fail due to timeout configuration issues
        agent = create_beam_yaml_guide_agent()
        self.assertIsNotNone(agent)


class TestBeamYAMLGuideAgentWorkflow(unittest.TestCase):
    """
    Test cases for the agent's workflow and guidance capabilities.
    """

    def setUp(self):
        """Set up test fixtures for workflow tests."""
        self.agent = create_beam_yaml_guide_agent()

    def test_workflow_phases_coverage(self):
        """Test that all required workflow phases are covered in instructions."""
        instruction = self.agent.instruction

        required_phases = [
            "Requirements Gathering",
            "Data Source Configuration",
            "Data Transformation Design",
            "Output Destination Setup",
            "Pipeline Generation & Validation",
        ]

        for phase in required_phases:
            self.assertIn(phase, instruction, f"Missing workflow phase: {phase}")

    def test_data_source_options_coverage(self):
        """Test that major data source options are mentioned."""
        instruction = self.agent.instruction

        data_sources = ["BigQuery", "PubSub", "Cloud Storage", "Kafka"]

        for source in data_sources:
            self.assertIn(source, instruction, f"Missing data source: {source}")

    def test_transform_categories_coverage(self):
        """Test that major transform categories are covered."""
        instruction = self.agent.instruction

        transforms = ["Filter", "MapToFields", "Combine", "Join", "WindowInto"]

        for transform in transforms:
            self.assertIn(transform, instruction, f"Missing transform: {transform}")


if __name__ == "__main__":
    # Run the tests
    unittest.main(verbosity=2)
