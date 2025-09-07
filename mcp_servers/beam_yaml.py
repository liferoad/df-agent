#!/usr/bin/env python3
"""
MCP Server for Apache Beam YAML pipeline operations.
This server provides tools to interact with Beam YAML documentation
and generate pipelines.
"""

import asyncio
from typing import Any, Dict, List

import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.types import Tool

# Create MCP server instance
server = Server("beam-yaml-mcp-server")

# Base URL for Beam YAML documentation
BEAM_YAML_DOC_BASE = "https://beam.apache.org/releases/yamldoc/current/"


@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    """
    List available tools for Beam YAML operations.
    """
    return [
        Tool(
            name="get_beam_yaml_transforms",
            description=(
                "Get list of available Beam YAML transforms and " "their documentation"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "category": {
                        "type": "string",
                        "description": (
                            "Filter transforms by category " "(io, transform, etc.)"
                        ),
                        "enum": ["all", "io", "transform", "ml", "sql"],
                    }
                },
                "required": [],
            },
        ),
        Tool(
            name="get_transform_details",
            description=(
                "Get detailed documentation for a specific " "Beam YAML transform"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "transform_name": {
                        "type": "string",
                        "description": ("Name of the transform to get details for"),
                    }
                },
                "required": ["transform_name"],
            },
        ),
        Tool(
            name="validate_beam_yaml",
            description="Validate a Beam YAML pipeline configuration",
            inputSchema={
                "type": "object",
                "properties": {
                    "yaml_content": {
                        "type": "string",
                        "description": ("The YAML pipeline content to validate"),
                    }
                },
                "required": ["yaml_content"],
            },
        ),
        Tool(
            name="generate_beam_yaml_pipeline",
            description=("Generate a Beam YAML pipeline based on requirements"),
            inputSchema={
                "type": "object",
                "properties": {
                    "source_type": {
                        "type": "string",
                        "description": (
                            "Type of data source (e.g., BigQuery, " "PubSub, Text, CSV)"
                        ),
                    },
                    "sink_type": {
                        "type": "string",
                        "description": (
                            "Type of data sink (e.g., BigQuery, " "PubSub, Text, CSV)"
                        ),
                    },
                    "transformations": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "List of transformations to apply "
                            "(e.g., Filter, Map, Combine)"
                        ),
                    },
                    "description": {
                        "type": "string",
                        "description": (
                            "Natural language description of the "
                            "pipeline requirements"
                        ),
                    },
                },
                "required": ["description"],
            },
        ),
        Tool(
            name="get_io_connector_schema",
            description=(
                "Get input/output schema information for Beam YAML IO " "connectors"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "connector_name": {
                        "type": "string",
                        "description": (
                            "Name of the IO connector (e.g., "
                            "ReadFromBigQuery, WriteToText)"
                        ),
                    }
                },
                "required": ["connector_name"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: Dict[str, Any]
) -> List[types.TextContent]:
    """
    Handle tool calls for Beam YAML operations.
    """
    try:
        if name == "get_beam_yaml_transforms":
            return await get_beam_yaml_transforms(arguments)
        elif name == "get_transform_details":
            return await get_transform_details(arguments)
        elif name == "validate_beam_yaml":
            return await validate_beam_yaml(arguments)
        elif name == "generate_beam_yaml_pipeline":
            return await generate_beam_yaml_pipeline(arguments)
        elif name == "get_io_connector_schema":
            return await get_io_connector_schema(arguments)
        else:
            raise ValueError(f"Unknown tool: {name}")
    except Exception as e:
        return [
            types.TextContent(type="text", text=f"Error executing {name}: {str(e)}")
        ]


async def get_beam_yaml_transforms(
    arguments: Dict[str, Any]
) -> List[types.TextContent]:
    """
    Get list of available Beam YAML transforms.
    """
    category = arguments.get("category", "all")

    # Known transforms from Beam YAML documentation
    transforms: Dict[str, List[str]] = {
        "transform": [
            "AnomalyDetection",
            "AssertEqual",
            "AssignTimestamps",
            "Combine",
            "Create",
            "Enrichment",
            "Explode",
            "ExtractWindowingInfo",
            "Filter",
            "Flatten",
            "Join",
            "LogForTesting",
            "MLTransform",
            "MapToFields",
            "Partition",
            "PyTransform",
            "RunInference",
            "Sql",
            "StripErrorMetadata",
            "ValidateWithSchema",
            "WindowInto",
        ],
        "io": [
            "ReadFromAvro",
            "WriteToAvro",
            "ReadFromBigQuery",
            "WriteToBigQuery",
            "WriteToBigTable",
            "ReadFromCsv",
            "WriteToCsv",
            "ReadFromIceberg",
            "WriteToIceberg",
            "ReadFromJdbc",
            "WriteToJdbc",
            "ReadFromJson",
            "WriteToJson",
            "ReadFromKafka",
            "WriteToKafka",
            "ReadFromMySql",
            "WriteToMySql",
            "ReadFromOracle",
            "WriteToOracle",
            "ReadFromParquet",
            "WriteToParquet",
            "ReadFromPostgres",
            "WriteToPostgres",
            "ReadFromPubSub",
            "WriteToPubSub",
            "ReadFromPubSubLite",
            "WriteToPubSubLite",
            "ReadFromSpanner",
            "WriteToSpanner",
            "ReadFromSqlServer",
            "WriteToSqlServer",
            "ReadFromTFRecord",
            "WriteToTFRecord",
            "ReadFromText",
            "WriteToText",
        ],
    }

    if category == "all":
        all_transforms = []
        for cat, trans_list in transforms.items():
            all_transforms.extend([(t, cat) for t in trans_list])
        result = "Available Beam YAML Transforms:\n\n"
        for transform, cat in sorted(all_transforms):
            result += f"- {transform} ({cat})\n"
    else:
        trans_list = transforms.get(category, [])
        result = f"Available {category} transforms:\n\n"
        for transform in sorted(trans_list):
            result += f"- {transform}\n"

    return [types.TextContent(type="text", text=result)]


async def get_transform_details(arguments: Dict[str, Any]) -> List[types.TextContent]:
    """
    Get detailed documentation for a specific transform.
    """
    transform_name = arguments["transform_name"]

    # Transform documentation templates
    transform_docs: Dict[str, Dict[str, Any]] = {
        "Filter": {
            "description": "Filters elements based on a condition",
            "config": {
                "condition": "Expression or callable to filter elements",
                "language": "Language for the condition (python, javascript, etc.)",
            },
            "example": """
type: Filter
input: InputData
config:
  condition: "element.age > 18"
  language: python""",
        },
        "LogForTesting": {
            "description": "Logs elements for debugging and testing purposes",
            "config": {
                "level": "Log level (INFO, DEBUG, WARN, ERROR)",
                "prefix": "Optional prefix for log messages",
            },
            "example": """
type: LogForTesting
input: InputData
config:
  level: "INFO"
  prefix: "Processing: """,
        },
        "Combine": {
            "description": "Groups and combines records sharing common fields",
            "config": {
                "group_by": "Fields to group by",
                "combine": "Aggregation functions (sum, max, min, count, etc.)",
            },
            "example": """
type: Combine
input: InputData
config:
  group_by: ["category"]
  combine:
    total_sales:
      sum: "sales_amount"
    count:
      count: "*""",
        },
        "ReadFromBigQuery": {
            "description": "Reads data from Google BigQuery",
            "config": {
                "table": "BigQuery table reference (project:dataset.table)",
                "query": "SQL query to execute (alternative to table)",
                "use_standard_sql": "Whether to use standard SQL (default: true)",
            },
            "example": """
type: ReadFromBigQuery
config:
  table: "my-project:my_dataset.my_table"
# OR
type: ReadFromBigQuery
config:
  query: "SELECT * FROM `my-project.my_dataset.my_table` WHERE date > '2023-01-01'""",
        },
        "WriteToBigQuery": {
            "description": "Writes data to Google BigQuery",
            "config": {
                "table": "BigQuery table reference (project:dataset.table)",
                "create_disposition": "CREATE_IF_NEEDED or CREATE_NEVER",
                "write_disposition": "WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY",
            },
            "example": """
type: WriteToBigQuery
input: ProcessedData
config:
  table: "my-project:my_dataset.output_table"
  create_disposition: "CREATE_IF_NEEDED"
  write_disposition: "WRITE_APPEND""",
        },
    }

    if transform_name in transform_docs:
        doc = transform_docs[transform_name]
        result = f"Transform: {transform_name}\n\n"
        result += f"Description: {doc['description']}\n\n"
        result += "Configuration:\n"
        for param, desc in doc["config"].items():
            result += f"- {param}: {desc}\n"
        result += f"\nExample:\n```yaml\n{doc['example']}\n```"
    else:
        result = (
            f"Documentation for '{transform_name}' not found in local "
            "cache. This transform may exist but detailed documentation "
            "is not available."
        )

    return [types.TextContent(type="text", text=result)]


async def validate_beam_yaml(arguments: Dict[str, Any]) -> List[types.TextContent]:
    """
    Validate a Beam YAML pipeline configuration.
    """
    yaml_content = arguments["yaml_content"]

    try:
        import yaml  # type: ignore

        # Parse YAML
        pipeline_config = yaml.safe_load(yaml_content)

        # Basic validation
        errors = []
        warnings = []

        # Check if it's a valid pipeline structure
        if not isinstance(pipeline_config, dict):
            errors.append("Pipeline must be a YAML object/dictionary")
            return [
                types.TextContent(
                    type="text", text="Validation failed:\n" + "\n".join(errors)
                )
            ]

        # Check for pipeline or transforms key
        if "pipeline" not in pipeline_config and "transforms" not in pipeline_config:
            errors.append("Pipeline must contain either 'pipeline' or 'transforms' key")

        # Validate transforms structure
        transforms = pipeline_config.get("pipeline", {}).get(
            "transforms", pipeline_config.get("transforms", [])
        )

        if not isinstance(transforms, list):
            errors.append("Transforms must be a list")
        else:
            for i, transform in enumerate(transforms):
                if not isinstance(transform, dict):
                    errors.append(f"Transform {i} must be an object")
                    continue

                if "type" not in transform:
                    errors.append(f"Transform {i} missing required 'type' field")

                # Check for common configuration issues
                if transform.get("type") in ["ReadFromBigQuery", "WriteToBigQuery"]:
                    config = transform.get("config", {})
                    if not config.get("table") and not config.get("query"):
                        warnings.append(
                            f"Transform {i} ({transform.get('type')}) "
                            "should specify either 'table' or 'query'"
                        )

        # Prepare result
        if errors:
            result = "Validation failed:\n" + "\n".join(
                f"- {error}" for error in errors
            )
        else:
            result = "✅ YAML pipeline validation passed!"

        if warnings:
            result += "\n\nWarnings:\n" + "\n".join(
                f"- {warning}" for warning in warnings
            )

        return [types.TextContent(type="text", text=result)]

    except yaml.YAMLError as e:
        return [types.TextContent(type="text", text=f"YAML parsing error: {str(e)}")]
    except Exception as e:
        return [types.TextContent(type="text", text=f"Validation error: {str(e)}")]


async def generate_beam_yaml_pipeline(
    arguments: Dict[str, Any]
) -> List[types.TextContent]:
    """
    Generate a Beam YAML pipeline based on requirements.
    """
    # description = arguments["description"]  # Reserved for future use
    source_type = arguments.get("source_type")
    sink_type = arguments.get("sink_type")
    transformations = arguments.get("transformations", [])

    # Generate pipeline based on description and parameters
    pipeline_template: Dict[str, Any] = {"pipeline": {"transforms": []}}

    transforms = pipeline_template["pipeline"]["transforms"]

    # Add source
    if source_type:
        if source_type.lower() in ["bigquery", "bq"]:
            transforms.append(
                {
                    "name": "ReadData",
                    "type": "ReadFromBigQuery",
                    "config": {"table": "your-project:your_dataset.your_table"},
                }
            )
        elif source_type.lower() in ["pubsub", "pub/sub"]:
            transforms.append(
                {
                    "name": "ReadData",
                    "type": "ReadFromPubSub",
                    "config": {"topic": "projects/your-project/topics/your-topic"},
                }
            )
        elif source_type.lower() in ["text", "file"]:
            transforms.append(
                {
                    "name": "ReadData",
                    "type": "ReadFromText",
                    "config": {"path": "gs://your-bucket/input/*"},
                }
            )
        elif source_type.lower() == "csv":
            transforms.append(
                {
                    "name": "ReadData",
                    "type": "ReadFromCsv",
                    "config": {"path": "gs://your-bucket/input.csv"},
                }
            )

    # Add transformations
    for i, transformation in enumerate(transformations):
        if transformation.lower() == "filter":
            transforms.append(
                {
                    "name": f"Filter{i+1}",
                    "type": "Filter",
                    "input": transforms[-1]["name"] if transforms else "ReadData",
                    "config": {
                        "condition": "# Add your filter condition here",
                        "language": "python",
                    },
                }
            )
        elif transformation.lower() == "combine":
            transforms.append(
                {
                    "name": f"Combine{i+1}",
                    "type": "Combine",
                    "input": transforms[-1]["name"] if transforms else "ReadData",
                    "config": {
                        "group_by": ["# Add grouping fields"],
                        "combine": {"count": {"count": "*"}},
                    },
                }
            )

    # Add sink
    if sink_type:
        input_name = transforms[-1]["name"] if transforms else "ReadData"
        if sink_type.lower() in ["bigquery", "bq"]:
            transforms.append(
                {
                    "name": "WriteData",
                    "type": "WriteToBigQuery",
                    "input": input_name,
                    "config": {
                        "table": "your-project:your_dataset.output_table",
                        "create_disposition": "CREATE_IF_NEEDED",
                        "write_disposition": "WRITE_APPEND",
                    },
                }
            )
        elif sink_type.lower() in ["pubsub", "pub/sub"]:
            transforms.append(
                {
                    "name": "WriteData",
                    "type": "WriteToPubSub",
                    "input": input_name,
                    "config": {"topic": "projects/your-project/topics/output-topic"},
                }
            )
        elif sink_type.lower() in ["text", "file"]:
            transforms.append(
                {
                    "name": "WriteData",
                    "type": "WriteToText",
                    "input": input_name,
                    "config": {"path": "gs://your-bucket/output/"},
                }
            )

    # Convert to YAML string
    import yaml

    yaml_output = yaml.dump(
        pipeline_template, default_flow_style=False, sort_keys=False
    )

    result = f"Generated Beam YAML Pipeline:\n\n```yaml\n{yaml_output}```\n\n"
    result += "📝 **Next Steps:**\n"
    result += (
        "1. Replace placeholder values (your-project, your-dataset, etc.) "
        "with actual values\n"
    )
    result += (
        "2. Customize transform configurations based on your specific requirements\n"
    )
    result += "3. Validate the pipeline using the validate_beam_yaml tool\n"
    result += "4. Test the pipeline with a small dataset first"

    return [types.TextContent(type="text", text=result)]


async def get_io_connector_schema(arguments: Dict[str, Any]) -> List[types.TextContent]:
    """
    Get schema information for IO connectors.
    """
    connector_name = arguments["connector_name"]

    # Schema information for common IO connectors
    connector_schemas: Dict[str, Dict[str, Any]] = {
        "ReadFromBigQuery": {
            "output_schema": (
                "Depends on the BigQuery table schema - automatically "
                "inferred from table metadata"
            ),
            "config_schema": {
                "table": (
                    "string (project:dataset.table) - Required if query not "
                    "specified"
                ),
                "query": "string (SQL query) - Required if table not specified",
                "use_standard_sql": (
                    "boolean (default: true) - Use standard SQL syntax"
                ),
                "selected_fields": (
                    "array[string] (optional) - Specific fields to read"
                ),
                "row_restriction": ("string (optional) - WHERE clause for filtering"),
            },
            "example_output": (
                "Row(id: int64, name: string, timestamp: timestamp, " "amount: float64)"
            ),
        },
        "WriteToBigQuery": {
            "input_schema": "Beam Row with fields matching target table schema",
            "config_schema": {
                "table": ("string (project:dataset.table) - Target table reference"),
                "create_disposition": (
                    "string (CREATE_IF_NEEDED|CREATE_NEVER) - Table creation "
                    "behavior"
                ),
                "write_disposition": (
                    "string (WRITE_TRUNCATE|WRITE_APPEND|WRITE_EMPTY) - Write "
                    "behavior"
                ),
                "schema": (
                    "array[object] (optional) - Table schema if creating new " "table"
                ),
                "clustering_fields": (
                    "array[string] (optional) - Fields for table clustering"
                ),
                "time_partitioning": (
                    "object (optional) - Time partitioning configuration"
                ),
            },
            "example_input": (
                "Row(id: int64, name: string, timestamp: timestamp, " "amount: float64)"
            ),
        },
        "ReadFromText": {
            "output_schema": "Row(line: string) - Each line as a string field",
            "config_schema": {
                "path": "string (file path or pattern) - Input file location",
                "compression_type": (
                    "string (AUTO|UNCOMPRESSED|GZIP|BZIP2) - Compression format"
                ),
            },
            "example_output": "Row(line: 'sample text line')",
        },
        "WriteToText": {
            "input_schema": (
                "Any Beam type - will be converted to string representation"
            ),
            "config_schema": {
                "path": "string (output file path) - Output location",
                "file_name_suffix": ("string (optional) - Suffix for output files"),
                "num_shards": "integer (optional) - Number of output shards",
                "shard_name_template": (
                    "string (optional) - Template for shard naming"
                ),
            },
            "example_input": (
                "Row(message: 'Hello World') -> 'Row(message=Hello World)'"
            ),
        },
        "ReadFromCsv": {
            "output_schema": (
                "Row with fields based on CSV headers and inferred types"
            ),
            "config_schema": {
                "path": "string (CSV file path) - Input CSV location",
                "delimiter": "string (default: ',') - Field separator",
                "header": (
                    "boolean (default: true) - Whether first row contains " "headers"
                ),
                "schema": ("array[object] (optional) - Explicit schema definition"),
                "skip_blank_lines": ("boolean (default: true) - Skip empty lines"),
            },
            "example_output": (
                "Row(id: int64, name: string, age: int64, salary: float64)"
            ),
        },
        "WriteToCsv": {
            "input_schema": "Beam Row with named fields",
            "config_schema": {
                "path": "string (output CSV path) - Output location",
                "delimiter": "string (default: ',') - Field separator",
                "header": "boolean (default: true) - Include header row",
            },
            "example_input": "Row(id: int64, name: string, age: int64)",
        },
        "ReadFromPubSub": {
            "output_schema": (
                "Row(data: bytes, attributes: map[string, string], "
                "timestamp: timestamp)"
            ),
            "config_schema": {
                "topic": ("string (projects/project/topics/topic) - PubSub topic"),
                "subscription": (
                    "string (projects/project/subscriptions/sub) - PubSub "
                    "subscription"
                ),
                "id_label": ("string (optional) - Attribute for deduplication"),
                "timestamp_attribute": (
                    "string (optional) - Attribute containing event timestamp"
                ),
            },
            "example_output": (
                "Row(data: b'message content', attributes: {'key': 'value'}, "
                "timestamp: 2023-01-01T00:00:00Z)"
            ),
        },
        "WriteToPubSub": {
            "input_schema": (
                "Row with 'data' field (bytes) and optional 'attributes' " "field (map)"
            ),
            "config_schema": {
                "topic": (
                    "string (projects/project/topics/topic) - Target PubSub " "topic"
                ),
                "id_label": ("string (optional) - Attribute for deduplication"),
                "timestamp_attribute": (
                    "string (optional) - Attribute for event timestamp"
                ),
            },
            "example_input": (
                "Row(data: b'message', attributes: {'source': 'pipeline'})"
            ),
        },
        "ReadFromParquet": {
            "output_schema": "Row with fields based on Parquet schema",
            "config_schema": {
                "path": "string (parquet file path) - Input Parquet location",
                "columns": "array[string] (optional) - Specific columns to read",
            },
            "example_output": (
                "Row(id: int64, name: string, nested: Row(field1: string, "
                "field2: int64))"
            ),
        },
        "WriteToParquet": {
            "input_schema": "Beam Row with typed fields",
            "config_schema": {
                "path": "string (output parquet path) - Output location",
                "file_name_suffix": ("string (optional) - Suffix for output files"),
                "num_shards": "integer (optional) - Number of output shards",
            },
            "example_input": ("Row(id: int64, name: string, data: array[float64])"),
        },
        "ReadFromJson": {
            "output_schema": "Row with fields based on JSON structure",
            "config_schema": {
                "path": "string (JSON file path) - Input JSON location",
                "schema": ("object (optional) - Explicit schema for JSON parsing"),
            },
            "example_output": (
                "Row(id: int64, data: Row(name: string, values: array[int64]))"
            ),
        },
        "WriteToJson": {
            "input_schema": "Beam Row - will be serialized to JSON",
            "config_schema": {
                "path": "string (output JSON path) - Output location",
                "file_name_suffix": ("string (optional) - Suffix for output files"),
            },
            "example_input": "Row(id: 123, name: 'example', data: [1, 2, 3])",
        },
    }

    if connector_name in connector_schemas:
        schema_info = connector_schemas[connector_name]
        result = f"# Schema Information for {connector_name}\n\n"

        if "output_schema" in schema_info:
            result += f"## Output Schema\n{schema_info['output_schema']}\n\n"
            if "example_output" in schema_info:
                result += (
                    f"**Example Output:**\n```\n{schema_info['example_output']}"
                    f"\n```\n\n"
                )

        if "input_schema" in schema_info:
            result += f"## Input Schema\n{schema_info['input_schema']}\n\n"
            if "example_input" in schema_info:
                result += (
                    f"**Example Input:**\n```\n{schema_info['example_input']}"
                    f"\n```\n\n"
                )

        result += "## Configuration Parameters\n"
        for param, desc in schema_info["config_schema"].items():
            result += f"- **`{param}`**: {desc}\n"

        result += "\n## Usage Tips\n"
        if "BigQuery" in connector_name:
            result += "- Ensure your Google Cloud project has BigQuery API enabled\n"
            result += "- Use fully qualified table names (project:dataset.table)\n"
            result += (
                "- Consider using `selected_fields` for large tables to "
                "improve performance\n"
            )
        elif "PubSub" in connector_name:
            result += "- Ensure PubSub API is enabled in your Google Cloud project\n"
            result += (
                "- Use subscriptions for ReadFromPubSub to ensure message delivery\n"
            )
            result += "- Consider setting `id_label` for exactly-once processing\n"
        elif "Text" in connector_name:
            result += "- Supports local files and cloud storage (gs://, s3://, etc.)\n"
            result += "- Use glob patterns for reading multiple files\n"
        elif "Csv" in connector_name:
            result += "- Schema is automatically inferred from the first few rows\n"
            result += "- Use explicit schema for better type control\n"
    else:
        result = (
            f"Schema information for '{connector_name}' not found in local cache.\n\n"
        )
        result += "Available connectors with schema information:\n"
        for name in sorted(connector_schemas.keys()):
            result += f"- {name}\n"

    return [types.TextContent(type="text", text=result)]


async def main():
    # Import here to avoid issues with event loops
    import mcp.server.stdio

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="beam-yaml",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
