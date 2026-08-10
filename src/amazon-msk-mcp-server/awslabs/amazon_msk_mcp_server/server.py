# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""awslabs amazon-msk MCP Server implementation."""

from loguru import logger
from mcp.server.fastmcp import FastMCP
from typing import Literal


mcp = FastMCP(
    "awslabs.amazon-msk-mcp-server",
    instructions="Use this server to inspect Amazon MSK clusters at the Kafka broker level using the caller's AWS IAM credentials. Tools require a cluster_arn as their first argument. Provides read-only broker-direct operations: topic/partition state with real-time ISR and leader info, topic and broker configs with source attribution (DYNAMIC_TOPIC_CONFIG / DYNAMIC_BROKER_CONFIG / STATIC_BROKER_CONFIG / default), consumer group state and per-partition lag, in-progress partition reassignments, cluster-wide under-replicated partitions, broker connectivity diagnostics classified by NETWORK/TLS/SASL/PROTOCOL stage, per-partition offset lookups by timestamp or earliest/latest, and message reads bounded by max_messages/max_bytes/max_wait_ms. Message reads and ACL enumeration require --allow-sensitive-data-access flag (off by default). Server does NOT wrap AWS control-plane APIs like ListClusters or DescribeCluster (use the AWS MCP Server for that) and does NOT perform writes (no produce, no offset commits, no group mutation, no topic/ACL changes).",
    dependencies=[
        'pydantic',
        'loguru',
    ],
)


@mcp.tool(name='ExampleTool')
async def example_tool(
    query: str,
) -> str:
    """Example tool implementation.

    Replace this with your own tool implementation.
    """
    project_name = 'awslabs amazon-msk MCP Server'
    return (
        f"Hello from {project_name}! Your query was {query}. Replace this with your tool's logic"
    )


@mcp.tool(name='MathTool')
async def math_tool(
    operation: Literal['add', 'subtract', 'multiply', 'divide'],
    a: int | float,
    b: int | float,
) -> int | float:
    """Math tool implementation.

    This tool supports the following operations:
    - add
    - subtract
    - multiply
    - divide

    Parameters:
        operation (Literal["add", "subtract", "multiply", "divide"]): The operation to perform.
        a (int): The first number.
        b (int): The second number.

    Returns:
        The result of the operation.
    """
    match operation:
        case 'add':
            return a + b
        case 'subtract':
            return a - b
        case 'multiply':
            return a * b
        case 'divide':
            try:
                return a / b
            except ZeroDivisionError:
                raise ValueError(f'The denominator {b} cannot be zero.')
        case _:
            raise ValueError(
                f'Invalid operation: {operation} (must be one of: add, subtract, multiply, divide)'
            )


def main():
    """Run the MCP server with CLI argument support."""

    logger.trace('A trace message.')
    logger.debug('A debug message.')
    logger.info('An info message.')
    logger.success('A success message.')
    logger.warning('A warning message.')
    logger.error('An error message.')
    logger.critical('A critical message.')

    mcp.run()


if __name__ == '__main__':
    main()
