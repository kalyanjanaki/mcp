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

import boto3
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka.admin import AdminClient
from loguru import logger
from mcp.server.fastmcp import FastMCP


mcp = FastMCP(
    'awslabs.amazon-msk-mcp-server',
    instructions=(
        "Use this server to inspect Amazon MSK clusters at the Kafka broker level "
        "using the caller's AWS IAM credentials. Tools require a cluster_arn as their "
        'first argument. Provides read-only broker-direct operations. Does NOT wrap AWS '
        'control-plane APIs (use the AWS MCP Server or aws-api-mcp-server for those) '
        'and does NOT perform writes. If a user references a cluster by name only, '
        'resolve the name to an ARN first (via kafka:ListClusters on the AWS MCP '
        'Server or aws-api-mcp-server), confirm the resolved ARN with the user, and '
        'then pass that ARN to this server.'
    ),
    dependencies=[
        'boto3',
        'confluent-kafka',
        'aws-msk-iam-sasl-signer-python',
    ],
)


def _region_from_arn(cluster_arn: str) -> str:
    # ARN shape: arn:aws:kafka:REGION:ACCOUNT:cluster/NAME/UUID
    parts = cluster_arn.split(':')
    if len(parts) < 4 or parts[0] != 'arn' or parts[2] != 'kafka':
        raise ValueError(f'Not a valid MSK cluster ARN: {cluster_arn}')
    return parts[3]


def _get_bootstrap_brokers(cluster_arn: str, region: str) -> str:
    """Resolve cluster_arn to a bootstrap broker string via the MSK control-plane API.

    Handles both provisioned and serverless clusters. Prefers the public IAM SASL
    endpoint when available (usable from outside the VPC), falling back to the
    private endpoint (usable from inside the VPC / VPN / bastion).
    """
    client = boto3.client('kafka', region_name=region)
    resp = client.get_bootstrap_brokers(ClusterArn=cluster_arn)
    brokers = (
        resp.get('BootstrapBrokerStringPublicSaslIam')
        or resp.get('BootstrapBrokerStringSaslIam')
    )
    if not brokers:
        raise RuntimeError(
            f'Cluster {cluster_arn} does not expose an IAM SASL bootstrap broker string. '
            f'Available keys: {sorted(k for k, v in resp.items() if v)}'
        )
    return brokers


def _oauth_cb(oauth_config, region: str):
    """Callback invoked by librdkafka to refresh the MSK OAUTHBEARER token."""
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
    return token, expiry_ms / 1000  # librdkafka wants seconds


def _admin_client_for(cluster_arn: str) -> AdminClient:
    region = _region_from_arn(cluster_arn)
    brokers = _get_bootstrap_brokers(cluster_arn, region)
    return AdminClient(
        {
            'bootstrap.servers': brokers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'OAUTHBEARER',
            'oauth_cb': lambda cfg: _oauth_cb(cfg, region),
        }
    )


@mcp.tool(name='list_topics')
async def list_topics(cluster_arn: str) -> dict:
    """List all topics on the MSK cluster identified by cluster_arn.

    Uses the caller's AWS credentials (from the SDK provider chain) to authenticate
    to the broker via IAM SASL/OAUTHBEARER. Broker-direct call — reflects current
    cluster state, not the ~1-minute-stale MSK Topic API.

    Args:
        cluster_arn: MSK cluster ARN, e.g.
            arn:aws:kafka:us-east-1:123456789012:cluster/my-cluster/uuid-N.
            If the user gave only a cluster name, resolve it to an ARN first
            (e.g., via kafka:ListClusters on the AWS MCP Server or
            aws-api-mcp-server) and confirm the resolved ARN with the user
            before invoking this tool.

    Returns:
        Dict with a `topics` list. Each entry: {name, partition_count, is_internal}.
    """
    admin = _admin_client_for(cluster_arn)
    md = admin.list_topics(timeout=10)
    topics = [
        {
            'name': t.topic,
            'partition_count': len(t.partitions),
            'is_internal': bool(getattr(t, 'is_internal', False)),
        }
        for t in md.topics.values()
    ]
    topics.sort(key=lambda t: t['name'])
    return {'cluster_arn': cluster_arn, 'topic_count': len(topics), 'topics': topics}


def main():
    """Run the MCP server."""
    logger.info('Starting awslabs.amazon-msk-mcp-server')
    mcp.run()


if __name__ == '__main__':
    main()
