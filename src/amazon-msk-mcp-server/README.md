# AWS Labs amazon-msk MCP Server

An AWS Labs Model Context Protocol (MCP) server for Amazon MSK broker-level operations via Kafka wire protocol

## Instructions

Use this server to inspect Amazon MSK clusters at the Kafka broker level using the caller's AWS IAM credentials. Tools require a cluster_arn as their first argument. Provides read-only broker-direct operations: topic/partition state with real-time ISR and leader info, topic and broker configs with source attribution (DYNAMIC_TOPIC_CONFIG / DYNAMIC_BROKER_CONFIG / STATIC_BROKER_CONFIG / default), consumer group state and per-partition lag, in-progress partition reassignments, cluster-wide under-replicated partitions, broker connectivity diagnostics classified by NETWORK/TLS/SASL/PROTOCOL stage, per-partition offset lookups by timestamp or earliest/latest, and message reads bounded by max_messages/max_bytes/max_wait_ms. Message reads and ACL enumeration require --allow-sensitive-data-access flag (off by default). Server does NOT wrap AWS control-plane APIs like ListClusters or DescribeCluster (use the AWS MCP Server for that) and does NOT perform writes (no produce, no offset commits, no group mutation

## TODO (REMOVE AFTER COMPLETING)

* [ ] Optionally add an ["RFC issue"](https://github.com/awslabs/mcp/issues) for the community to review
* [ ] Generate a `uv.lock` file with `uv sync` -> See [Getting Started](https://docs.astral.sh/uv/getting-started/)
* [ ] Remove the example tools in `./awslabs/amazon_msk_mcp_server/server.py`
* [ ] Add your own tool(s) following the [DESIGN_GUIDELINES.md](https://github.com/awslabs/mcp/blob/main/DESIGN_GUIDELINES.md)
* [ ] Keep test coverage at or above the `main` branch - NOTE: GitHub Actions run this command for CodeCov metrics `uv run --frozen pytest --cov --cov-branch --cov-report=term-missing`
* [ ] Document the MCP Server in this "README.md"
* [ ] Add a section for this amazon-msk MCP Server at the top level of this repository "../../README.md"
* [ ] Create the "../../docusaraus/docs/servers/amazon-msk-mcp-server.md" file with these contents:

    ```markdown
    ---
    title: amazon-msk MCP Server
    ---

    import ReadmeContent from "../../../src/amazon-msk-mcp-server/README.md";

    <div className="readme-content">
      <style>
        {`
        .readme-content h1:first-of-type {
          display: none;
        }
        `}
      </style>
      <ReadmeContent />
    </div>
    ```
  
* [ ] Reference within the "../../docusaraus/sidebars.ts" in the appropriate category.
* [ ] Add an entry to "../../docusaraus/statics/assets/server-cards.json" in the servers json. 



* [ ] Submit a PR and pass all the checks
