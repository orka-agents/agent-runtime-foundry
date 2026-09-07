# Microsoft Foundry Hosted Agents adapter for Orka

This repository connects a deployed **Microsoft Foundry Hosted Agent** to Orka.
For harness v2, use the [ACP child and durable Foundry broker](docs/harness-v2.md)
with Orka's existing supervisor. The default HTTP entry point exposes the
Responses agent as an
[`orka.harness.v1`](https://github.com/orka-agents/orka/blob/main/website/docs/development/agent-runtime-adapter-contract.md)
`AgentRuntime` endpoint.

To run the supervisor and ACP child inside Foundry itself, use the
[Hosted Agent v2 package and Kubernetes gateway](docs/foundry-hosted-v2.md).

The adapter calls the Hosted Agent's dedicated Responses endpoint:

```text
{project_endpoint}/agents/{agent_name}/endpoint/protocols/openai/responses
```

It does not use the classic Threads/Runs API. The preserved classic adapter is
[`orka-agents/agent-runtime-foundry-classic`](https://github.com/orka-agents/agent-runtime-foundry-classic).

Foundry identifiers and Azure credentials stay inside this adapter deployment.
Orka continues to own task lifecycle, brokered tool policy, approvals,
idempotency, and result storage. Orka tool credentials and execution endpoints
are never sent to Foundry.

## Status

The adapter is experimental. Run a single replica. In harness v1, runtime-session and active
turn state are currently process-local, so a pod replacement cannot resume a
retained session or deduplicate an active turn. Orka facade samples use an external endpoint and do
not install or manage this adapter.

## Hosted Agent prerequisites

Deploy a Hosted Agent that exposes the Responses protocol. The agent container
must implement the Foundry Hosted Agent Responses contract (`POST /responses`
and `GET /readiness`). The adapter invokes the deployed agent through the
project endpoint. The default adapter runs outside Foundry; the optional
v2 hosted package runs its supervisor inside a separate Hosted Agent.

For Orka brokered-tool mode, use one of two schema delivery modes. The default
`request` mode requires the Hosted Agent endpoint to accept function tools on
each Responses request. `provider-static` mode is available for Hosted Agent
endpoints that reject request-level tools; in that mode the Hosted Agent must
preconfigure the function schemas. The adapter still rejects any function call
that was not supplied in the current Orka turn, and Orka still owns argument
validation, policy, approvals, credentials, execution, and audit. Brokered
profiles are disabled by default; enable only the classes and schema mode the
deployed agent has passed in conformance. Do not move Orka production tool
credentials into Foundry Toolbox or MCP merely to make a probe pass; that
changes the governance boundary.

## Configuration

| Environment variable | Purpose |
| --- | --- |
| `ORKA_FOUNDRY_ADAPTER_ADDR` | HTTP listen address, default `:8090`. |
| `ORKA_FOUNDRY_RUNTIME_NAME` | Runtime name advertised in `/v1/capabilities`. |
| `ORKA_FOUNDRY_ADAPTER_BEARER_TOKEN` | Bearer token Orka uses for mutating and streaming harness endpoints. |
| `ORKA_FOUNDRY_PROJECT_ENDPOINT` | Foundry project endpoint, for example `https://account.services.ai.azure.com/api/projects/project`. Required for readiness and version validation. |
| `ORKA_FOUNDRY_RESPONSES_ENDPOINT` | Optional full dedicated Responses endpoint. When omitted, the adapter derives it from the project endpoint and agent name. |
| `ORKA_FOUNDRY_AGENT_NAME` | Hosted Agent name (alphanumeric and hyphens, at most 63 characters). |
| `ORKA_FOUNDRY_AGENT_VERSION` | Optional concrete immutable agent version. When set, the adapter creates and reuses a version-pinned Foundry session. Aliases such as `@latest` are rejected. |
| `ORKA_FOUNDRY_API_VERSION` | Foundry REST API version, default `v1`. |
| `ORKA_FOUNDRY_TURN_TIMEOUT` | Absolute adapter maximum for one Orka turn, default `20s`. |
| `ORKA_FOUNDRY_ISOLATION_MODE` | `entra` (default) or `header`. In `header` mode, the adapter sends an opaque hash of Orka's runtime session ID as `x-ms-user-isolation-key`. |
| `ORKA_FOUNDRY_FEATURES` | Preview feature header value, default `HostedAgents=V1Preview`. Set an empty value only when the deployed API no longer requires the header. |
| `ORKA_FOUNDRY_BROKERED_TOOL_CLASSES` | Optional comma-separated classes to advertise and accept: `read`, `write`, or `read,write`. Empty by default (observed-only). Enable only after the Hosted Agent passes the matching conformance probes in the configured schema mode. |
| `ORKA_FOUNDRY_TOOL_SCHEMA_MODE` | Brokered schema delivery: `request` (default) sends Orka's safe function schemas on each Responses request; `provider-static` omits request-level tools and requires matching schemas to be preconfigured in the Hosted Agent. |

The adapter authenticates with Azure SDK `DefaultAzureCredential` and requests
the `https://ai.azure.com/.default` scope. In Kubernetes, use Azure Workload
Identity or another refreshable Entra credential supported by the Azure SDK.
Do not inject a static access token or put Foundry credentials in an Orka
`AgentRuntime` or `Task` resource.

Example deployment fragment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: foundry-hosted-runtime
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: foundry-hosted-runtime
  template:
    metadata:
      labels:
        app: foundry-hosted-runtime
        azure.workload.identity/use: "true"
    spec:
      serviceAccountName: foundry-hosted-runtime
      containers:
        - name: adapter
          image: ghcr.io/orka-agents/agent-runtime-foundry:latest
          env:
            - name: ORKA_FOUNDRY_PROJECT_ENDPOINT
              value: https://account.services.ai.azure.com/api/projects/project
            - name: ORKA_FOUNDRY_AGENT_NAME
              value: incident-scout
            - name: ORKA_FOUNDRY_AGENT_VERSION
              value: "2"
            - name: ORKA_FOUNDRY_ADAPTER_BEARER_TOKEN
              valueFrom:
                secretKeyRef:
                  name: foundry-hosted-runtime
                  key: token
```

## Protocol mapping

- `StartTurnRequest` starts a streaming Responses request with `stream: true` and
  `store: true`.
- `response.created` becomes `TurnStarted`.
- `response.output_text.delta` becomes `RuntimeOutput`.
- When explicitly enabled in `request` mode, request-provided safe Orka tool schemas become Responses function tools.
- In `provider-static` mode, the provider uses its preconfigured schemas while the adapter enforces every returned function name against the current turn's Orka-supplied allowlist.
- `function_call` output items become `ToolCallRequested` frames.
- `/v1/turns/{turnID}/continue` sends `function_call_output` items with the same
  `call_id` and chains them with `previous_response_id`.
- Completed responses become `TurnCompleted`; failed or incomplete responses
  become `TurnFailed`.
- Orka `RuntimeSessionID` maps to the latest Foundry `response.id` and
  `agent_session_id`. Follow-up Orka turns reuse both conversation and sandbox
  state.
- Cancellation closes the active synchronous Responses stream. The adapter does
  not call the background-response cancellation endpoint for synchronous turns.

The adapter deliberately omits downstream tool URLs, credentials, headers, and
secret references from Foundry requests.

## Readiness and conformance

`GET /v1/health` validates configuration, Azure credential acquisition, agent
existence, and (when configured) that the concrete agent version is active.
Orka's harness conformance probes then verify observed and brokered behavior.

```bash
make verify
```

The repository test suite uses fake Responses JSON/SSE servers and covers:

- SSE and JSON parsing, text deltas, and terminal states
- response and Hosted Agent session continuation
- function-call mapping and `function_call_output` continuation
- cancellation and timeouts
- failed and incomplete responses
- agent name and version validation
- malformed and oversized streams
- observed, brokered-read, and brokered-write `orka.harness.v1` conformance

A live Foundry E2E requires Azure access and a deployed test agent; it is not run
by default in CI.

## Build

```bash
make build
docker build -t ghcr.io/orka-agents/agent-runtime-foundry:latest .
```

## Orka facade

Deploy this adapter and its Kubernetes `Service` separately, then point an Orka
`AgentRuntime` at the Service using `deployment.mode: external-endpoint`. Orka's
facade samples live under `config/samples` and
`examples/fibey-custom-agent-demo`.

## Official Foundry references

- [Hosted agent runtime contract](https://learn.microsoft.com/azure/foundry/agents/concepts/hosted-agent-contract)
- [Hosted Agents concepts](https://learn.microsoft.com/azure/foundry/agents/concepts/hosted-agents)
- [Deploy a Hosted Agent](https://learn.microsoft.com/azure/foundry/agents/how-to/deploy-hosted-agent)
- [Manage Hosted Agent sessions](https://learn.microsoft.com/azure/foundry/agents/how-to/manage-hosted-sessions)
- [Foundry Agent Service Responses API](https://learn.microsoft.com/azure/foundry/openai/how-to/responses)
