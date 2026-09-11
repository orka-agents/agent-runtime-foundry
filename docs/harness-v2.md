# Orka harness v2

The v2 adapter is an ACP child of Orka's existing supervisor. A separate
Foundry broker owns Azure identity, remote session creation, and durable
cleanup records. The ACP child has no Azure credentials.

Use one Pod with a supervisor container and a broker sidecar, one replica,
and the Recreate strategy. The broker listens on `127.0.0.1:8091`. Its state
directory must be on a persistent volume that survives either container or
Pod replacement. Mount that volume only in the broker. Never share the
broker's Azure identity or durable state with the ACP child.

## Build and register

Create a nonsecret JSON configuration using
[`examples/foundry-acp.json`](../examples/foundry-acp.json). Pin a concrete
Hosted Agent version. Both processes verify the SHA-256 of the exact file
bytes. The target project, agent, version, model, and schema mode therefore
belong to the registered runtime profile. Per-Task configuration overrides
are rejected.

Build the configured Foundry source image:

```sh
docker buildx build --platform linux/amd64 -f Dockerfile.acp \
  --build-arg FOUNDRY_CONFIG=examples/foundry-acp.json \
  -t <registry>/foundry-configured:<tag> --push .
```

In the Orka checkout, compose the supervisor with that image:

```sh
make docker-build-acp-foundry-runtime \
  FOUNDRY_RUNTIME_IMAGE=<registry>/foundry-configured@sha256:<digest> \
  FOUNDRY_ADAPTER_DIGEST=sha256:<same-digest> \
  ACP_FOUNDRY_RUNTIME_IMG=<registry>/foundry-runtime:<tag>
```

Register the composed image's service as an external `orka.harness.v2`
AgentRuntime. Set `providerKind: foundry`, `adapterName: foundry-serve-acp`,
and `adapterDigest` to the configured Foundry source image digest. The
`agentConfigurationDigest` is `sha256:` plus the SHA-256 of `/agent/foundry.json`.
Set `supportsAgentSessionConfiguration: false`. Follow Orka's external v2
registration contract for the remaining profile, operation authentication,
controller epoch, workspace, and MCP settings.

The child accepts text prompts and resource links represented as text. It
uses the supervisor's HTTP MCP server for tools. It does not support image,
audio, embedded-resource, permission, terminal, filesystem, or session-load
ACP requests. Keep approval-required tools empty. The Task must use the
runtime profile's exact brokered tool allowlist.

The hosted agent's static function names must exactly match those MCP tools.
Use Orka's built-in names or the names of its Kubernetes Tool resources. The
child rejects unknown function names before calling a tool.
Tool results sent to Foundry contain only validated text content,
`structuredContent`, and `isError`. MCP metadata and unrecognized extension
fields are not forwarded.

## Process configuration

The supervisor starts the child as:

```text
/agent-runtime-foundry --protocol acp --config /agent/foundry.json
```

Orka supplies these child-only values:

| Variable | Value |
| --- | --- |
| `ORKA_FOUNDRY_ACP_PROVIDER_BASE_URL` | Per-session supervisor loopback proxy. |
| `ORKA_FOUNDRY_ACP_PROVIDER_TOKEN` | Ephemeral local proxy credential. |
| `ORKA_FOUNDRY_ACP_MODEL` | Model in the baked JSON. |
| `ORKA_FOUNDRY_ACP_AGENT_CONFIGURATION_DIGEST` | Exact baked-file SHA-256. |

Run the configured source image as the broker sidecar with arguments
`--protocol broker --config /agent/foundry.json`. Give it the same model and
configuration digest, plus:

| Variable | Value |
| --- | --- |
| `ORKA_FOUNDRY_BROKER_ADDR` | `127.0.0.1:8091`. |
| `ORKA_FOUNDRY_BROKER_STATE_DIR` | Absolute path on the broker-only persistent volume. |
| `ORKA_FOUNDRY_BROKER_BEARER_TOKEN` | At least 32 bytes, from a Kubernetes Secret. |
| `ORKA_FOUNDRY_BROKER_AGENTKIT_CONTINUATION_PROOF` | Optional shared secret for an AgentKit Hosted Agent's governed tool results. See below. |

Only the broker receives Azure Workload Identity or another refreshable
`DefaultAzureCredential` configuration. The initial implementation requires
Entra isolation. The supervisor's `ORKA_ACP_PROVIDER_PROXY_BASE_URL` points to
`http://127.0.0.1:8091/v1`; its provider token file contains the broker bearer.
Neither the broker bearer nor Azure identity enters the child environment.

For a Kubernetes exec readiness probe, run:

```text
/agent-runtime-foundry --protocol broker --health-check
```

This checks the configured loopback `/healthz` without loading Azure identity
or opening the ledger. Set the state directory to a private subdirectory of
the persistent volume, such as `/broker-state/ledger`, that the broker user
can create with mode `0700`.

The broker's persistent directory is private to its OS user. Its ledger
contains remote identifiers and ownership metadata, never prompts, tool
arguments, provider response bodies, Azure tokens, or local bearer tokens.
Do not delete or replace this ledger while it owns remote work.

## AgentKit tool workflows

Configure the hosted AgentKit agent with static `brokeredTools` and
`AGENTKIT_FOUNDRY_BROKERED_MODEL_LOOP=1`. Use an AgentKit version that supports
sequential tool rounds and set `toolSchemaMode` to `provider-static` in the
Foundry configuration. The hosted agent and Orka runtime must use the same
tool names. Each round proposes one tool; Orka executes it and the hosted agent
receives its result before deciding whether to call another tool or answer.

For an AgentKit Hosted Agent configured with brokered tools, give the broker
`ORKA_FOUNDRY_BROKER_AGENTKIT_CONTINUATION_PROOF` and give the hosted AgentKit
process `AGENTKIT_FOUNDRY_BROKERED_CONTINUATION_PROOF` with the same value. Use a
secret of at least 32 bytes, without whitespace or control characters. Keep it
in secret-backed environment variables for those two processes only. Never
put it in `foundry.json`, an image, an Orka Task, the supervisor environment,
or the ACP child environment. Use a separate secret for each deployment pair.

When the agent requests a tool, the ACP child calls Orka's session MCP server.
The broker validates the resulting request against its owner, prompt, lease,
previous response and pending call IDs before returning the result to AgentKit.
It then attaches the secret in the top-level `brokered_continuation_proof` JSON
field. Ordinary prompts do not carry it. The broker rejects proof fields or
proof headers supplied by its caller and never persists the secret in its
ledger or returns it to the ACP child.

AgentKit expects a result envelope with `approved` and either `output` or
`error`. The broker converts Orka's validated MCP result to this format.
An explicit `isError: false` becomes `approved: true` with the text and
structured content preserved under `output`. `isError: true` becomes
`approved: false` with a `brokered_tool_error` code and the validated error
text. Malformed results and missing error flags are rejected. MCP authorization
failures abort the tool call before any result is sent to AgentKit. The
`approved` field is AgentKit's result format; it does not report human approval.
Approval-required tools remain unsupported by this adapter.

This is the existing AgentKit shared-secret contract. It authenticates the
broker's continuation route; it is not a signed execution receipt. The broker's
ownership checks and AgentKit's session, pending-call and replay checks remain
required. The Foundry Responses gateway must preserve the body extension and
deliver it to the hosted wrapper. Local contract tests do not establish that
the deployed Foundry gateway forwards it; verify the configured agent version
before enabling its tools in a live runtime.

Leave the variable unset for other Hosted Agents. Their function output format
and requests remain unchanged. AgentKit must also support repeated tool rounds
for workflows that need several lookups before answering.

## Lifecycle guarantees and limits

The broker persists a random caller-chosen remote session ID before sending
create. Every inference request binds the exact Orka runtime fence, Task,
attempt, prompt digest, lease, invocation sequence, and request-body digest.
It never repeats an inference request after ambiguous acceptance. Remote
response IDs are represented to the child by owner-scoped opaque aliases.

Successful continuation retains only the last successful response alias.
Failed or cancelled prompts do not become successful conversation history.
`request` mode sends the discovered function schemas to Foundry. The configured
Hosted Agent version's Responses endpoint must accept top-level function tools;
container readiness and SDK support do not prove that Foundry ingress accepts
them. If that endpoint rejects request-level `tools`, use `provider-static` and
preconfigure matching schemas in the Hosted Agent. This mode omits request-level
schemas while retaining the current MCP allowlist.

Prompt completion and cancellation require remote settlement proof. Session
deletion also requires remote retirement proof. A closed HTTP connection,
local process death, or a single `404` is insufficient when a create or
inference request has an unresolved acceptance outcome. Those cases remain
blocked and Orka reports `OutcomeUnknown` without replaying the prompt.

Lease expiry and broker startup trigger cleanup of exactly owned sessions.
Kubernetes container-termination recovery does not apply to Foundry because
remote execution can survive the local container. Preserve unresolved
ownership records for investigation; do not fabricate retirement receipts
or remove finalizers to bypass them.

An authenticated drain can replace a surviving supervisor after a controller
epoch change. After a supervisor crash, Orka cannot import the broker's
old-owner proof through the current harness contract. That recovery remains
blocked even if the broker later contains the remote work.

## Verification

Run `make verify`. The ACP tests cover real stdio pipes, loopback provider and
MCP servers, continuation, cancellation, malformed Responses streams,
tool allowlists, and blocked output. Broker tests cover durable ownership,
lease cleanup, repeated controls, remote stop/delete proof, and ambiguous
acceptance. Live validation additionally requires the exact configured
Hosted Agent version and Azure identity.

For local tests against an AgentKit source checkout, install its common package
in a Python environment and run the opt-in integration tests:

```sh
export AGENTKIT_SOURCE_DIR=/path/to/agentkit
uv venv /tmp/foundry-agentkit-venv
export AGENTKIT_PYTHON=/tmp/foundry-agentkit-venv/bin/python
uv pip install --python "$AGENTKIT_PYTHON" -e "$AGENTKIT_SOURCE_DIR/runtimes/common"
go test ./internal/broker -run TestBrokerAgentKitHosted -count=1 -v
```

This runs the production hosted AgentKit server and model loop, the Foundry ACP
entrypoint over pipes, and the lifecycle broker. It checks two sequential tools,
tool-error recovery, authorization denial, response identity changes, and proof
isolation. A gateway that strips the proof is also tested to verify that no model
resume occurs. Cancellation cases hold the model connection open, wait for an
early hosted response acknowledgement, and verify that disconnect and lease
expiry close the model connection and allow proven retirement. A gateway that
loses the acknowledgement must leave the broker's ownership unresolved.

To include the native Microsoft Agent Framework path without brokered tools,
install its adapter and select that Python environment as well:

```sh
uv pip install --python "$AGENTKIT_PYTHON" -e "$AGENTKIT_SOURCE_DIR/runtimes/microsoft-agent-framework"
export AGENTKIT_MAF_PYTHON="$AGENTKIT_PYTHON"
go test ./internal/broker -run TestBrokerAgentKitHostedCancellation/native_disconnect -count=1 -v
```

This case uses the real MAF runtime against a held model connection and verifies
that cancellation closes that connection after the broker records the early
response ID. It is skipped when `AGENTKIT_MAF_PYTHON` is unset.

The model, MCP backend, supervisor context stamping, and Azure
session-management API are local fixtures. It requires no Azure or model credentials
and does not validate a deployed Orka controller or the public Foundry gateway.
These tests are skipped when `AGENTKIT_SOURCE_DIR` is unset.
