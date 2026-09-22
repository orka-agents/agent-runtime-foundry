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
ACP requests. Orka can require human approval for brokered tools as described
below. This does not enable native ACP permission requests. The Task must use
the runtime profile's exact brokered tool allowlist.

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
text. Orka's recognized structured approval and execution outcomes retain their
specific code and use a fixed safe message. Malformed results and missing error
flags are rejected. MCP authorization failures abort the tool call before any
result is sent to AgentKit. The `approved` field is AgentKit's result format;
it does not report human approval, and `false` is a final outcome.

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

## Human approvals

Use matched builds containing [Orka's brokered approval support](https://github.com/orka-agents/orka/issues/582),
[this bridge's approval integration](https://github.com/orka-agents/agent-runtime-foundry/issues/4),
and [AgentKit's approval compatibility](https://github.com/sozercan/agentkit/issues/26).
Older bridges impose a two-minute tool limit. The Orka v2 runtime must advertise
`supportsBrokeredToolApprovals: true` for the qualified provider combination;
native `supportsPermissions` remains false. Validate the exact pinned Foundry
agent version and gateway before enabling approval-required tools.

Orka owns the review, authorized reviewers, saved decision, execution, and audit.
Configure an automatic lookup and a harmless counted action requiring approval
in Orka's tool policy. When the agent proposes the action, Orka's Task approval
API and panel show the proposed operation and safe input preview. The action's
execution count remains zero until an authorized reviewer approves it. Each later
approval-required action needs its own decision, including another invocation
of the same tool.

The existing MCP `tools/call` request stays open while the review is pending.
There is no interim result, polling model request, or resubmission of the prompt.
An approved call returns its actual result to the original pending function call
and previous response. Separate runtime sessions can progress during the wait.
Each child still admits at most two concurrent MCP calls.

| Operation | Maximum duration |
| --- | --- |
| Human review in Orka | 600 seconds |
| Approved tool execution in Orka | 240 seconds |
| One MCP `tools/call`, including review, execution, and delivery | 900 seconds |
| Model requests and MCP discovery | 120 seconds |
| Local connection and TLS handshake | 10 seconds each |
| Broker provisioning or cleanup operation | 45 seconds |

These are upper bounds. Task and session deadlines, cancellation, and loss of
the live broker lease can end a wait sooner. The supervisor must continue
renewing its lease while a tool waits; each lease grant remains limited to five
minutes. A longer tool wait does not lengthen any model, discovery, or cleanup
request. The bridge's 900-second limit is fixed in the qualified adapter build;
there is no timeout environment variable to pass to the child.

Configure both the pinned Foundry version's session idle timeout and AgentKit's
pending-response TTL to at least 1,800 seconds for the full review window.
Set `AGENTKIT_FOUNDRY_RESPONSE_STATE_TTL_SECONDS=1800` on the hosted AgentKit
process. It exposes the configured TTL as `foundryResponses.stateTtlSeconds` on
`/readiness`; use `AGENTKIT_FOUNDRY_RESPONSE_STATE_FILE` for its supported private,
single-writer persistent storage. [Foundry sessions](https://learn.microsoft.com/azure/foundry/agents/how-to/manage-hosted-sessions)
default to a 900-second idle timeout and accept 120 through 3,600 seconds.
Changing that setting requires a new agent version. Neither the default idle
timeout nor a 900-second AgentKit TTL leaves enough margin for the maximum tool
wait and model continuation. A suspended or lost hosted process must not cause
the bridge to repeat an action whose outcome is unknown.

Orka returns final errors with `isError: true` and an allowlisted
`structuredContent.code`, also marked `isError: true`. The AgentKit continuation
uses `approved: false` with that code and a fixed message:

| Code | Meaning |
| --- | --- |
| `approval_declined` | The reviewer declined the action; it did not execute. |
| `approval_expired` | The review expired before execution. |
| `approval_cancelled` | The unstarted action was cancelled. |
| `approval_stale` | The recorded decision no longer authorizes the proposed action. |
| `tool_execution_failed` | An approved execution failed. |
| `tool_outcome_unknown` | Execution may have happened; do not repeat the action. |

The bridge never interprets tool text as an approval decision. Recognized codes
discard raw error text and private metadata when converting to AgentKit's
envelope. Other admitted tool errors keep the existing `brokered_tool_error`
format. HTTP or JSON-RPC authorization errors remain fatal protocol errors.
Cancellation and transport failures close the wait without a continuation;
a late decision cannot reopen that ACP session. Broker restart closes the old
prompt's authority while retaining its ownership evidence.

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
Kubernetes container termination alone cannot prove Foundry cleanup because
remote execution can survive the local container. Preserve unresolved
ownership records; do not fabricate retirement receipts or remove finalizers
to bypass them.

The broker supports recovery through a replacement Orka supervisor with
authenticated `GET /internal/v1/identity` and `POST /internal/v1/retire-boot`
controls. Both require the lifecycle bearer. The identity response binds a
stable `ledgerIdentityDigest` to `agentConfigurationDigest`. Orka must witness
that identity before admission, retain it with the original boot fence, and
prove termination of the original supervisor before requesting recovery.
A fresh or replaced ledger cannot satisfy that witness.

Boot retirement accepts the protocol, witnessed ledger and configuration
digests, exact pool-wide `retiredFence`, and `operationID` in a strict JSON
body. It durably seals the boot before cancelling work or contacting Foundry.
The seal covers every matching owner, including renewal-only owners and
in-flight creation. Delayed admission for that boot is permanently rejected.
The broker returns complete proof only after every owner has real retirement
evidence and no pending creation, ambiguous request, or active invocation
remains. An empty owner set is valid only against the witnessed ledger and
its persisted seal. Missing history never creates an owner as recovery proof.

The response binds the exact request-body digest, operation, ledger, retired
fence, and sorted owner-set digest. Its canonical proof digest covers the
public retirement evidence; a fresh retry operation preserves the same seal
and completed proof. Uncertain operations remain blocked across retries and
restarts. The broker does not replay inference.

Upgrading a legacy ledger adds a fresh durable identity while preserving its
owners. That identity supports future enrollment and does not establish a
witness for an earlier boot. A legacy ledger already at its byte limit keeps
exact-owner cleanup available but cannot advertise an unpersisted identity.

The broker writes one bounded JSON diagnostic to stderr when a dispatched
response fails. It records the failure stage, outer HTTP status, invocation
sequence, hashed owner, and whether a response acknowledgement was persisted.
An observed terminal frame adds its status and an allowlisted AgentKit error
code. The optional `error.upstream_status` is recorded only as an integer from
400 through 599. Unknown error codes become `unknown`; provider messages,
response bodies, URLs, remote IDs, headers, and credentials are excluded.
These diagnostics leave ownership and cleanup decisions to the existing
durable evidence and settlement checks.

An authenticated drain can replace a surviving supervisor after a controller
epoch change. Recovery after supervisor loss requires Orka's enrolled broker
identity, exact termination witness, and authenticated boot-retirement proof.
Without those witnesses, recovery remains blocked even if the broker later
contains the remote work.

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
isolation. Counted approval fixtures hold the first call, verify zero execution
and no model continuation, then exercise approval, decline, expiry, and a tool
failure after approval. A gateway that strips the proof is also tested to verify that no model
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

The ordinary Go suite also exercises the full 900-second transport budget with
virtual time, approval cancellation, independent sessions, distinct decisions
for sequential calls, lost tool results, rolling leases, and broker restart.
These tests use fixtures for the human decision and do not establish that the
configured Foundry gateway supports a live review.

For deployment acceptance, run Orka's real Task and approval APIs against the
configured Foundry gateway with a disposable Task and a harmless counted tool.
Record the visible pending approval with count zero, the saved decision, count
one after approval, continuation of the original conversation, and cleanup.
Repeat with a declined review and a cancelled Task; both must keep the unstarted
action's count at zero, including after a late decision. Keep the record free of
credentials and private review metadata. Passing the local fixtures does not
complete this live check.
