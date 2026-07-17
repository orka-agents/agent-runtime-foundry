# AGENTS.md

This repository contains the Microsoft Foundry Hosted Agents Responses adapter for Orka's `orka.harness.v1` AgentRuntime contract.

## Development

- Do not commit, log, or print credentials, bearer tokens, session contents, or provider response bodies.
- Use Conventional Commit subjects and sign commits with `git commit -s`.
- Keep Foundry production credentials in Azure identity configuration; never pass them through Orka task specs.
- Run `make verify` after Go changes.
- Build the image with `docker build -t agent-runtime-foundry .`.
