# Contributing to DataSlate

DataSlate is in its foundation phase. Contributions should move the shared product toward real, tested integrations. Start with the [capability inventory](docs/implementation/coverage.md), [development contract](AGENTS.md) and [foundation API contract](docs/implementation/foundation-contract.md).

For substantial changes, open a [GitHub issue](https://github.com/NimeshBuilds/dataslate/issues) with the problem, proposed behavior and integration boundary. Small fixes can go directly to a pull request. Keep changes focused and include the validation needed to assess them.

## Development

Follow the [README](README.md#run-the-application-for-development) to start the control plane and workspace. The component documentation covers API, browser, SDK and deployment checks. Run the checks relevant to your change; document any live-cluster or cloud validation that remains pending.

Never commit credentials or customer data. Examples must use placeholders or explicitly disposable test values. Preserve workspace authorization on every resource, including results, logs and AI memory. A preview must remain labeled as a preview; an unavailable backend must return a clear capability error rather than a fabricated success.

## Pull requests

Describe the user-visible problem, resulting behavior and tests performed. Link the issue when one exists. Add evidence for interface/version assumptions when integrating an upstream component. Declare new dependency licenses and preserve applicable attribution. Contributions are licensed under the repository's [Apache-2.0 license](LICENSE).

For vulnerabilities, follow [SECURITY.md](SECURITY.md) instead of posting exploit details in a public issue.
