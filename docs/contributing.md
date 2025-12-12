# Contributing

We have started welcoming minor bug fixes at this point. At some point, we will also open the project up for major contributions.

## Preparing to contribute

Multigres is a complex project. For making major contributions, a good deal of learning is required depending on the part of the project you'd like to contribute to.
We encourage you to take the time to learn. We'll also produce developer documentation to come up to speed on the different areas.
As we open the project for more contributions, pleaser reach out to us if interested.

In general, major contributions will required the creation of an RFC. You can do this by filing an issue. We'll make sure that the proposal fits the big picture. Then, you can start working on your change.

## Coding Standards

Multigres is mostly written in Go, and we follow the industry recommended [coding standards](https://go.dev/doc/effective_go).
As the document mentions, Go has evolved with newer constructs, and we prefer unsing such newer constructs where possible.
Most code editors automatically recommend such modernizations.

We have a few pre-commit hooks that perform some basic checks at commit time. When a pull request is created, we have a more elaborate set of linters that ensure that the code is up to standards.

## Testing

Multigres is designed to serve large scale mission critical workloads. This means that most features will have to be well-tested. How the system handles errors and failures is as important as how a feature works. For this reason, we typically require 100% code coverage. We have tools that measure and report the test coverage.

We also require integration tests that demonstrate how the feature works.

## Pull Requests

We follow [Conventional Commit](https://www.conventionalcommits.org/en/v1.0.0/) standards for pull requests.
