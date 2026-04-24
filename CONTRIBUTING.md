# Contributing

Thank you for your interest in contributing to `nodex-proxy`!

This project is a high-performance blockchain JSON-RPC proxy server designed for load balancing, health checking, rate limiting, and observability. It helps distribute requests across a pool of blockchain nodes, enabling efficient traffic management.

We welcome contributions, whether it's improving documentation, fixing bugs, or adding new features. Please ensure your contributions align with the overall goals of this project: improving performance, reliability, and scalability.

## Getting Started

### Requirements

- Go (>= 1.22), see `go.mod`
- Docker (optional, for local deployment)
- Make (optional, for CI)
- GolangCI-Lint (for code linting)

### Install Dependencies

Clone the repository and install the dependencies:

```bash
git clone https://github.com/your-org/nodex-proxy.git
cd nodex-proxy
go mod download
make ci
```

## Development Workflow
Fork the repository.
Create a feature branch from main.
Make your changes.
Run the tests locally.
Open a pull request.

We encourage keeping pull requests small and focused, which will make the review process smoother.

## Local Checks (must pass)

Before submitting a pull request, make sure your code passes the following checks:

```bash
go fmt ./...                # Format code
golangci-lint run            # Lint the code
go test ./...                # Run all tests
```

## Code Guidelines

### General
- Write clean, readable, and well-documented code.
- Follow Go idioms, especially error handling and concurrency patterns.
- Prefer simple and explicit logic over complex abstractions.

### Performance
- Optimize the performance of the proxy under high load.
- Ensure the load balancing algorithm is efficient and scalable.
- Make sure rate limiting and traffic management do not cause unnecessary overhead.

### Distributed Systems
- Be cautious of distributed state and ensure consistency in traffic distribution.
- Ensure nodes are checked for availability and health before sending requests.
- Avoid unnecessary retries and ensure graceful degradation when nodes are unavailable.

### Observability
- Implement detailed and accurate metrics for monitoring the proxy's performance.
- Expose health checks and status endpoints to enable external monitoring tools.
- Log important events and errors for troubleshooting.

## Testing

All changes must include tests.

We recommend writing unit tests for business logic and integration tests for interactions with external systems like blockchain nodes and databases.

### Run Tests
```bash
go test ./...              # Run all tests
# make test
# make race
```

For integration tests that require external services, mark them with build tags:

```bash
//go:build integration
```

Run integration tests manually with:

```bash
go test -tags=integration ./...
```

### Formatting & Lint


Please ensure that your code follows the Go formatting rules and passes the linters:

```bash
go fmt ./...
golangci-lint run --timeout=5m
```

## Pull Requests

Before submitting a pull request, please:

- Ensure that all tests pass.
- Update the documentation if necessary.
- Write a clear and concise commit message.
- Link your pull request to any related issues.

Pull requests should include:

- A description of the changes made
- Justification for any performance optimizations or design decisions
- Any potential impacts or backward compatibility concerns

## Compatibility Policy
- Avoid making breaking changes to public APIs.
- If a breaking change is necessary, document it clearly and provide migration instructions.
- Ensure backward compatibility where possible.

## Commit Guidelines

Use clear, descriptive commit messages. Here’s an example format:

```bash
<type>: <short description>

<optional detailed description>

Fixes #<issue-number> (if applicable)
```

Types of commits:

- feat: A new feature.
- fix: A bug fix.
- docs: Documentation changes.
- style: Code style changes (no logic change).
- refactor: Refactoring code (no functionality change).
- test: Adding or modifying tests.
- chore: Miscellaneous changes (e.g., version bumps, dependencies).

Example:

```bash
feat: add support for rate limiting with custom window size
```

## Reporting Issues

Please include the following when reporting an issue:

- Your Go version
- nodex-proxy version (e.g., commit hash or tag)
- The steps to reproduce the issue
- Logs, stack traces, or error messages
- Any relevant configuration or setup details


## Security

Please follow responsible disclosure practices when discovering a security vulnerability. Do not disclose it publicly before notifying the maintainers.

See `SECURITY.md` for more details on how to report vulnerabilities.

## License

By contributing, you agree that your contributions will be licensed under the project's license.