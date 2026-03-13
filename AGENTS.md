# Kestra Tika Plugin

## What

Extract text and metadata using Apache Tika in Kestra workflows. Exposes 1 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Apache Tika, allowing orchestration of Apache Tika-based operations as part of data pipelines and automation workflows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `tika`

### Key Plugin Classes

- `io.kestra.plugin.tika.Parse`

### Project Structure

```
plugin-tika/
├── src/main/java/io/kestra/plugin/tika/
├── src/test/java/io/kestra/plugin/tika/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
