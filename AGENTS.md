# Kestra Tika Plugin

## What

- Provides plugin components under `io.kestra.plugin.tika`.
- Includes classes such as `Parse`.

## Why

- What user problem does this solve? Teams need to extract text and metadata from files using Apache Tika from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Apache Tika steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Apache Tika.

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

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
