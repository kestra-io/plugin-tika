# Kestra Tika Plugin

## What

- Provides plugin components under `io.kestra.plugin.tika`.
- Includes classes such as `Parse`.

## Why

- This plugin integrates Kestra with Apache Tika.
- It provides tasks that extract text and metadata from files using Apache Tika.

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
