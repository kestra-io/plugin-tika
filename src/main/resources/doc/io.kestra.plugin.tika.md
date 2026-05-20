# How to use the Tika plugin

Extract text content and metadata from documents (PDF, Word, Excel, images, and more) using Apache Tika from Kestra flows.

## Tasks

`Parse` parses a document and extracts its text and metadata — set `from` (required, a `kestra://` URI). Control output format with `contentType` (`XHTML` by default; also `TEXT` or `XHTML_NO_HEADER`). Set `extractEmbedded: true` to also extract embedded documents (default `false`). Set `charactersLimit` to cap extracted text length. Set `store: false` to return results inline rather than writing to internal storage (default `true`).

Configure OCR via the `ocrOptions` object: set `strategy` (default `NO_OCR`; set to `OCR_ONLY` or `OCR_AND_TEXT_EXTRACTION` to enable Tesseract), `language` (Tesseract language code), and `enableImagePreprocessing`.

When `store: true` (default), the output includes `uri` (ION file in internal storage). When `store: false`, the output includes `result` with `content` (extracted text), `metadata` (map of document properties), and `embedded` (map of embedded document name → URI).
