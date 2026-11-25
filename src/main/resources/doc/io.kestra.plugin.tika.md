# How to use the Tika plugin

Extract text content and metadata from documents (PDF, Word, Excel, images, and more) using Apache Tika from Kestra flows.

## Tasks

`Parse` parses a document and extracts its text and metadata — set `from` (required, a `kestra://` URI). Control output format with `contentType` (`XHTML` by default; also `TEXT` or `XHTML_NO_HEADER`). Set `extractEmbedded: true` to also extract embedded documents (default `false`). Set `charactersLimit` to cap extracted text length. Set `store: false` to return results inline rather than writing to internal storage (default `true`).

Configure OCR via the `ocrOptions` object: set `strategy` (default `NO_OCR`; set to `OCR_ONLY` or `OCR_AND_TEXT_EXTRACTION` to enable Tesseract), `language` (Tesseract language code), and `enableImagePreprocessing`.

When `store: true` (default), the output includes `uri` (ION file in internal storage). When `store: false`, the output includes `result` with `content` (extracted text), `metadata` (map of document properties), and `embedded` (map of embedded document name → URI).


## Metadata keys (Apache Tika 4)

Apache Tika 4 renamed most keys of `result.metadata`: for example `pdf:PDFVersion` is now `pdf:pdf-version`, `access_permission:can_print` is now `access-permission:can-print`, `width` is now `img:width` and `imagereader:NumImages` is now `tk:num-images`. Keys such as `Content-Type`, `tiff:*` and Dublin Core (`dc:*`) keep their names in both versions.

To keep existing flows working, `legacyMetadataKeys` is `true` by default and returns the Tika 3 key names. Set `legacyMetadataKeys: false` to get the Tika 4 names. The default may change to `false` in a future release, so new flows should set it to `false` and read the Tika 4 names.
