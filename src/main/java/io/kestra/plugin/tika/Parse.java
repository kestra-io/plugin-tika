package io.kestra.plugin.tika;

import ch.qos.logback.classic.LoggerContext;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.image.ImageParser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.parser.txt.TXTParser;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.DefaultHandler;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.tika.io.TikaInputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Parse files with Apache Tika",
    description = "Auto-detects MIME type, extracts text and metadata, and can capture embedded files. Defaults to XHTML content, no OCR, and stores the parsed Ion payload to internal storage unless `store` is false."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Extract text and embedded files from an upload.",
            code = """
                id: tika_parse_file
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: parse
                    type: io.kestra.plugin.tika.Parse
                    from: "{{ inputs.file }}"
                    extractEmbedded: true
                    store: false

                  - id: log_embedded
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.parse.result.embedded }}"
                """
        ),
        @Example(
            full = true,
            title = "Extract text from an image using OCR.",
            code = """
                id: tika_parse_image_ocr
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: parse
                    type: io.kestra.plugin.tika.Parse
                    from: "{{ inputs.file }}"
                    ocrOptions:
                      strategy: OCR_AND_TEXT_EXTRACTION
                    store: true
                """
        ),
        @Example(
            full = true,
            title = "Download and extract image metadata using Apache Tika.",
            code = """
                id: parse-image-metadata-using-apache-tika
                namespace: company.team

                tasks:
                  - id: get_image
                    type: io.kestra.plugin.core.http.Download
                    uri: https://kestra.io/cdn-cgi/image/onerror=redirect,width=1080,height=608,fit=cover,format=webp/_astro/main.C_OjFrVt.jpg

                  - id: tika
                    type: io.kestra.plugin.tika.Parse
                    from: "{{ outputs.get_image.uri }}"
                    store: false
                    contentType: TEXT
                    ocrOptions:
                      strategy: OCR_AND_TEXT_EXTRACTION

                  - id: log_metadata
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.tika.result.metadata }}"
                """
        ),
        @Example(
            full = true,
            title = "Download a PDF file and extract text from it using Apache Tika.",
            code = """
                id: parse-pdf
                namespace: company.team

                tasks:
                  - id: download_pdf
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/resolve/main/pdf/app_store.pdf

                  - id: parse_text
                    type: io.kestra.plugin.tika.Parse
                    from: "{{ outputs.download_pdf.uri }}"
                    contentType: TEXT
                    store: false

                  - id: log_extracted_text
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.parse_text.result.content }}"
                """
        )
    }
)
public class Parse extends Task implements RunnableTask<Parse.Output> {
    @Schema(
        title = "Source file to parse",
        description = "Internal storage URI (e.g. `kestra://...`)."
    )
    @PluginProperty(internalStorageURI = true, group = "source")
    private Property<String> from;

    @Schema(
        title = "Extract embedded files",
        description = "If true, inline/embedded resources are saved to internal storage and returned in `embedded`; default is false."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> extractEmbedded = Property.ofValue(false);

    @Schema(
        title = "Output content format",
        description = "Choose `TEXT`, `XHTML` (default), or `XHTML_NO_HEADER`. `charactersLimit` applies only to `TEXT`."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<ContentType> contentType = Property.ofValue(ContentType.XHTML);

    @Schema(
        title = "Custom OCR options",
        description = "Install [Tesseract](https://cwiki.apache.org/confluence/display/TIKA/TikaOCR) to enable OCR. Default strategy is `NO_OCR`."
    )
    @PluginProperty(dynamic = false, group = "advanced")
    @Builder.Default
    private OcrOptions ocrOptions = OcrOptions.builder()
        .strategy(Property.ofValue(PDFParserConfig.OCR_STRATEGY.NO_OCR))
        .build();

    @Schema(
        title = "Store parsed payload to internal storage",
        description = "When true (default), writes the parsed Ion file to internal storage and returns its URI; when false, emits the result inline."
    )
    @Builder.Default
    @PluginProperty(group = "destination")
    protected final Property<Boolean> store = Property.ofValue(true);

    @PluginProperty(group = "processing")
    @Schema(
        title = "Character write limit",
        description = "Maximum characters when writing TEXT content; -1 (default) disables the limit."
    )
    private Property<Integer> charactersLimit;

    static {
        LoggerFactory.getLogger("org.apache.pdfbox");
        LoggerFactory.getLogger("org.apache.tika");

        ((LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory())
            .getLoggerList()
            .stream()
            .filter(logger -> logger.getName().startsWith("org.apache.pdfbox") ||
                logger.getName().startsWith("org.apache.tika")
            )
            .forEach(
                logger -> logger.setLevel(ch.qos.logback.classic.Level.ERROR)
            );
    }

    @Override
    public Parse.Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // Build Tika config once
        TikaConfig config = new TikaConfig(this.getClass().getClassLoader());

        // Resolve source URI
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        // Media type detection (separate light pass)
        // We detect upfront to decide whether to force PDFParser
        MediaType mediaType;
        try (InputStream detect = new BufferedInputStream(runContext.storage().getFile(from))) {
            mediaType = config.getDetector().detect(detect, new Metadata());
        }

        // Resolve OCR strategy early — needed to pick the right image parser below
        PDFParserConfig.OCR_STRATEGY ocrStrategy = runContext.render(ocrOptions.getStrategy())
            .as(PDFParserConfig.OCR_STRATEGY.class)
            .orElse(PDFParserConfig.OCR_STRATEGY.NO_OCR);

        // Choose parser
        // IMPORTANT: force specific parsers to avoid the empty-output regression
        // that occurs with AutoDetectParser inside a shadow JAR on some MIME types
        Parser parser;
        // Track whether ImageParser is the effective parser so we can apply the
        // metadata-as-content fallback after parsing (ImageParser writes nothing to
        // the ContentHandler — it only populates Metadata with EXIF/image fields).
        boolean useImageParserFallback = false;
        if (mediaType != null
            && "application".equals(mediaType.getType())
            && "pdf".equals(mediaType.getSubtype())) {
            parser = new PDFParser();
        } else if (mediaType != null && "image".equals(mediaType.getType())) {
            // Use TesseractOCRParser when OCR is requested; ImageParser only extracts metadata.
            // In Tika 3.x, TesseractOCRParser uses instance-level setSkipOCR(), not ParseContext
            // TesseractOCRConfig — so we must configure it explicitly on the instance.
            if (ocrStrategy != PDFParserConfig.OCR_STRATEGY.NO_OCR) {
                org.apache.tika.parser.ocr.TesseractOCRParser tesseractParser =
                    new org.apache.tika.parser.ocr.TesseractOCRParser();
                // initialize() must be called before use when not going through TikaConfig;
                // otherwise hasTesseract() returns false and OCR is silently skipped.
                tesseractParser.initialize(java.util.Collections.emptyMap());
                if (tesseractParser.hasTesseract()) {
                    tesseractParser.setSkipOCR(false);
                    parser = tesseractParser;
                } else {
                    // Tesseract not available: fall back to ImageParser for metadata extraction.
                    // The metadata-as-content fallback below will provide a useful text representation.
                    parser = new ImageParser();
                    useImageParserFallback = true;
                }
            } else {
                // NO_OCR on an image: ImageParser extracts EXIF/format metadata only.
                parser = new ImageParser();
                useImageParserFallback = true;
            }
        } else if (mediaType != null
            && "text".equals(mediaType.getType())
            && "plain".equals(mediaType.getSubtype())) {
            parser = new TXTParser();
        } else {
            parser = new AutoDetectParser(config);
        }

        Metadata metadata = new Metadata();
        // When using a specific parser (not AutoDetectParser), pre-populate Content-Type
        // so the parser knows the image/text format without needing to re-detect it.
        if (!(parser instanceof AutoDetectParser) && mediaType != null) {
            metadata.set("Content-Type", mediaType.toString());
        }

        EmbeddedDocumentExtractor embeddedDocumentExtractor = new EmbeddedDocumentExtractor(
            config,
            (parser instanceof AutoDetectParser)
                ? ((AutoDetectParser) parser).getDetector()
                : config.getDetector(),
            logger,
            runContext.render(this.extractEmbedded).as(Boolean.class).orElse(false),
            runContext
        );

        // Handler
        DefaultHandler handler;
        var type = runContext.render(contentType).as(ContentType.class).orElse(ContentType.XHTML);
        var writeLimit = runContext.render(charactersLimit).as(Integer.class).orElse(-1);

        if (type == ContentType.XHTML) {
            handler = new ToXMLContentHandler();
        } else if (type == ContentType.XHTML_NO_HEADER) {
            handler = new BodyContentHandler(new ToXMLContentHandler());
        } else {
            handler = new BodyContentHandler(writeLimit);
        }

        // When a specific parser was selected for the main document, use AutoDetectParser
        // for embedded resources so they are handled generically.
        Parser embeddedParser = !(parser instanceof AutoDetectParser)
            ? new AutoDetectParser(config)
            : parser;
        ParseContext context = new ParseContext();
        context.set(org.apache.tika.extractor.EmbeddedDocumentExtractor.class, embeddedDocumentExtractor);
        context.set(Parser.class, embeddedParser);

        // TesseractOCRConfig
        TesseractOCRConfig ocrConfig = new TesseractOCRConfig();

        // Skip OCR if strategy is NO_OCR
        ocrConfig.setSkipOcr(ocrStrategy == PDFParserConfig.OCR_STRATEGY.NO_OCR);

        if (ocrOptions.getEnableImagePreprocessing() != null) {
            ocrConfig.setEnableImagePreprocessing(
                runContext.render(ocrOptions.getEnableImagePreprocessing()).as(Boolean.class).orElseThrow()
            );
        }
        if (ocrOptions.getLanguage() != null) {
            ocrConfig.setLanguage(runContext.render(ocrOptions.getLanguage()).as(String.class).orElseThrow());
        }
        context.set(TesseractOCRConfig.class, ocrConfig);

        // Register TesseractOCRParser in the context when OCR is enabled so that
        // PDF/image pipelines can actually find an OCR-capable parser.
        // Skip when TesseractOCRParser IS the main parser to avoid recursive invocation.
        if (ocrStrategy != PDFParserConfig.OCR_STRATEGY.NO_OCR
            && !(parser instanceof org.apache.tika.parser.ocr.TesseractOCRParser)) {
            context.set(org.apache.tika.parser.ocr.TesseractOCRParser.class,
                new org.apache.tika.parser.ocr.TesseractOCRParser());
        }

        PDFParserConfig pdfConfig = new PDFParserConfig();
        boolean extract = runContext.render(this.extractEmbedded).as(Boolean.class).orElse(false);
        pdfConfig.setExtractInlineImages(extract);
        pdfConfig.setExtractUniqueInlineImagesOnly(extract);
        pdfConfig.setOcrStrategy(ocrStrategy);
        context.set(PDFParserConfig.class, pdfConfig);

        // Copy to a temp file so that parsers like TesseractOCRParser can access
        // the content via a real file path (required for Tesseract's CLI invocation).
        Path tempContent = runContext.workingDir().createTempFile();
        try (InputStream raw = runContext.storage().getFile(from)) {
            Files.copy(raw, tempContent, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }

        try (TikaInputStream stream = TikaInputStream.get(tempContent)) {
            parser.parse(stream, handler, metadata, context);

            String handlerContent = handler.toString();
            // When ImageParser is used it writes nothing to the ContentHandler — only Metadata is
            // populated with EXIF/format fields (width, height, color space, …).  Build a plain-text
            // representation of those fields so that callers always receive something useful.
            String content = (useImageParserFallback && handlerContent.isBlank())
                ? buildMetadataText(metadata)
                : handlerContent;

            Parsed parsed = Parsed.builder()
                .embedded(embeddedDocumentExtractor.extracted)
                .metadata(Arrays.stream(metadata.names())
                    .map(key -> new AbstractMap.SimpleEntry<>(
                        key,
                        metadata.get(key)
                    ))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                )
                .content(content)
                .build();

            if (runContext.render(this.store).as(Boolean.class).orElse(true)) {
                Path tempFile = runContext.workingDir().createTempFile(".ion");
                try (
                    OutputStream output = new FileOutputStream(tempFile.toFile());
                ) {
                    FileSerde.write(output, parsed);
                }

                return Output.builder()
                    .uri(runContext.storage().putFile(tempFile.toFile()))
                    .build();
            } else {
                return Output.builder()
                    .result(parsed)
                    .build();
            }
        }
    }

    /**
     * Builds a plain-text representation of all Metadata entries, one {@code key: value} pair per line,
     * sorted alphabetically.  Used as a fallback when ImageParser produced no text content.
     */
    private static String buildMetadataText(Metadata metadata) {
        return Arrays.stream(metadata.names())
            .sorted()
            .map(name -> name + ": " + metadata.get(name))
            .collect(Collectors.joining("\n"));
    }

    public static class EmbeddedDocumentExtractor implements org.apache.tika.extractor.EmbeddedDocumentExtractor {
        private final TikaConfig config;
        private final Detector detector;
        private final Logger logger;
        private final Boolean parseEmbedded;
        private final RunContext runContext;
        private int fileCount = 0;
        private final Map<String, URI> extracted = new HashMap<>();

        public EmbeddedDocumentExtractor(
            TikaConfig config,
            Detector detector,
            Logger logger,
            Boolean parseEmbedded,
            RunContext runContext
        ) {
            this.config = config;
            this.detector = detector;
            this.logger = logger;
            this.parseEmbedded = parseEmbedded;
            this.runContext = runContext;
        }

        @Override
        public boolean shouldParseEmbedded(Metadata metadata) {
            return this.parseEmbedded;
        }
        @Override
        public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml) throws IOException {
            String name = this.fileName(stream, metadata);
            String extension = FilenameUtils.getExtension(name);

            logger.debug("Extracting file {}", name);

            // Upload
            Path path = runContext.workingDir().createTempFile("." + extension);
            //noinspection ResultOfMethodCallIgnored
            path.toFile().delete();

            Files.copy(stream, path);
            URI uri = runContext.storage().putFile(path.toFile());

            extracted.put(name, uri);
        }

        private String fileName(InputStream stream, Metadata metadata) throws IOException {
            String name = metadata.get(TikaCoreProperties.RESOURCE_NAME_KEY);

            if (name == null) {
                name = "file_" + fileCount++;
            } else {
                //make sure to select only the file name (not any directory paths that might be included in the name)
                // and make sure to normalize the name
                name = name.replaceAll("\u0000", " ");
                int prefix = FilenameUtils.getPrefixLength(name);
                if (prefix > -1) {
                    name = name.substring(prefix);
                }
                name = FilenameUtils.normalize(FilenameUtils.getName(name));
            }

            //now try to figure out the right extension for the embedded file
            MediaType contentType = detector.detect(stream, metadata);

            if (name.indexOf('.') == -1 && contentType != null) {
                try {
                    name += config.getMimeRepository().forName(contentType.toString()).getExtension();
                } catch (MimeTypeException e) {
                    logger.debug("Unable to detect MIME type on {}", name);
                }
            }

            return name;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        Parsed result;
        URI uri;
    }

    @Builder
    @Getter
    public static class Parsed {
        Map<String, URI> embedded;
        Map<String, Object> metadata;
        String content;
    }

    @Builder
    @ToString
    @EqualsAndHashCode
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OcrOptions {

        @Schema(
            title = "OCR strategy",
            description = "Requires Tesseract and language packs. Default is `NO_OCR`; use `OCR_AND_TEXT_EXTRACTION` to merge OCR and text extraction."
        )
        @Builder.Default
        private Property<PDFParserConfig.OCR_STRATEGY> strategy = Property.ofValue(PDFParserConfig.OCR_STRATEGY.NO_OCR);

        @Schema(
            title = "Enable image preprocessing",
            description = "Apache Tika will run preprocessing of images (rotation detection and image normalizing with ImageMagick) " +
                "before sending the image to Tesseract if the user has included dependencies (listed below) " +
                "and if the user opts to include these preprocessing steps."
        )
        private Property<Boolean> enableImagePreprocessing;

        @Schema(
            title = "Language used for OCR",
            description = "Tesseract language code (e.g. `eng`, `fra`)."
        )
        private Property<String> language;
    }

    public enum ContentType {
        TEXT,
        XHTML,
        XHTML_NO_HEADER
    }
}
