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
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.helpers.DefaultHandler;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    title = "Parse a document with Tika and extract its content and metadata."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Extract text from a file.",
            code = """
                id: tika_parse
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: parse
                    type: io.kestra.plugin.tika.Parse
                    from: '{{ inputs.file }}'
                    extractEmbedded: true
                    store: false
                """
        ),
        @Example(
            full = true,
            title = "Extract text from an image using OCR.",
            code = """
                id: tika_parse
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: parse
                    type: io.kestra.plugin.tika.Parse
                    from: '{{ inputs.file }}'
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
                    uri: https://kestra.io/blogs/2023-05-31-beginner-guide-kestra.jpg

                  - id: tika
                    type: io.kestra.plugin.tika.Parse
                    from: "{{ outputs.get_image.uri }}"
                    store: false
                    contentType: TEXT
                    ocrOptions:
                      strategy: OCR_AND_TEXT_EXTRACTION
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
        title = "The file to parse",
        description = "Must be an internal storage URI."
    )
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Set whether to extract the embedded document."
    )
    @Builder.Default
    private Property<Boolean> extractEmbedded = Property.ofValue(false);

    @Schema(
        title = "The content type of the extracted text"
    )
    @Builder.Default
    private Property<ContentType> contentType = Property.ofValue(ContentType.XHTML);

    @Schema(
        title = "Custom options for OCR processing",
        description = "You need to install [Tesseract](https://cwiki.apache.org/confluence/display/TIKA/TikaOCR) " +
            "to enable OCR processing."
    )
    @PluginProperty(dynamic = false)
    @Builder.Default
    private OcrOptions ocrOptions = OcrOptions.builder()
        .strategy(Property.ofValue(PDFParserConfig.OCR_STRATEGY.NO_OCR))
        .build();

    @Schema(
        title = "Set whether to store the data from the query result into an Ion serialized data file in Kestra internal storage."
    )
    @Builder.Default
    protected final Property<Boolean> store = Property.ofValue(true);

    @PluginProperty
    @Schema(title = "Set maximum number of characters to include in the string, or -1 (default) to disable the write limit.")
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

        TikaConfig config = new TikaConfig(this.getClass().getClassLoader());

        AutoDetectParser parser = new AutoDetectParser(config);
        Metadata metadata = new Metadata();
        EmbeddedDocumentExtractor embeddedDocumentExtractor = new EmbeddedDocumentExtractor(
            config,
            parser.getDetector(),
            logger,
            runContext.render(this.extractEmbedded).as(Boolean.class).orElseThrow(),
            runContext
        );

        // Handler
        DefaultHandler handler;
        var type = runContext.render(contentType).as(ContentType.class).orElseThrow();
        var writeLimit = runContext.render(charactersLimit).as(Integer.class).orElse(-1);

        if (type == ContentType.XHTML) {
            handler = new ToXMLContentHandler();
        } else if (type == ContentType.XHTML_NO_HEADER) {
            handler = new BodyContentHandler(new ToXMLContentHandler());
        } else {
            handler = new BodyContentHandler(writeLimit);
        }

        // ParseContext
        ParseContext context = new ParseContext();
        context.set(org.apache.tika.extractor.EmbeddedDocumentExtractor.class, embeddedDocumentExtractor);
        context.set(Parser.class, parser);

        // TesseractOCRConfig
        TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
        ocrConfig.setSkipOcr(runContext.render(ocrOptions.getStrategy()).as(PDFParserConfig.OCR_STRATEGY.class).orElseThrow() == PDFParserConfig.OCR_STRATEGY.NO_OCR);

        if (ocrOptions.getEnableImagePreprocessing() != null) {
            ocrConfig.setEnableImagePreprocessing(runContext.render(ocrOptions.getEnableImagePreprocessing()).as(Boolean.class).orElseThrow());
        }

        if (ocrOptions.getLanguage() != null) {
            ocrConfig.setLanguage(runContext.render(ocrOptions.getLanguage()).as(String.class).orElseThrow());
        }

        context.set(TesseractOCRConfig.class, ocrConfig);

        // PDFParserConfig
        PDFParserConfig pdfConfig = new PDFParserConfig();
        pdfConfig.setExtractInlineImages(runContext.render(this.extractEmbedded).as(Boolean.class).orElse(false));
        pdfConfig.setExtractUniqueInlineImagesOnly(runContext.render(this.extractEmbedded).as(Boolean.class).orElse(false));
        pdfConfig.setOcrStrategy(runContext.render(ocrOptions.getStrategy()).as(PDFParserConfig.OCR_STRATEGY.class).orElseThrow());
        context.set(PDFParserConfig.class, pdfConfig);

        // Process
        URI from = new URI(runContext.render(this.from).as(String.class).orElseThrow());

        try (InputStream stream = runContext.storage().getFile(from)) {
            parser.parse(stream, handler, metadata, context);

            String content = handler.toString();

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
            title = "OCR strategy to use for OCR processing.",
            description = "You need to install [Tesseract](https://cwiki.apache.org/confluence/display/TIKA/TikaOCR) " +
                "to enable OCR processing, along with Tesseract language pack."
        )
        @Builder.Default
        private Property<PDFParserConfig.OCR_STRATEGY> strategy = Property.ofValue(PDFParserConfig.OCR_STRATEGY.NO_OCR);

        @Schema(
            title = "Whether to enable image preprocessing.",
            description = "Apache Tika will run preprocessing of images (rotation detection and image normalizing with ImageMagick) " +
                "before sending the image to Tesseract if the user has included dependencies (listed below) " +
                "and if the user opts to include these preprocessing steps."
        )
        private Property<Boolean> enableImagePreprocessing;

        @Schema(
            title = "Language used for OCR."
        )
        private Property<String> language;
    }

    public enum ContentType {
        TEXT,
        XHTML,
        XHTML_NO_HEADER
    }
}
