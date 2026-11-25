package io.kestra.plugin.tika;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.apache.tika.exception.WriteLimitReachedException;
import org.apache.tika.parser.pdf.OcrConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ParseTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    static Stream<Arguments> noOcrSource() {
        return Stream.of(
            Arguments.of("docs/full.pdf", Parse.ContentType.XHTML, OcrConfig.Strategy.NO_OCR, "<p>subsample" , 46),
            Arguments.of("docs/image.pdf", Parse.ContentType.XHTML_NO_HEADER, OcrConfig.Strategy.NO_OCR, "<img src=\"embedded:image-0.jpg\" alt=\"image-0.jpg\" /></div>" , 1),
            Arguments.of("docs/multi.pdf", Parse.ContentType.TEXT, OcrConfig.Strategy.NO_OCR, "3 Aliquam erat volutpat" , 1),
            Arguments.of("docs/image.pdf", Parse.ContentType.XHTML_NO_HEADER, OcrConfig.Strategy.OCR_AND_TEXT_EXTRACTION, "This is an example" , 1),
            Arguments.of("docs/simple-text.pdf", Parse.ContentType.TEXT, OcrConfig.Strategy.NO_OCR, "This is supposed to be an output for Tika Plugin", 0),
            Arguments.of("docs/image.png", Parse.ContentType.TEXT, OcrConfig.Strategy.OCR_AND_TEXT_EXTRACTION, "age of foolishness" , 0),
            Arguments.of("docs/simple-text.txt", Parse.ContentType.TEXT, OcrConfig.Strategy.NO_OCR, "plain text file for Tika Plugin", 0),
            // When OCR is not requested for an image, ImageParser extracts metadata only and writes
            // nothing to the ContentHandler.  The fallback builds content from the Metadata fields.
            Arguments.of("docs/image.png", Parse.ContentType.TEXT, OcrConfig.Strategy.NO_OCR, "tiff:ImageWidth", 0)
        );
    }

    @ParameterizedTest
    @MethodSource("noOcrSource")
    void run(String doc, Parse.ContentType contentType, OcrConfig.Strategy ocrStrategy, String contains, int embeddedCount) throws Exception {
        URL resource = ParseTest.class.getClassLoader().getResource(doc);

        URI storage = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Parse task = Parse.builder()
            .from(Property.ofValue(storage.toString()))
            .extractEmbedded(Property.ofValue(true))
            .store(Property.ofValue(false))
            .contentType(Property.ofValue(contentType))
            .ocrOptions(Parse.OcrOptions.builder()
                .strategy(Property.ofValue(ocrStrategy))
                .build())
            .build();

        Parse.Output runOutput = task.run(runContext);

        assertThat(runOutput.getResult().getContent(), containsString(contains));
        assertThat(runOutput.getResult().getEmbedded().size(), is(embeddedCount));
    }

    @Test
    void docxIsParsedWithActualContent() throws Exception {
        URL resource = ParseTest.class.getClassLoader().getResource("docs/sample.docx");

        URI storage = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Parse task = Parse.builder()
            .from(Property.ofValue(storage.toString()))
            .store(Property.ofValue(false))
            .contentType(Property.ofValue(Parse.ContentType.TEXT))
            .build();

        Parse.Output runOutput = task.run(runContext);

        assertThat(runOutput.getResult().getContent(), containsString("TIKA_MARKER_TEXT_12345"));
        assertThat((String) runOutput.getResult().getMetadata().get("X-TIKA:Parsed-By"),
            not(containsString("EmptyParser")));
    }

    @ParameterizedTest
    @MethodSource("characterLimitExceedingSource")
    void shouldThrowWhenCharacterLimitExceeded(String doc, int limit) throws Exception {
        URL resource = ParseTest.class.getClassLoader().getResource(doc);

        URI storage = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Parse task = Parse.builder()
            .from(Property.ofValue(storage.toString()))
            .extractEmbedded(Property.ofValue(false))
            .store(Property.ofValue(false))
            .contentType(Property.ofValue(Parse.ContentType.TEXT))
            .charactersLimit(Property.ofValue(limit))
            .build();

        assertThrows(WriteLimitReachedException.class, () -> task.run(runContext));
    }

    @ParameterizedTest
    @MethodSource("characterLimitValidSource")
    void shouldParseTextWhenCharacterLimitIsSufficient(String doc, int limit, String expectedText) throws Exception {
        URL resource = ParseTest.class.getClassLoader().getResource(doc);

        URI storage = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        RunContext runContext = runContextFactory.of(ImmutableMap.of());

        Parse task = Parse.builder()
            .from(Property.ofValue(storage.toString()))
            .extractEmbedded(Property.ofValue(false))
            .store(Property.ofValue(false))
            .contentType(Property.ofValue(Parse.ContentType.TEXT))
            .charactersLimit(Property.ofValue(limit))
            .build();

        Parse.Output runOutput = task.run(runContext);
        assertThat(runOutput.getResult().getContent(), containsString(expectedText));
    }

    static Stream<Arguments> characterLimitExceedingSource() {
        return Stream.of(
            Arguments.of("docs/multi.pdf",10)
        );
    }

    static Stream<Arguments> characterLimitValidSource() {
        return Stream.of(
            Arguments.of("docs/multi.pdf", -1, "Cras fringilla")
        );
    }

    private Map<String, Object> parseMetadata(String doc) throws Exception {
        return parseMetadata(doc, null);
    }

    private Map<String, Object> parseMetadata(String doc, Property<Boolean> legacyMetadataKeys) throws Exception {
        URL resource = ParseTest.class.getClassLoader().getResource(doc);

        URI storage = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        Parse.ParseBuilder<?, ?> builder = Parse.builder()
            .from(Property.ofValue(storage.toString()))
            .store(Property.ofValue(false))
            .contentType(Property.ofValue(Parse.ContentType.TEXT));
        if (legacyMetadataKeys != null) {
            builder.legacyMetadataKeys(legacyMetadataKeys);
        }

        return builder.build().run(runContextFactory.of(ImmutableMap.of())).getResult().getMetadata();
    }

    @Test
    void pdfMetadataKeepsTika3KeysByDefault() throws Exception {
        Map<String, Object> metadata = parseMetadata("docs/full.pdf");

        assertThat(metadata, hasKey("pdf:PDFVersion"));
        assertThat(metadata, hasKey("access_permission:can_print"));
        assertThat(metadata, not(hasKey("pdf:pdf-version")));
        assertThat(metadata, not(hasKey("access-permission:can-print")));
        assertThat(metadata, hasKey("Content-Type"));
    }

    @Test
    void imageMetadataKeepsTika3KeysByDefault() throws Exception {
        Map<String, Object> metadata = parseMetadata("docs/image.png");

        assertThat(metadata, hasKey("width"));
        assertThat(metadata, hasKey("height"));
        assertThat(metadata, hasKey("imagereader:NumImages"));
        assertThat(metadata, not(hasKey("img:width")));
        assertThat(metadata, not(hasKey("tk:num-images")));
        assertThat(metadata, hasKey("Content-Type"));
        assertThat(metadata, hasKey("tiff:ImageWidth"));
        assertThat(metadata, hasKey("tiff:ImageLength"));
    }

    @Test
    void pdfMetadataUsesTika4KeysWhenLegacyDisabled() throws Exception {
        Map<String, Object> metadata = parseMetadata("docs/full.pdf", Property.ofValue(false));

        assertThat(metadata, hasKey("pdf:pdf-version"));
        assertThat(metadata, hasKey("access-permission:can-print"));
        assertThat(metadata, not(hasKey("pdf:PDFVersion")));
        assertThat(metadata, not(hasKey("access_permission:can_print")));
        assertThat(metadata, hasKey("Content-Type"));
    }

    @Test
    void imageMetadataUsesTika4KeysWhenLegacyDisabled() throws Exception {
        Map<String, Object> metadata = parseMetadata("docs/image.png", Property.ofValue(false));

        assertThat(metadata, hasKey("img:width"));
        assertThat(metadata, hasKey("img:height"));
        assertThat(metadata, hasKey("tk:num-images"));
        assertThat(metadata, not(hasKey("width")));
        assertThat(metadata, not(hasKey("height")));
        assertThat(metadata, not(hasKey("imagereader:NumImages")));
        assertThat(metadata, hasKey("Content-Type"));
        assertThat(metadata, hasKey("tiff:ImageWidth"));
        assertThat(metadata, hasKey("tiff:ImageLength"));
    }

    @Test
    void legacyMetadataKeysIsRenderedFromExpression() throws Exception {
        Map<String, Object> metadata = parseMetadata("docs/full.pdf", Property.ofExpression("{{ false }}"));

        assertThat(metadata, hasKey("pdf:pdf-version"));
        assertThat(metadata, not(hasKey("pdf:PDFVersion")));
    }
}
