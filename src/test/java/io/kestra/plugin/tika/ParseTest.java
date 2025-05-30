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
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.Objects;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ParseTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    static Stream<Arguments> noOcrSource() {
        return Stream.of(
            Arguments.of("docs/full.pdf", Parse.ContentType.XHTML, PDFParserConfig.OCR_STRATEGY.NO_OCR, "<p>subsample" , 46),
            Arguments.of("docs/image.pdf", Parse.ContentType.XHTML_NO_HEADER, PDFParserConfig.OCR_STRATEGY.NO_OCR, "<img src=\"embedded:image0.jpg\" alt=\"image0.jpg\" /></div>" , 1),
            Arguments.of("docs/multi.pdf", Parse.ContentType.TEXT, PDFParserConfig.OCR_STRATEGY.NO_OCR, "3 Aliquam erat volutpat" , 1),
            Arguments.of("docs/image.pdf", Parse.ContentType.XHTML_NO_HEADER, PDFParserConfig.OCR_STRATEGY.OCR_AND_TEXT_EXTRACTION, "This is an example" , 1),
            Arguments.of("docs/image.png", Parse.ContentType.TEXT, PDFParserConfig.OCR_STRATEGY.OCR_AND_TEXT_EXTRACTION, "age of foolishness" , 0)
        );
    }

    @ParameterizedTest
    @MethodSource("noOcrSource")
    void run(String doc, Parse.ContentType contentType, PDFParserConfig.OCR_STRATEGY ocrStrategy, String contains, int embeddedCount) throws Exception {
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
}
