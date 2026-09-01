package io.kestra.plugin.tika;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

/**
 * tika-parsers-standard-package ships one META-INF/services/org.apache.tika.parser.Parser file per
 * module jar (OOXML, HTML, ZIP, mail…). If shadowJar's duplicatesStrategy drops duplicates before
 * mergeServiceFiles() can combine them, the shaded jar ends up registering a single parser and
 * AutoDetectParser silently falls back to EmptyParser for every other MIME type — DOCX, XLSX, ZIP,
 * EML and HTML all returned empty content and SUCCESS (see #140). Only the packaged shaded jar
 * exhibits this: on the raw test classpath every module jar contributes its own service file, so
 * this can't be reproduced through a plain RunContext-based unit test.
 */
class ShadowJarPackagingTest {

    @Test
    void shadedJarRegistersParsersFromEveryTikaModule() throws Exception {
        File shadedJar = findShadedJar();

        try (JarFile jar = new JarFile(shadedJar)) {
            JarEntry entry = jar.getJarEntry("META-INF/services/org.apache.tika.parser.Parser");
            assertThat("shaded jar must contain a merged Tika Parser service file", entry, notNullValue());

            Set<String> registeredParsers;
            try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(jar.getInputStream(entry), StandardCharsets.UTF_8))) {
                registeredParsers = reader.lines()
                    .map(String::strip)
                    .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                    .collect(Collectors.toSet());
            }

            assertThat(registeredParsers, hasItem("org.apache.tika.parser.pdf.PDFParser"));
            assertThat(registeredParsers, hasItem("org.apache.tika.parser.microsoft.ooxml.OOXMLParser"));
            assertThat(registeredParsers, hasItem("org.apache.tika.parser.microsoft.OfficeParser"));
            assertThat(registeredParsers, hasItem("org.apache.tika.parser.pkg.PackageParser"));
            assertThat(registeredParsers, hasItem("org.apache.tika.parser.mail.RFC822Parser"));
            assertThat(registeredParsers, hasItem("org.apache.tika.parser.html.JSoupParser"));
        }
    }

    private File findShadedJar() {
        File libs = new File("build/libs");
        File[] jars = libs.listFiles((dir, name) -> name.endsWith(".jar") && !name.contains("-plain"));
        assertThat("run ./gradlew shadowJar before this test (build/libs not found or empty)",
            jars, notNullValue());
        assertThat(jars.length, greaterThan(0));
        return jars[0];
    }
}
