package test.kotlin

import main.kotlin.WordCount.CountWords
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Test


class WordCountTest {
    @Test
    fun countWordsTest() {
        // Arrange
        val p: Pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false)
        val input: PCollection<String> = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of())
        val output: PCollection<KV<String, Long>>? = input.apply(CountWords())

        // Act
        p.run()

        // Assert
        PAssert.that<KV<String, Long>>(output).containsInAnyOrder(COUNTS_ARRAY)
    }

    companion object {
        val WORDS: List<String> = listOf(
            "hi there", "hi", "hi sue bob",
            "hi sue", "", "bob hi"
        )
        val COUNTS_ARRAY = listOf(
            KV.of("hi", 5L),
            KV.of("there", 2L),
            KV.of("sue", 2L),
            KV.of("bob", 2L)
        )
    }
}
