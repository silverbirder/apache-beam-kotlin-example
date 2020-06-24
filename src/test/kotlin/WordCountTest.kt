package test.kotlin

import io.kotest.matchers.nulls.shouldNotBeNull
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
    // Example test that tests the pipeline's transforms.

    @Test
    fun countWordsTest() {
        val p: Pipeline = TestPipeline.create()

        // Create a PCollection from the WORDS static input data.
        val input: PCollection<String> =
            p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of())

        // Run ALL the pipeline's transforms (in this case, the CountWords composite transform).
        val output: PCollection<KV<String, Long>>? = input.apply(CountWords())

        // Assert that the output PCollection matches the COUNTS_ARRAY known static output data.
        PAssert.that<KV<String, Long>>(output).shouldNotBeNull()

        // Run the pipeline.
        p.run()
    }

    companion object {
        // Our static input data, which will comprise the initial PCollection.
        val WORDS: List<String> = listOf(
            "hi there", "hi", "hi sue bob",
            "hi sue", "", "bob hi"
        )

        // Our static output data, which is the expected data that the final PCollection must match.
        val COUNTS_ARRAY = mapOf(
            "hi" to 5, "there" to 1, "sue" to 2, "bob" to 2
        )
    }
}