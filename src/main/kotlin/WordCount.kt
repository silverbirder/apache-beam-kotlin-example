package main.kotlin

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection

object WordCount {
    @JvmStatic
    fun main(args: Array<String>) {
        val options =
            PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(WordCountOptions::class.java)
        val p = Pipeline.create(options)
        p.apply(
            "ReadLines",
            TextIO.read().from(options.inputFile)
        )
            .apply(CountWords())
            .apply(
                MapElements.via(
                    FormatAsTextFn()
                )
            )
            .apply("WriteCounts", TextIO.write().to(options.output))
        p.run().waitUntilFinish()
    }

    internal class ExtractWordsFn : DoFn<String?, String?>() {
        private val emptyLines = Metrics.counter(
            ExtractWordsFn::class.java, "emptyLines"
        )

        @ProcessElement
        fun processElement(c: ProcessContext) {
            if (c.element()!!.trim { it <= ' ' }.isEmpty()) {
                emptyLines.inc()
            }
            val words = c.element()!!.split("[^\\p{L}]+".toRegex()).toTypedArray()
            for (word in words) {
                if (!word.isEmpty()) {
                    c.output(word)
                }
            }
        }
    }

    class FormatAsTextFn : SimpleFunction<KV<String, Long>, String>() {
        override fun apply(input: KV<String, Long>): String {
            return input.key.toString() + ": " + input.value
        }
    }

    class CountWords :
        PTransform<PCollection<String?>, PCollection<KV<String, Long>>>() {
        override fun expand(lines: PCollection<String?>): PCollection<KV<String, Long>> {
            val words =
                lines.apply(
                    ParDo.of<String, String>(ExtractWordsFn())
                )
            return words.apply(Count.perElement())
        }
    }

    interface WordCountOptions : PipelineOptions {
        @get:Default.String("./src/main/kotlin/kinglear.txt")
        @get:Description("Path of the file to read from")
        var inputFile: String?

        @get:Default.String("./src/main/kotlin/kinglear_out.txt")
        @get:Description("Path of the file to write to")
        var output: String?
    }
}