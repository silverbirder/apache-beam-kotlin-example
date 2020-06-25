package main.kotlin

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection

object Bitfyer {
    @JvmStatic // objectのmethodをstaticにcallしたい場合、JvmStaticをつけるらしい
    fun main(args: Array<String>) {
        // PipelineOptionsFactoryでは、fromArgs(*args) で入力値を入れ、withValidationでチェック、create()するのが基本。
        // ただ、後続で型を指定したいので、`as`を使っている。(options.inputFile とか)
        // asは、castの意味。バッククォートは、methodを実行できるようにしている。asは予約語なので使えないが、methpdとして使いたいって感じ。
        // ::class.javaは、クラスオブジェクトを取得する文法。
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(BitfyerOptions::class.java)
        val p = Pipeline.create(options)

        // textをread
        p.apply("ReadLines", TextIO.read().from(options.inputFile))
            //
            .apply(CountWords())
            .apply(MapElements.via(FormatAsTextFn()))
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

    // PTransformというIFを実装、ResponseをPCollectionとしている書き方？
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

    interface BitfyerOptions : PipelineOptions {
        @get:Default.String("./src/main/kotlin/bitfyer.txt")
        var inputFile: String?

        @get:Default.String("./src/main/kotlin/bitfyer_out.txt")
        var output: String?
    }
}