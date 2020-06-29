package main.kotlin

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.*
import java.lang.Iterable as JavaIterable

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
            .apply(FilterBit())
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

    internal class Extract: DoFn<String, KV<String, Int>>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val el = c.element().split(',')
            // BTC/JPY,bitflyer,1519845731987,1127174.0,1126166.0
            val com = el[1]
            val up = el[3].toDouble().toInt()
            c.output(KV.of(com, up))
        }
    }

    internal class Extract2 : DoFn<KV<String, JavaIterable<Int>>, KV<String, Int>>() {
        @ProcessElement
        fun processElement(@Element a: KV<String, JavaIterable<Int>>, receiver: OutputReceiver<KV<String, Int>>) {
            var sum = 0
            a.value.forEach{
                sum += it
            }
            receiver.output(KV.of(a.key, sum))
        }
    }

    class FormatAsTextFn : SimpleFunction<KV<String, Int>, String>() {
        override fun apply(input: KV<String, Int>): String {
            return input.key + input.value.toString()
        }
    }

    class FilterBit : PTransform<PCollection<String>, PCollection<KV<String, Int>>>() {
        override fun expand(input: PCollection<String>): PCollection<KV<String, Int>>? {
//            val type =
//                Schema.builder().addMapField("KV",
//                    Schema.FieldType.STRING,
//                    Schema.FieldType.array(Schema.FieldType.INT64)).build()
            var a = input.apply(ParDo.of(Extract()))
            var b = a.apply(GroupByKey.create<String, Int>()) as PCollection<KV<String, JavaIterable<Int>>>
//                .setSchema(
//                    type,
//                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.iterables(TypeDescriptors.integers())),
//                    SerializableFunctions.constant<KV<String, Iterable<Int>>, Row>(Row.withSchema(type).build()),
//                    SerializableFunctions.constant<Row, KV<String, Iterable<Int>>>(KV.of("", listOf(1)))
//                )
            var c = b.apply(ParDo.of(Extract2()))
            return c
            // main.kotlin.Bitfyer$Extract2,
            // @ProcessElement processElement(ProcessContext), @ProcessElement processElement(ProcessContext),
            // parameter of type DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>.ProcessContext at index 0:
            // ProcessContext argument must have type DoFn<KV<String, Iterable<? extends Integer>>, KV<String, Integer>>.ProcessContext

        }
    }

    // PTransformというIFを実装
    class CountWords :
    // https://beam.apache.org/releases/javadoc/2.2.0/org/apache/beam/sdk/transforms/PTransform.html
    // input, output の形式。
        PTransform<PCollection<String?>, PCollection<KV<String, Long>>>() {
        // PTransformは、expandを実装する必要がある
        // https://beam.apache.org/releases/javadoc/2.2.0/org/apache/beam/sdk/transforms/PTransform.html#expand-InputT-
        override fun expand(lines: PCollection<String?>): PCollection<KV<String, Long>> {

            // ParDo. Transformの中で基本的な処理？. (PTransformの中に、ParDo, GroupByKey, などある)
            // ParDoは、DoFn を継承したものじゃないとだめ。
            val words =
                lines.apply(
                    ParDo.of<String, String>(ExtractWordsFn())
                )
            // GroupByKeyは、キーとなるものでGroupByする
            // Combineは、...
            // https://beam.apache.org/documentation/transforms/java/aggregation/combine/ を読もう
            // https://github.com/apache/beam/tree/master/examples/java/src/main/java/org/apache/beam/examples も読もう・
            // CoGroupByKey ...
            // Flatten ...
            // Partition ...
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
