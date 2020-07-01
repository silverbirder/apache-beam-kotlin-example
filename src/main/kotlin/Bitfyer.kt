package main.kotlin

// @see https://gist.github.com/marcoslin/e1e19afdbacac9757f6974592cfd8d7f#file-working-kt-L10

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.Partition.PartitionFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList
import java.lang.Iterable as JavaIterable


object Bitfyer {
    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(BitfyerOptions::class.java)
        val p = Pipeline.create(options)
        p.apply("ReadLines", TextIO.read().from(options.inputFile))
            .apply("Filters", FilterBit())
            .apply(MapElements.via(FormatAsTextFn()))
            .apply("WriteCounts", TextIO.write().to(options.output))
        p.run().waitUntilFinish()
    }

    class FilterBit : PTransform<PCollection<String>, PCollection<KV<String, Int>>>() {
        override fun expand(input: PCollection<String>): PCollection<KV<String, Int>>? {
            var a = input.apply(ParDo.of(Extract()))
            var pp = a.apply(Partition.of(10, PartitionFunc()))
//            var pc = PCollectionList.of(a).and(a)
            var pa = pp.apply(Flatten.pCollections())
//            var b = a.apply(GroupByKey.create<String, Int>()) as PCollection<KV<String, JavaIterable<Int>>>
            var d = pa.apply(Sum.integersPerKey())
            var u = d.apply<PCollection<KV<String, Int>>>(Combine.perKey(SumInt()))
            return u
        }
    }

    internal class PartitionFunc: Partition.PartitionFn<KV<String, Int>> {
        override fun partitionFor(elem: KV<String, Int>, numPartitions: Int): Int {
            return numPartitions / 100
        }
    }

    internal class Extract : DoFn<String, KV<String, Int>>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val el = c.element().split(',')
            val com = el[1]
            val up = el[3].toDouble().toInt()
            c.output(KV.of(com, up))
        }
    }

    internal class Extract2 : DoFn<KV<String, JavaIterable<Int>>, KV<String, Int>>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val el = c.element()
            var sum = 0
            el.value.forEach {
                sum += it
            }
            c.output(KV.of(el.key, sum))
        }
    }

    internal class SumInt : SerializableFunction<Iterable<Int>, Int> {
        override fun apply(input: Iterable<Int>): Int {
            return input.sum()
        }
    }

    class FormatAsTextFn : SimpleFunction<KV<String, Int>, String>() {
        override fun apply(input: KV<String, Int>): String {
            return input.key + input.value.toString()
        }
    }

    interface BitfyerOptions : PipelineOptions {
        @get:Default.String("./src/main/kotlin/bitfyer.txt")
        var inputFile: String?

        @get:Default.String("./src/main/kotlin/bitfyer_out.txt")
        var output: String?
    }
}
