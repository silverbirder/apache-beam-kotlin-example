package main.kotlin

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.coders.AvroCoder
import org.apache.beam.sdk.coders.DefaultCoder
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionList
import java.lang.Iterable as JavaIterable


@DefaultCoder(AvroCoder::class)
data class OandaJson(
    val instrument: String = "",
    val granularity: Granularity = Granularity.S5,
    val candles: List<Candlestick> = listOf()
)

@DefaultCoder(AvroCoder::class)
data class FlattenOandaJson(
    val instrument: String = "",
    val granularity: Granularity = Granularity.S5,
    val type: String = "",
    val time: String = "",
    val volume: Int = 0,
    val complete: Boolean = false,
    val o: Double = 0.0,
    val h: Double = 0.0,
    val l: Double = 0.0,
    val c: Double = 0.0
)

enum class Granularity {
    S5, S10, S15, S30, M1, M2, M4, M5, M10, M15, M30, H1, H2, H3, H4, H6, H8, H12, D, W, M
}

data class Candlestick(
    val time: String = "",
    val bid: CandlestickData? = CandlestickData(o = "", h = "", l = "", c = ""),
    val ask: CandlestickData? = CandlestickData(o = "", h = "", l = "", c = ""),
    val mid: CandlestickData? = CandlestickData(o = "", h = "", l = "", c = ""),
    val volume: Int = 0,
    val complete: Boolean = false
)

data class CandlestickData(
    val o: String = "",
    val h: String = "",
    val l: String = "",
    val c: String = ""
)

object Oanda {
    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(OandaOptions::class.java)
        val p = Pipeline.create(options)
        p
            .apply("ReadLines", TextIO.read().from(options.inputFile))
            .apply("Transform", Transform())
            .apply("Format", MapElements.via(Format()))
            .apply("WriteLines", TextIO.write().to(options.output))
        p.run().waitUntilFinish()
    }

    internal class Transform : PTransform<PCollection<String>, PCollection<JavaIterable<FlattenOandaJson>>>() {
        override fun expand(input: PCollection<String>): PCollection<JavaIterable<FlattenOandaJson>> {
            return input
                .apply(ParDo.of(JsonToData()))
                .apply(ParDo.of(FlattenCandles()))
        }
    }

    internal class JsonToData : DoFn<String, KV<String, OandaJson>>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val mapper = jacksonObjectMapper()
            val o = mapper.readValue<OandaJson>(c.element())
            c.output(KV.of(o.instrument, o))
        }
    }

    internal class FlattenCandles : DoFn<KV<String, OandaJson>, JavaIterable<FlattenOandaJson>>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val el = c.element()
            val oanda = el.value
            val candles = oanda.candles
            val flattenOanda = mutableListOf<FlattenOandaJson>()
            candles.map {
                var type = ""
                var o = 0.0
                var h = 0.0
                var l = 0.0
                var c = 0.0
                if (it.ask.hashCode() !== 0 && it.ask !== null) {
                    type = "ask"
                    o = it.ask.o.toDouble()
                    h = it.ask.h.toDouble()
                    l = it.ask.l.toDouble()
                    c = it.ask.c.toDouble()
                } else if (it.bid.hashCode() !== 0 && it.bid !== null) {
                    type = "bid"
                    o = it.bid.o.toDouble()
                    h = it.bid.h.toDouble()
                    l = it.bid.l.toDouble()
                    c = it.bid.c.toDouble()
                } else if (it.mid.hashCode() !== 0 && it.mid !== null){
                    type = "mid"
                    o = it.mid.o.toDouble()
                    h = it.mid.h.toDouble()
                    l = it.mid.l.toDouble()
                    c = it.mid.c.toDouble()
                }
                flattenOanda.add(
                    FlattenOandaJson(
                        instrument = oanda.instrument,
                        granularity = oanda.granularity,
                        type = type,
                        time = it.time,
                        volume = it.volume,
                        complete = it.complete,
                        o = o,
                        h = h,
                        l = l,
                        c = c
                    )
                )
            }
            c.output(flattenOanda as JavaIterable<FlattenOandaJson>)
        }
    }

    internal class Format : SimpleFunction<JavaIterable<FlattenOandaJson>, String>() {
        override fun apply(input: JavaIterable<FlattenOandaJson>): String {
            return "" + input.toString()
        }
    }

    interface OandaOptions : PipelineOptions {
        @get:Default.String("./src/main/kotlin/oanda.json")
        var inputFile: String?

        @get:Default.String("./src/main/kotlin/oanda_out.txt")
        var output: String?
    }
}
