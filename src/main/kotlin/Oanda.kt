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
    val o: Long = 0,
    val h: Long = 0,
    val l: Long = 0,
    val c: Long = 0
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

    internal class Transform : PTransform<PCollection<String>, PCollection<KV<String, JavaIterable<FlattenOandaJson>>>>() {
        override fun expand(input: PCollection<String>): PCollection<KV<String, JavaIterable<FlattenOandaJson>>> {
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

    internal class FlattenCandles : DoFn<KV<String, OandaJson>, KV<String, JavaIterable<FlattenOandaJson>>>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val el = c.element()
            val oanda = el.value
            val candles = oanda.candles
            val flattenOanda = mutableListOf<FlattenOandaJson>()
            candles.map {
                var type = ""
                var o: Long = 0
                var h: Long = 0
                var l: Long = 0
                var c: Long = 0
                if (it.ask !== null) {
                    type = "ask"
                    o = it.ask.o.toLong()
                    h = it.ask.h.toLong()
                    l = it.ask.l.toLong()
                    c = it.ask.c.toLong()
                } else if (it.bid !== null) {
                    type = "bid"
                    o = it.bid.o.toLong()
                    h = it.bid.h.toLong()
                    l = it.bid.l.toLong()
                    c = it.bid.c.toLong()
                } else if (it.mid !== null){
                    type = "mid"
                    o = it.mid.o.toLong()
                    h = it.mid.h.toLong()
                    l = it.mid.l.toLong()
                    c = it.mid.c.toLong()
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
            c.output(KV.of(el.key, flattenOanda as JavaIterable<FlattenOandaJson>))
        }
    }

    internal class Format : SimpleFunction<KV<String, JavaIterable<FlattenOandaJson>>, String>() {
        override fun apply(input: KV<String, JavaIterable<FlattenOandaJson>>): String {
            return "" + input.key
        }
    }

    interface OandaOptions : PipelineOptions {
        @get:Default.String("./src/main/kotlin/oanda.json")
        var inputFile: String?

        @get:Default.String("./src/main/kotlin/oanda_out.txt")
        var output: String?
    }
}
