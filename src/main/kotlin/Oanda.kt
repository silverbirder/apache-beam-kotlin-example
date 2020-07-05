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

    internal class Transform : PTransform<PCollection<String>, PCollection<KV<String, FlattenOandaJson>>>() {
        override fun expand(input: PCollection<String>): PCollection<KV<String, FlattenOandaJson>> {
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

    internal class FlattenCandles : DoFn<KV<String, OandaJson>, KV<String, FlattenOandaJson>>() {
        @ProcessElement
        fun processElement(c: ProcessContext) {
            val el = c.element()
            val oanda = el.value
            val candles = oanda.candles
            val flattenOanda = mutableListOf<FlattenOandaJson>()
            candles.map {
                var type = ""
                var op = 0.0
                var hp = 0.0
                var lp = 0.0
                var cp = 0.0
                if (it.ask.hashCode() !== 0 && it.ask !== null) {
                    type = "ask"
                    op = it.ask.o.toDouble()
                    hp = it.ask.h.toDouble()
                    lp = it.ask.l.toDouble()
                    cp = it.ask.c.toDouble()
                } else if (it.bid.hashCode() !== 0 && it.bid !== null) {
                    type = "bid"
                    op = it.bid.o.toDouble()
                    hp = it.bid.h.toDouble()
                    lp = it.bid.l.toDouble()
                    cp = it.bid.c.toDouble()
                } else if (it.mid.hashCode() !== 0 && it.mid !== null){
                    type = "mid"
                    op = it.mid.o.toDouble()
                    hp = it.mid.h.toDouble()
                    lp = it.mid.l.toDouble()
                    cp = it.mid.c.toDouble()
                }
                val flattenObject = FlattenOandaJson(
                    instrument = oanda.instrument,
                    granularity = oanda.granularity,
                    type = type,
                    time = it.time,
                    volume = it.volume,
                    complete = it.complete,
                    o = op,
                    h = hp,
                    l = lp,
                    c = cp
                )
                c.output(KV.of(el.key, flattenObject))
            }
        }
    }

    internal class Format : SimpleFunction<KV<String, FlattenOandaJson>, String>() {
        override fun apply(input: KV<String, FlattenOandaJson>): String {
            return "${input.key},${input.value.time}"
        }
    }

    interface OandaOptions : PipelineOptions {
        @get:Default.String("./src/main/kotlin/oanda.json")
        var inputFile: String?

        @get:Default.String("./src/main/kotlin/oanda_out.txt")
        var output: String?
    }
}
