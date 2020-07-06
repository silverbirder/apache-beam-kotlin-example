package test.kotlin

import main.kotlin.FlattenOandaJson
import main.kotlin.Granularity
import main.kotlin.Oanda.Transform
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.junit.Test


class OandaTest {
    @Test
    fun oandaTransformTest() {
        // Arrange
        val p: Pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false)
        val input: PCollection<String> = p.apply(Create.of(INPUT))
        val output: PCollection<KV<String, FlattenOandaJson>> = input.apply(Transform())

        // Act
        p.run()

        // Assert
        PAssert.that<KV<String, FlattenOandaJson>>(output).containsInAnyOrder(OUTPUT)
    }

    companion object {
        const val INPUT: String =
            "{\"instrument\":\"USD_JPY\",\"granularity\":\"S5\",\"candles\":[{\"complete\":true,\"volume\":1,\"time\":\"2020-07-03T10:15:45.000000000Z\",\"bid\":{\"o\":\"107.492\",\"h\":\"107.492\",\"l\":\"107.492\",\"c\":\"107.492\"}}]}"
        val OUTPUT: KV<String, FlattenOandaJson> = KV.of(
            "USD_JPY", FlattenOandaJson(
                instrument = "USD_JPY",
                granularity = Granularity.S5,
                complete = true,
                volume = 1,
                time = "2020-07-03T10:15:45.000000000Z",
                type = "bid",
                o = 107.492,
                h = 107.492,
                l = 107.492,
                c = 107.492
            )
        )
    }
}
