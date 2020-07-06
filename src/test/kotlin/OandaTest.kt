package test.kotlin

import io.kotest.matchers.nulls.shouldNotBeNull
import main.kotlin.FlattenOandaJson
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
        val input: PCollection<String> = p.apply(Create.of(""))
        val output: PCollection<KV<String, FlattenOandaJson>> = input.apply(Transform())

        // Act
        p.run()

        // Assert
        PAssert.that<KV<String, FlattenOandaJson>>(output).shouldNotBeNull()
    }

    companion object {
        private val f = FlattenOandaJson()
        val INPUT: KV<String, FlattenOandaJson> = KV.of("a", f)
    }
}
