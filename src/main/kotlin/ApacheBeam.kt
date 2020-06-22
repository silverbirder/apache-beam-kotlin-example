package main.kotlin

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/** Duplicated from beam-examples-java to avoid dependency.  */
object WordCount {
    @JvmStatic
    fun main(args: Array<String>) {
        val options =
            PipelineOptionsFactory.fromArgs(*args).withValidation().`as`(WordCountOptions::class.java)
        val p = Pipeline.create(options)

        // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
        // static FormatAsTextFn() to the ParDo transform.
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

    /**
     * Concept #2: You can make your pipeline code less verbose by defining your DoFns statically out-
     * of-line. This DoFn tokenizes lines of text into individual words; we pass it to a ParDo in the
     * pipeline.
     */
    internal class ExtractWordsFn : DoFn<String?, String?>() {
        private val emptyLines = Metrics.counter(
            ExtractWordsFn::class.java, "emptyLines"
        )

        @ProcessElement
        fun processElement(c: ProcessContext) {
            if (c.element()!!.trim { it <= ' ' }.isEmpty()) {
                emptyLines.inc()
            }

            // Split the line into words.
            val words = c.element()!!.split("[^\\p{L}]+".toRegex()).toTypedArray()

            // Output each word encountered into the output PCollection.
            for (word in words) {
                if (!word.isEmpty()) {
                    c.output(word)
                }
            }
        }
    }

    /** A SimpleFunction that converts a Word and Count into a printable string.  */
    class FormatAsTextFn : SimpleFunction<KV<String, Long>, String>() {
        override fun apply(input: KV<String, Long>): String {
            return input.key.toString() + ": " + input.value
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     *
     *
     * Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
     * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */
    class CountWords :
        PTransform<PCollection<String?>, PCollection<KV<String, Long>>>() {
        override fun expand(lines: PCollection<String?>): PCollection<KV<String, Long>> {

            // Convert lines of text into individual words.
            val words =
                lines.apply(
                    ParDo.of<String, String>(ExtractWordsFn())
                )

            // Count the number of times each word occurs.
            return words.apply(Count.perElement())
        }
    }

    /**
     * Options supported by [WordCount].
     *
     *
     * Concept #4: Defining your own configuration options. Here, you can add your own arguments to
     * be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     *
     * Inherits standard configuration options.
     */
    interface WordCountOptions : PipelineOptions {
        @get:String("gs://beam-samples/shakespeare/kinglear.txt")
        @get:Description("Path of the file to read from")
        var inputFile: String?

        @get:Description("Path of the file to write to")
        var output: String?
    }
}