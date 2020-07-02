# try-kotlin
My first learning the Kotlin

# Apache Beam でできること

Streaming処理を将来的に実現したいもの。（初期はバッチ）
データは、BQIOが一番手軽にできそう。
個人で使う分には、BQは少し恐ろしい（無料枠がもちろんあるが、その範囲を超えないか）

GCEの中に、Apache Beam を動かし、Streamingし続ける？
データはどこから読んで、どこに書き出す？
=> 例えば、OandaのAPIは、streamingがあって、これをpub/sub すれば、
読み込みはできる。
書き込みは、どこに？できれば、見えるところにあるのと、API提供したくなるが...。
スプレットシートをDBとすれば、書き込みし見えるが... api提供しにくそう。

Dataflowは使わないのか...? 個人でやる分には冗長すぎる。。。


○ 会社としてなら
Dataflow + BQ (IO)

○ 個人としてなら
Apache Beam + Free API (I) + Spreadsheet (O) 
 
