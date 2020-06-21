import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import main.kotlin.sum

class MyTests : StringSpec({
    "sum case" {
        val result = sum(1,2)
        result.shouldBe(3)
    }
})