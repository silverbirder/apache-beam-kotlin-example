import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.property.checkAll
import main.kotlin.sum

class PropertyExample : StringSpec({
    "String size" {
        checkAll<Int, Int> { a, b ->
            a + b shouldBe sum (a, b)
        }
    }
})