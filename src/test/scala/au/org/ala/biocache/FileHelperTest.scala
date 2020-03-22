package au.org.ala.biocache

import java.io.File
import au.org.ala.biocache.util.FileHelper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.Assertions._


@RunWith(classOf[JUnitRunner])
class FileHelperTest extends ConfigFunSuite {
    test("SHA1 Hash") {
        expectResult("3c1bb0cd5d67dddc02fae50bf56d3a3a4cbc7204") {
            val file = File.createTempFile("test", ".val")
            val helper = FileHelper.file2helper(file);
            helper.write("This is a test\n")
            val hash = helper.sha1Hash()
            file.delete()
            hash
        }
    }

}
