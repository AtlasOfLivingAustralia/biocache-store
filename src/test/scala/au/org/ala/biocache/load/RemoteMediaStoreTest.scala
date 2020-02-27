package au.org.ala.biocache.load

import au.org.ala.biocache.ConfigFunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RemoteMediaStoreTest extends ConfigFunSuite {
  test("http and https test") {
    // test config = http://images-dev.ala.org.au
    expectResult(true){ RemoteMediaStore.isRemoteMediaStoreUrl("http://images-dev.ala.org.au/image/proxy")}
    expectResult(true){ RemoteMediaStore.isRemoteMediaStoreUrl("https://images-dev.ala.org.au/image/proxy")}
  }
}
