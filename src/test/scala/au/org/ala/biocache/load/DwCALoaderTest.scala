/*
 * Copyright (C) 2012 Atlas of Living Australia
 * All Rights Reserved.
 *
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 *
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 */
package au.org.ala.biocache.load

import java.io.File
import java.net.URL
import java.nio.file.Paths

import au.org.ala.biocache.ConfigFunSuite
import org.gbif.dwc.terms.{DcTerm, DwcTerm}
import org.gbif.dwc.DwcFiles
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DwCALoaderTest extends ConfigFunSuite {
  val WORK_DIR = new File("src/test/resources/au/org/ala/load/test-dwca")
  val IMAGE_BASE = WORK_DIR.toURI.toURL

  test("locate multimedia 1") {
    val loader = new DwCALoader
    val archive = DwcFiles.fromLocation(WORK_DIR.toPath())
    val row = archive.getExtension(DwCALoader.IMAGE_TYPE).iterator().next()
    expectResult(Some(new URL(IMAGE_BASE, "672737.jpg"))) {
      loader.locateMultimedia(row, IMAGE_BASE)
    }
  }

  test("locate multimedia 2") {
    val loader = new DwCALoader
    val archive = DwcFiles.fromLocation(WORK_DIR.toPath())
    val iterator = archive.getExtension(DwCALoader.IMAGE_TYPE).iterator()
    iterator.next()
    val row = iterator.next()
    expectResult(Some(new URL("http://localhost/no.where/nothing.png"))) {
      loader.locateMultimedia(row, IMAGE_BASE)
    }
  }

  test("locate multimedia 3") {
    val loader = new DwCALoader
    val archive = DwcFiles.fromLocation(WORK_DIR.toPath())
    val iterator = archive.getExtension(DwCALoader.IMAGE_TYPE).iterator()
    iterator.next()
    iterator.next()
    val row = iterator.next()
    expectResult(Some(new URL("http://localhost/nowhere/something.gif?format=gif"))) {
      loader.locateMultimedia(row, IMAGE_BASE)
    }
  }

  test("load multimedia 1") {
    val loader = new DwCALoader
    val archive = DwcFiles.fromLocation(WORK_DIR.toPath())
    val ai = archive.iterator()
    val record = ai.next()
    val multimediaList = loader.loadMultimedia(record, DwCALoader.IMAGE_TYPE, IMAGE_BASE)
    expectResult(3) { multimediaList.size }
    val multimedia = multimediaList(0)
    expectResult(new URL(IMAGE_BASE, "672737.jpg")) { multimedia.location }
    expectResult("image/jpeg") { multimedia.mediaType }
    expectResult("672737.jpg") { multimedia.metadata(DcTerm.identifier.simpleName()) }
    expectResult("jpeg") { multimedia.metadata(DcTerm.format.simpleName()) }
    expectResult("Tosia  australis") { multimedia.metadata(DcTerm.title.simpleName()) }
    expectResult("Tosia australis, Biscuit Star specimen") { multimedia.metadata(DcTerm.description.simpleName()) }
    expectResult("Healley, Benjamin") { multimedia.metadata(DcTerm.creator.simpleName()) }
    expectResult("CC BY (Attribution)") { multimedia.metadata(DcTerm.license.simpleName()) }
    expectResult("Benjamin Healley / Museum Victoria") { multimedia.metadata(DcTerm.rightsHolder.simpleName()) }
  }

  test("load multimedia 2") {
    val loader = new DwCALoader
    val archive = DwcFiles.fromLocation(WORK_DIR.toPath())
    val ai = archive.iterator()
    val record = ai.next()
    val multimediaList = loader.loadMultimedia(record, DwCALoader.IMAGE_TYPE, IMAGE_BASE)
    expectResult(3) { multimediaList.size }
    val multimedia = multimediaList(1)
    expectResult(new URL("http://localhost/no.where/nothing.png")) { multimedia.location }
    expectResult("image/png") { multimedia.mediaType }
    expectResult("http://localhost/no.where/nothing.png") { multimedia.metadata(DcTerm.identifier.simpleName()) }
    expectResult("png") { multimedia.metadata(DcTerm.format.simpleName()) }
    expectResult("Nowhere") { multimedia.metadata(DcTerm.title.simpleName()) }
    expectResult(false) { multimedia.metadata.isDefinedAt(DcTerm.description.simpleName()) }
    expectResult("A. N. Other") { multimedia.metadata(DcTerm.creator.simpleName()) }
    expectResult("CC BY (Attribution)") { multimedia.metadata(DcTerm.license.simpleName()) }
    expectResult("A. N. Other") { multimedia.metadata(DcTerm.rightsHolder.simpleName()) }
  }

  test("load multimedia 3") {
    val loader = new DwCALoader
    val archive = DwcFiles.fromLocation(WORK_DIR.toPath())
    val ai = archive.iterator()
    val record = ai.next()
    val multimediaList = loader.loadMultimedia(record, DwCALoader.IMAGE_TYPE, IMAGE_BASE)
    expectResult(3) { multimediaList.size }
    val multimedia = multimediaList(2)
    expectResult(new URL("http://localhost/nowhere/something.gif?format=gif")) { multimedia.location }
    expectResult("image/gif") { multimedia.mediaType }
    expectResult("http://localhost/nowhere/something.gif?format=gif") { multimedia.metadata(DcTerm.identifier.simpleName()) }
    expectResult(false) { multimedia.metadata.isDefinedAt(DcTerm.format.simpleName()) }
    expectResult("Somewhere") { multimedia.metadata(DcTerm.title.simpleName()) }
    expectResult(false) { multimedia.metadata.isDefinedAt(DcTerm.description.simpleName()) }
    expectResult(false) { multimedia.metadata.isDefinedAt(DcTerm.creator.simpleName()) }
    expectResult(false) { multimedia.metadata.isDefinedAt(DcTerm.license.simpleName()) }
    expectResult("A. N. Other") { multimedia.metadata(DcTerm.rightsHolder.simpleName()) }
  }

  test("load multimedia 4") {
    val loader = new DwCALoader
    val archive = DwcFiles.fromLocation(WORK_DIR.toPath())
    val ai = archive.iterator()
    ai.next()
    val record = ai.next()
    val multimediaList = loader.loadMultimedia(record, DwCALoader.IMAGE_TYPE, IMAGE_BASE)
    expectResult(0) { multimediaList.size }
  }

}
