package com.jamesward.zio_mavencentral

import com.jamesward.zio_mavencentral.MavenCentral.*
import zio.schema.Schema
import zio.schema.annotation.description
import zio.test.*

/**
 * The coordinate types are exposed with their **record** schema as the ambient
 * given, and every property carries a `@description`.
 */
object SchemaSpec extends ZIOSpecDefault:

  private def isDescribed(field: Schema.Field[?, ?]): Boolean =
    field.annotations.exists:
      case _: description => true
      case _              => false

  def spec = suite("MavenCentral coordinate schemas")(

    test("Schema[GroupArtifact] is a record with a described field per property"):
      summon[Schema[GroupArtifact]] match
        case r: Schema.Record[?] =>
          val names = r.fields.map(_.name).toList
          assertTrue(
            r.fields.size == 2,
            names.contains("groupId"),
            names.contains("artifactId"),
            r.fields.forall(isDescribed),
          )
        case other =>
          assertTrue(false) ?? s"expected a record schema, got $other"
    ,

    test("Schema[GroupArtifactVersion] is a record with a described field per property"):
      summon[Schema[GroupArtifactVersion]] match
        case r: Schema.Record[?] =>
          val names = r.fields.map(_.name).toList
          assertTrue(
            r.fields.size == 3,
            names.contains("groupId"),
            names.contains("artifactId"),
            names.contains("version"),
            r.fields.forall(isDescribed),
          )
        case other =>
          assertTrue(false) ?? s"expected a record schema, got $other"
  )
