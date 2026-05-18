package com.jamesward.zio_mavencentral

import com.jamesward.zio_mavencentral.MavenCentral.Signer
import org.bouncycastle.bcpg.{HashAlgorithmTags, PublicKeyAlgorithmTags, PublicKeyPacket, SymmetricKeyAlgorithmTags}
import org.bouncycastle.openpgp.operator.jcajce.{JcaKeyFingerprintCalculator, JcaPGPContentSignerBuilder, JcaPGPContentVerifierBuilderProvider, JcaPGPDigestCalculatorProviderBuilder, JcaPGPKeyPair, JcePBESecretKeyEncryptorBuilder}
import org.bouncycastle.openpgp.{PGPKeyRingGenerator, PGPObjectFactory, PGPPublicKey, PGPSecretKeyRing, PGPSignature, PGPSignatureList, PGPSignatureSubpacketGenerator, PGPUtil}
import zio.*
import zio.test.*

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.security.KeyPairGenerator
import java.util.{Base64, Date}

object SignerSpec extends ZIOSpecDefault:

  given CanEqual[Boolean, Boolean] = CanEqual.derived

  /** Build an RSA-based PGP secret key ring (binary, not ASCII-armored)
   *  encoded as a base64 string — the format that `Signer.make` parses
   *  via `PGPSecretKeyRing(keyBytes, JcaKeyFingerprintCalculator())`. */
  private def generateBase64KeyRing(identity: String, passphrase: Option[String]): String =
    val sha1Calc = JcaPGPDigestCalculatorProviderBuilder().build().nn.get(HashAlgorithmTags.SHA1).nn

    val kpg = KeyPairGenerator.getInstance("RSA").nn
    kpg.initialize(2048)
    val rsaKp = kpg.generateKeyPair().nn

    val pgpKeyPair = JcaPGPKeyPair(PublicKeyPacket.VERSION_4, PublicKeyAlgorithmTags.RSA_GENERAL, rsaKp, new Date())

    val signerBuilder =
      JcaPGPContentSignerBuilder(pgpKeyPair.getPublicKey.nn.getAlgorithm, HashAlgorithmTags.SHA256)

    val keyEncryptor = passphrase match
      case Some(pw) =>
        JcePBESecretKeyEncryptorBuilder(SymmetricKeyAlgorithmTags.AES_256, sha1Calc)
          .build(pw.toCharArray.nn).nn
      case None =>
        null

    val keyRingGen = PGPKeyRingGenerator(
      PGPSignature.POSITIVE_CERTIFICATION,
      pgpKeyPair,
      identity,
      sha1Calc,
      PGPSignatureSubpacketGenerator().generate(),
      null,
      signerBuilder,
      keyEncryptor,
    )

    val secretKeys: PGPSecretKeyRing = keyRingGen.generateSecretKeyRing().nn
    val out = ByteArrayOutputStream()
    secretKeys.encode(out)
    Base64.getEncoder.nn.encodeToString(out.toByteArray.nn).nn

  /** Re-derive the public key from the generated key ring so the verifier
   *  doesn't have to know about Signer's internals. */
  private def publicKeyOf(gpgKey: String): PGPPublicKey =
    val keyBytes = Base64.getDecoder.nn.decode(gpgKey).nn
    PGPSecretKeyRing(keyBytes, JcaKeyFingerprintCalculator()).getPublicKey.nn

  /** Verify an ASCII-armored detached PGP signature against `data` using `pubKey`. */
  private def verifySignature(armoredSignature: Array[Byte], data: Array[Byte], pubKey: PGPPublicKey): Boolean =
    val sigIn         = ByteArrayInputStream(armoredSignature)
    val decoderStream = PGPUtil.getDecoderStream(sigIn).nn
    val factory       = PGPObjectFactory(decoderStream, JcaKeyFingerprintCalculator())
    val sigList       = factory.nextObject().asInstanceOf[PGPSignatureList]
    val parsed        = sigList.get(0).nn
    parsed.init(JcaPGPContentVerifierBuilderProvider(), pubKey)
    parsed.update(data)
    parsed.verify()

  def spec = suite("Signer")(

    test("ascSign produces an ASCII-armored signature that verifies against the generated public key"):
      val passphrase = "test-passphrase"
      val gpgKey     = generateBase64KeyRing("test@example.com", Some(passphrase))
      val pubKey     = publicKeyOf(gpgKey)
      val dataBytes  = "hello world".getBytes("UTF-8").nn
      val data       = Chunk.fromArray(dataBytes)

      val signed: ZIO[Signer, Throwable, Chunk[Byte]] =
        ZIO.serviceWithZIO[Signer]:
          _.ascSign(data).someOrFail(RuntimeException("ascSign returned None"))

      signed.map: sig =>
        val sigBytes = sig.toArray
        val sigText  = String(sigBytes, "UTF-8")
        val verified = verifySignature(sigBytes, dataBytes, pubKey)
        // Tampering with the data should cause verification to fail —
        // sanity-check that the verifier isn't trivially returning true.
        val tamperedFails = !verifySignature(sigBytes, "hello there".getBytes("UTF-8").nn, pubKey)
        assertTrue(
          sig.nonEmpty,
          sigText.startsWith("-----BEGIN PGP SIGNATURE-----"),
          sigText.contains("-----END PGP SIGNATURE-----"),
          verified,
          tamperedFails,
        )
      .provide(Signer.make(gpgKey, Some(passphrase))),

    test("ascSign works with an unencrypted secret key (no passphrase)"):
      // When the secret key isn't encrypted, `Signer.make` should still
      // be able to extract the private key with an empty passphrase
      // (the `getOrElse("")` path inside `Signer.make`).
      val gpgKey    = generateBase64KeyRing("noop@example.com", None)
      val dataBytes = "payload".getBytes("UTF-8").nn
      val data      = Chunk.fromArray(dataBytes)

      ZIO.serviceWithZIO[Signer]:
        _.ascSign(data).someOrFail(RuntimeException("ascSign returned None"))
      .map: sig =>
        val sigText = String(sig.toArray, "UTF-8")
        assertTrue(
          sig.nonEmpty,
          sigText.startsWith("-----BEGIN PGP SIGNATURE-----"),
        )
      .provide(Signer.make(gpgKey, None)),

  )
