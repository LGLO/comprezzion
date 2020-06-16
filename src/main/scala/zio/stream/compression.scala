package zio.stream

import zio._
import zio.console.putStrLn
import zio.stream._
import zio.Chunk.ByteArray

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, FileOutputStream}
import java.nio.file.Path
import java.util.zip.{CRC32, DataFormatException, Inflater}
import scala.annotation.tailrec

package object compression {

  /** Signals that exception occurred in compression/decompression */
  class CompressionException(cause: Exception) extends Exception(cause)

  /**
    * Decompresses deflated stream. Compression method is described in https://tools.ietf.org/html/rfc1951.
    *
    * @param noWrap  wheater wrapped in ZLIB header and trailer, see https://tools.ietf.org/html/rfc1951.
    *                For HTTP 'deflate' content-encoding should be false, see https://tools.ietf.org/html/rfc2616
    * @param bufferSize size of buffer used internally
    * HTTP 'deflate' content-encoding should use nowrap = false. See .
    * */
  def inflate(
      bufferSize: Int = 64 * 1024,
      noWrap: Boolean = false
  ): ZTransducer[Any, CompressionException, Byte, Byte] = {
    def makeInflater(
        bufferSize: Int
    ): ZManaged[Any, Nothing, Option[zio.Chunk[Byte]] => ZIO[Any, CompressionException, Chunk[Byte]]] = {
      ZManaged
        .make(ZIO.effectTotal((new Array[Byte](bufferSize), new Inflater(noWrap)))) {
          case (_, inflater) => ZIO.effectTotal(inflater.end())
        }
        .map {
          case (buffer, inflater) => {
            case None => ZIO.succeed(Chunk.empty)
            case Some(chunk) =>
              ZIO.effectTotal(inflater.setInput(chunk.toArray)) *> pullOutput(inflater, buffer, chunk)
          }
        }
    }

    def pullOutput(
        inflater: Inflater,
        buffer: Array[Byte],
        input: Chunk[Byte]
    ): ZIO[Any, CompressionException, Chunk[Byte]] =
      ZIO
        .effect {
          @tailrec
          def next(prev: Chunk[Byte]): Chunk[Byte] = {
            val read = inflater.inflate(buffer)
            val remaining = inflater.getRemaining()
            val current = Chunk.fromArray(Array.copyOf(buffer, read))
            if (remaining > 0) {
              if (read > 0) next(prev ++ current)
              else if (inflater.finished()) {
                val leftover = input.takeRight(remaining)
                inflater.reset()
                inflater.setInput(leftover.toArray)
                next(prev ++ current)
              } else {
                // Impossible happened (aka programmer error). Die.
                throw new Exception("read = 0, remaining > 0, not finished")
              }
            } else prev ++ current
          }

          if (inflater.needsInput()) Chunk.empty else next(Chunk.empty)
        }
        .refineOrDie {
          case e: DataFormatException => new CompressionException(e)
        }

    ZTransducer(makeInflater(bufferSize))
  }

  def gunzip(bufferSize: Int = 64 * 1024): ZTransducer[Any, Throwable, Byte, Byte] =
    ZTransducer(
      ZManaged
        .make(Gunzipper.make(bufferSize))(_.close)
        .map(gunzipper => {
          case None        => ZIO.succeed(Chunk.empty)
          case Some(chunk) => gunzipper.onChunk(chunk)
        })
    )

  private[compression] class Gunzipper private (var state: Gunzipper.State) {
    def onChunk(c: Chunk[Byte]): ZIO[Any, Throwable, Chunk[Byte]] =
      state.feed(c).map {
        case (newState, output) =>
          state = newState
          output
      }

    def close: UIO[Unit] = state.close
  }

  private[compression] object Gunzipper {

    private val fixedHeaderLength = 10

    sealed trait State {
      def feed: Chunk[Byte] => ZIO[Any, Throwable, (State, Chunk[Byte])]
      def close: UIO[Unit] = ZIO.succeed(())
    }

    private def nextStep(
        bufferSize: Int,
        parsedBytes: Array[Byte],
        checkCrc16: Boolean,
        parseExtra: Boolean,
        commentsToSkip: Int
    ): Gunzipper.State =
      if (parseExtra) new ParseExtraStep(bufferSize, parsedBytes, checkCrc16, commentsToSkip)
      else if (commentsToSkip > 0) new SkipCommentsStep(bufferSize, parsedBytes, checkCrc16, commentsToSkip)
      else if (checkCrc16) new CheckCrc16Step(bufferSize, parsedBytes, Array.empty)
      else Decompress(bufferSize)

    class ParseHeaderStep(bufferSize: Int, oldBytes: Array[Byte]) extends State {

      //TODO: If whole input is shorther than fixed header, not output is produced and no error is singaled. Is it ok?
      def feed: Chunk[Byte] => ZIO[Any, Throwable, (State, Chunk[Byte])] = { c =>
        ZIO.effect {
          val bytes = oldBytes ++ c.toArray
          if (bytes.length < fixedHeaderLength) {
            if (bytes.length == oldBytes.length && c.length > 0)
              ZIO.fail(new CompressionException(new RuntimeException("Invalid GZIP header")))
            else ZIO.succeed((new ParseHeaderStep(bufferSize, bytes), Chunk.empty))
          } else {
            val (header, rest) = bytes.splitAt(fixedHeaderLength)
            if (u8(header(0)) != 31 || u8(header(1)) != 139) ZIO.fail(new Exception("Invalid GZIP header"))
            else if (header(2) != 8)
              ZIO.fail(new Exception(s"Only deflate (8) compression method is supported, present: ${header(2)}"))
            else {
              val flags = header(3) & 0xff
              val checkCrc16 = (flags & 2) > 0
              val hasExtra = (flags & 4) > 0
              val skipFileName = (flags & 8) > 0
              val skipFileComment = (flags & 16) > 0
              val commentsToSkip = (if (skipFileName) 1 else 0) + (if (skipFileComment) 1 else 0)
              val restChunk = Chunk.fromArray(rest)
              nextStep(bufferSize, header, checkCrc16, hasExtra, commentsToSkip).feed(Chunk.fromArray(rest))
            }
          }
        }.flatten
      }
    }

    class ParseExtraStep(bufferSize: Int, bytes: Array[Byte], checkCrc16: Boolean, commentsToSkip: Int) extends State {

      def feed: Chunk[Byte] => ZIO[Any, Throwable, (State, Chunk[Byte])] =
        c =>
          ZIO.effect {
            val header = bytes ++ c.toArray
            if (header.length < 12) {
              ZIO.succeed((new ParseExtraStep(bufferSize, header, checkCrc16, commentsToSkip), Chunk.empty))
            } else {
              val extraBytes: Int = u16(header(fixedHeaderLength), header(fixedHeaderLength + 1))
              val headerWithExtraLength = fixedHeaderLength + extraBytes
              if (header.length < headerWithExtraLength)
                ZIO.succeed((new ParseExtraStep(bufferSize, header, checkCrc16, commentsToSkip), Chunk.empty))
              else {
                val (headerWithExtra, rest) = header.splitAt(headerWithExtraLength)
                nextStep(bufferSize, headerWithExtra, checkCrc16, false, commentsToSkip).feed(Chunk.fromArray(rest))
              }
            }
          }.flatten
    }

    class SkipCommentsStep(bufferSize: Int, pastBytes: Array[Byte], checkCrc16: Boolean, commentsToSkip: Int)
        extends State {
      def feed: Chunk[Byte] => ZIO[Any, Throwable, (State, Chunk[Byte])] =
        c =>
          ZIO.effect {
            val idx = c.indexWhere(_ == 0)
            val (upTo0, rest) = if (idx == -1) (c, Chunk.empty) else c.splitAt(idx + 1)
            nextStep(bufferSize, pastBytes ++ upTo0.toArray, checkCrc16, false, commentsToSkip - 1).feed(rest)
          }.flatten
    }

    class CheckCrc16Step(bufferSize: Int, pastBytes: Array[Byte], pastCrc16Bytes: Array[Byte]) extends State {
      def feed: Chunk[Byte] => ZIO[Any, Throwable, (State, Chunk[Byte])] =
        c =>
          ZIO.effect {
            val (crc16Bytes, rest) = (pastCrc16Bytes ++ c.toArray).splitAt(2)
            if (crc16Bytes.length < 2) {
              ZIO.succeed((new CheckCrc16Step(bufferSize, pastBytes, crc16Bytes), Chunk.empty))
            } else {
              val crc = new CRC32
              crc.update(pastBytes)
              val computedCrc16 = crc.getValue.toInt & 0xffff
              val expectedCrc = u16(crc16Bytes(0), crc16Bytes(1))
              if (computedCrc16 != expectedCrc) ZIO.fail(new Exception("CRC16 checksum mismatch"))
              else Decompress(bufferSize).feed(Chunk.fromArray(rest))
            }
          }.flatten
    }
    private[compression] class Decompress private (buffer: Array[Byte], inflater: Inflater, crc32: CRC32)
        extends State {

      private[compression] def pullOutput(
          inflater: Inflater,
          buffer: Array[Byte]
      ): ZIO[Any, DataFormatException, Chunk[Byte]] =
        ZIO
          .effect {
            @tailrec
            def next(prev: Chunk[Byte]): Chunk[Byte] = {
              val read = inflater.inflate(buffer)
              val newBytes = Array.copyOf(buffer, read)
              crc32.update(newBytes)
              val current = Chunk.fromArray(newBytes)
              val pulled = prev ++ current
              if (read > 0 && inflater.getRemaining > 0) next(pulled) else pulled
            }
            if (inflater.needsInput()) Chunk.empty else next(Chunk.empty)
          }
          .refineOrDie {
            case e: DataFormatException => e
          }

      def feed: Chunk[Byte] => ZIO[Any, Throwable, (State, Chunk[Byte])] =
        chunk => {
          (ZIO.effectTotal(inflater.setInput(chunk.toArray)) *> pullOutput(inflater, buffer)).flatMap { newChunk =>
            if (inflater.finished())
              CheckTrailerStep(buffer.length, crc32, inflater).feed(chunk.takeRight(inflater.getRemaining())).map {
                case (state, _) => (state, newChunk) // CheckTrailerStep returns empty chunk only
              }
            else ZIO.succeed((this, newChunk))
          }
        }
      override def close(): UIO[Unit] = ZIO.succeed(())
    }

    object Decompress {
      def apply(bufferSize: Int) = new Decompress(new Array[Byte](bufferSize), new Inflater(true), new CRC32)
    }

    class CheckTrailerStep(bufferSize: Int, oldBytes: Array[Byte], expectedCrc32: Long, expectedIsize: Long)
        extends State {

      def readInt(a: Array[Byte]): Int = u32(a(0), a(1), a(2), a(3))

      override def feed: Chunk[Byte] => ZIO[Any, Throwable, (State, Chunk[Byte])] =
        c => {
          val bytes = oldBytes ++ c.toArray
          if (bytes.length < 8) ZIO.succeed((this, Chunk.empty))
          else {
            val (trailerBytes, leftover) = bytes.splitAt(8)
            val crc32 = readInt(bytes.take(4))
            val isize = readInt(bytes.drop(4))
            if (expectedCrc32.toInt != crc32) ZIO.fail(new Exception("Invalid CRC32"))
            else if (expectedIsize.toInt != isize) ZIO.fail(new Exception("Invalid ISIZE"))
            else new ParseHeaderStep(bufferSize, leftover).feed(Chunk.empty)
          }
        }
    }
    object CheckTrailerStep {
      def apply(bufferSize: Int, crc32: CRC32, inflater: Inflater) =
        new CheckTrailerStep(bufferSize, Array.empty, crc32.getValue(), inflater.getBytesWritten())
    }

    def make(bufferSize: Int): ZIO[Any, Nothing, Gunzipper] =
      ZIO.succeed(new Gunzipper(new ParseHeaderStep(bufferSize, Array.empty)))

    private def u8(b: Byte): Int = b & 0xff

    private def u16(b1: Byte, b2: Byte): Int = u8(b1) | (u8(b2) << 8)

    private def u32(b1: Byte, b2: Byte, b3: Byte, b4: Byte) = u16(b1, b2) | (u16(b3, b4) << 16)

  }
}
