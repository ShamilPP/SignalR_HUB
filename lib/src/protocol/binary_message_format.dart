import 'dart:typed_data';
import 'dart:math';

class BinaryMessageFormat {
  // Properties

  static const RecordSeparatorCode = 0x1e;
  static String recordSeparator =
      String.fromCharCode(BinaryMessageFormat.RecordSeparatorCode);

  static Uint8List write(Uint8List output) {
    var size = output.length;

    final lenBuffer = [];
    do {
      var sizePart = size & 0x7f;
      size = size >> 7;
      if (size > 0) {
        sizePart |= 0x80;
      }
      lenBuffer.add(sizePart);
    } while (size > 0);

    size = output.length;

    final buf = Uint8List(lenBuffer.length + size);
    final dat = ByteData.view(buf.buffer, buf.offsetInBytes);
    var offset = 0;
    final builder = BytesBuilder(copy: false);

    for (var element in lenBuffer) {
      dat.setUint8(offset, element);
      offset++;
    }
    for (var element in output) {
      dat.setUint8(offset, element);
      offset++;
    }
    builder.add(Uint8List.view(
      buf.buffer,
      buf.offsetInBytes,
      offset,
    ));
    final x = builder.takeBytes();
    return x;
  }

  static List<Uint8List> parse(Uint8List input) {
    final List<Uint8List> result = [];
    final uint8Array = input;
    const maxLengthPrefixSize = 5;
    const numBitsToShift = [0, 7, 14, 21, 28];

    for (var offset = 0; offset < input.length;) {
      var numBytes = 0;
      var size = 0;
      var byteRead = 0;
      do {
        byteRead = uint8Array[offset + numBytes];
        size = size | ((byteRead & 0x7f) << (numBitsToShift[numBytes]));
        numBytes++;
      } while (numBytes < min(maxLengthPrefixSize, input.length - offset) &&
          (byteRead & 0x80) != 0);

      if ((byteRead & 0x80) != 0 && numBytes < maxLengthPrefixSize) {
        throw Exception("Cannot read message size.");
      }

      if (numBytes == maxLengthPrefixSize && byteRead > 7) {
        throw Exception("Messages bigger than 2GB are not supported.");
      }

      if (uint8Array.length >= (offset + numBytes + size)) {
        result.add(
            uint8Array.sublist(offset + numBytes, offset + numBytes + size));
      } else {
        throw Exception("Incomplete message.");
      }

      offset = offset + numBytes + size;
    }

    return result;
  }
}
