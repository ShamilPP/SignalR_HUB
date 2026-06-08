import 'dart:convert';
import 'dart:typed_data';

import '../core/signalr_exception.dart';
import 'text_message_format.dart';

/// private
class HandshakeRequestMessage {
  // Properties

  final String protocol;
  final int version;

  // Methods
  HandshakeRequestMessage(this.protocol, this.version);

  Map toJson() => {"protocol": protocol, "version": version};
}

/// private
class HandshakeResponseMessage {
  // Properties
  final String? error;
  final int? minorVersion;

  // Methods
  HandshakeResponseMessage(this.error, this.minorVersion);

  HandshakeResponseMessage.fromJson(Map<String, dynamic> json)
      : error = json["error"],
        minorVersion = json["minorVersion"];
}

/// private
class ParseHandshakeResponseResult {
  // Properites
  /// Either a string (json) or a Uint8List (binary).
  final Object? remainingData;

  /// The HandshakeResponseMessage
  final HandshakeResponseMessage handshakeResponseMessage;

  // Methods
  ParseHandshakeResponseResult(
      this.remainingData, this.handshakeResponseMessage);
}

/// @private
class HandshakeProtocol {
  // Properties

  // Methods

  // Handshake request is always JSON
  String writeHandshakeRequest(HandshakeRequestMessage handshakeRequest) {
    return TextMessageFormat.write(json.encode(handshakeRequest.toJson()));
  }

  /// Parse the handshake reponse
  /// data: either a string (json) or a Uint8List (binary) of the handshake response data.
  ParseHandshakeResponseResult parseHandshakeResponse(Object? data) {
    late String messageData;
    Object? remainingData;

    if (data == null) {
      throw SignalRException(
          message: 'Handshake response data is null.',
          type: SignalRExceptionType.signalr);
    }

    if (data is Uint8List) {
      // Format is binary but still need to read JSON text from handshake response
      final int separatorIndex =
          data.indexOf(TextMessageFormat.RecordSeparatorCode);
      if (separatorIndex == -1) {
        throw SignalRException(
            message: "Message is incomplete.",
            type: SignalRExceptionType.invalidPayload);
      }

      // content before separator is handshake response
      // optional content after is additional messages
      final responseLength = separatorIndex + 1;
      messageData = utf8.decode(data.sublist(0, responseLength));
      remainingData = (data.length > responseLength)
          ? data.sublist(responseLength, data.length)
          : null;
    } else if (data is String) {
      final textData = data;
      final separatorIndex =
          textData.indexOf(TextMessageFormat.recordSeparator);
      if (separatorIndex == -1) {
        throw SignalRException(
            message: "Message is incomplete.",
            type: SignalRExceptionType.invalidPayload);
      }

      // content before separator is handshake response
      // optional content after is additional messages
      final responseLength = separatorIndex + 1;
      messageData = textData.substring(0, responseLength);
      remainingData = (textData.length > responseLength)
          ? textData.substring(responseLength)
          : null;
    } else {
      throw SignalRException(
        message:
            'Handshake response must be String or Uint8List, got ${data.runtimeType}.',
        type: SignalRExceptionType.invalidPayload,
      );
    }

    // At this point we should have just the single handshake message
    final messages = TextMessageFormat.parse(messageData);
    if (messages.isEmpty) {
      throw SignalRException(
          message: 'Handshake response contained no JSON payload.',
          type: SignalRExceptionType.invalidPayload);
    }
    final response =
        HandshakeResponseMessage.fromJson(json.decode(messages[0]));

    // multiple messages could have arrived with handshake
    // return additional data to be parsed as usual, or null if all parsed
    return ParseHandshakeResponseResult(remainingData, response);
  }
}
