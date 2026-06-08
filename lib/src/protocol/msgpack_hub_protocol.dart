import 'dart:typed_data';
import 'package:logging/logging.dart';
import "package:message_pack_dart/message_pack_dart.dart" as msgpack;

import '../core/signalr_exception.dart';
import '../core/itransport.dart';
import 'binary_message_format.dart';
import 'ihub_protocol.dart';

const String MSGPACK_HUB_PROTOCOL_NAME = "messagepack";
const int PROTOCOL_VERSION = 1;
const TransferFormat TRANSFER_FORMAT = TransferFormat.binary;

class MessagePackHubProtocol implements IHubProtocol {
  @override
  String get name => MSGPACK_HUB_PROTOCOL_NAME;
  @override
  TransferFormat get transferFormat => TRANSFER_FORMAT;

  @override
  int get version => PROTOCOL_VERSION;

  static const _errorResult = 1;
  static const _voidResult = 2;
  static const _nonVoidResult = 3;

  @override
  List<HubMessageBase> parseMessages(Object input, Logger logger) {
    if (input is! Uint8List) {
      throw SignalRException(
          message:
              "Invalid input for MessagePack hub protocol. Expected an Uint8List.",
          type: SignalRExceptionType.invalidPayload);
    }

    final binaryInput = input;
    final List<HubMessageBase> hubMessages = [];

    final messages = BinaryMessageFormat.parse(binaryInput);
    if (messages.isEmpty) {
      throw SignalRException(
          message: 'No MessagePack frames in payload.',
          type: SignalRExceptionType.invalidPayload);
    }

    for (var message in messages) {
      if (message.isEmpty) {
        throw SignalRException(
            message: 'Empty MessagePack frame in payload.',
            type: SignalRExceptionType.invalidPayload);
      }

      final unpackedData = msgpack.deserialize(message);
      List<dynamic> unpackedList;
      if (unpackedData == null) {
        throw SignalRException(
            message: 'MessagePack deserialized to null.',
            type: SignalRExceptionType.invalidPayload);
      }
      try {
        unpackedList = List<dynamic>.from(unpackedData as List<dynamic>);
      } catch (_) {
        throw SignalRException(
            message: "Invalid payload.",
            type: SignalRExceptionType.invalidPayload);
      }
      if (unpackedList.isEmpty) {
        throw SignalRException(
            message: 'MessagePack message array is empty.',
            type: SignalRExceptionType.invalidPayload);
      }
      final messageObj = _parseMessage(unpackedList, logger);
      if (messageObj != null) {
        hubMessages.add(messageObj);
      }
    }
    return hubMessages;
  }

  static HubMessageBase? _parseMessage(List<dynamic> data, Logger logger) {
    if (data.isEmpty) {
      throw SignalRException(
          message: "Invalid payload.",
          type: SignalRExceptionType.invalidPayload);
    }
    HubMessageBase? messageObj;

    final rawType = data[0];
    if (rawType is! int) {
      throw SignalRException(
          message:
              "Invalid message type value: expected int, got ${rawType.runtimeType}.",
          type: SignalRExceptionType.invalidPayload);
    }
    final messageType = rawType;

    if (messageType == MessageType.invocation.index) {
      messageObj = _createInvocationMessage(data);
      return messageObj;
    }

    if (messageType == MessageType.streamInvocation.index) {
      messageObj = _createStreamInvocationMessage(data);
      return messageObj;
    }

    if (messageType == MessageType.streamItem.index) {
      messageObj = _createStreamItemMessage(data);
      return messageObj;
    }

    if (messageType == MessageType.completion.index) {
      messageObj = _createCompletionMessage(data);
      return messageObj;
    }

    if (messageType == MessageType.ping.index) {
      messageObj = _createPingMessage(data);
      return messageObj;
    }

    if (messageType == MessageType.close.index) {
      messageObj = _createCloseMessage(data);
      return messageObj;
    } else {
      // Future protocol changes can add message types, old clients can ignore them
      logger.info("Unknown message type '$messageType' ignored.");
      return messageObj;
    }
  }

  static MessageHeaders createMessageHeaders(List<dynamic> data) {
    if (data.length < 2) {
      throw SignalRException(
          message: "Invalid headers",
          type: SignalRExceptionType.invalidPayload);
    }
    final raw = data[1];
    if (raw == null) return MessageHeaders();
    if (raw is! Map) {
      throw SignalRException(
          message: "Invalid headers",
          type: SignalRExceptionType.invalidPayload);
    }
    final headers = MessageHeaders();
    for (final entry in raw.entries) {
      headers.setHeaderValue(
        entry.key.toString(),
        entry.value?.toString() ?? '',
      );
    }
    return headers;
  }

  static InvocationMessage _createInvocationMessage(List<dynamic> data) {
    if (data.length < 5) {
      throw SignalRException(
          message: "Invalid payload for Invocation message.",
          type: SignalRExceptionType.invalidPayload);
    }

    final MessageHeaders headers = createMessageHeaders(data);

    final rawArgs = data[4];
    if (rawArgs is! List) {
      throw SignalRException(
          message:
              "Invalid payload for Invocation message: arguments must be a List.",
          type: SignalRExceptionType.invalidPayload);
    }

    return InvocationMessage(
        target: data[3] as String?,
        headers: headers,
        invocationId: data[2] as String?,
        streamIds: [],
        arguments: List<Object?>.from(rawArgs));
  }

  static StreamInvocationMessage _createStreamInvocationMessage(
      List<dynamic> data) {
    if (data.length < 5) {
      throw SignalRException(
          message: "Invalid payload for StreamInvocation message.",
          type: SignalRExceptionType.invalidPayload);
    }

    final MessageHeaders headers = createMessageHeaders(data);

    final rawArgs = data[4];
    if (rawArgs is! List) {
      throw SignalRException(
          message:
              "Invalid payload for StreamInvocation message: arguments must be a List.",
          type: SignalRExceptionType.invalidPayload);
    }

    return StreamInvocationMessage(
      target: data[3] as String?,
      headers: headers,
      invocationId: data[2] as String?,
      arguments: List<Object?>.from(rawArgs),
      streamIds: data.length > 5 && data[5] is List
          ? List<String>.from(data[5] as List)
          : null,
    );
  }

  static StreamItemMessage _createStreamItemMessage(List<dynamic> data) {
    if (data.length < 4) {
      throw SignalRException(
          message: "Invalid payload for StreamItem message.",
          type: SignalRExceptionType.invalidPayload);
    }
    final MessageHeaders headers = createMessageHeaders(data);
    final message = StreamItemMessage(
      item: data[3] as Object?,
      headers: headers,
      invocationId: data[2] as String?,
    );

    return message;
  }

  static CompletionMessage _createCompletionMessage(List<dynamic> data) {
    if (data.length < 4) {
      throw SignalRException(
          message: "Invalid payload for Completion message.",
          type: SignalRExceptionType.invalidPayload);
    }
    final MessageHeaders headers = createMessageHeaders(data);
    final resultKind = data[3];
    if (resultKind != _voidResult && data.length < 5) {
      throw SignalRException(
          message: "Invalid payload for Completion message.",
          type: SignalRExceptionType.invalidPayload);
    }

    if (resultKind == _errorResult) {
      return CompletionMessage(
        error: data[4] as String?,
        result: null,
        headers: headers,
        invocationId: data[2] as String?,
      );
    } else if (resultKind == _nonVoidResult) {
      return CompletionMessage(
        result: data[4] as Object?,
        error: null,
        headers: headers,
        invocationId: data[2] as String?,
      );
    } else {
      return CompletionMessage(
        headers: headers,
        result: null,
        error: null,
        invocationId: data[2] as String?,
      );
    }
  }

  static PingMessage _createPingMessage(List<dynamic> data) {
    if (data.isEmpty) {
      throw SignalRException(
          message: "Invalid payload for Ping message.",
          type: SignalRExceptionType.invalidPayload);
    }
    return PingMessage();
  }

  static CloseMessage _createCloseMessage(List<dynamic> data) {
    if (data.length < 2) {
      throw SignalRException(
          message: "Invalid payload for Close message.",
          type: SignalRExceptionType.invalidPayload);
    }
    if (data.length >= 3) {
      return CloseMessage(allowReconnect: data[2], error: data[1]);
    } else {
      return CloseMessage(error: data[1]);
    }
  }

  @override
  Object writeMessage(HubMessageBase message) {
    final messageType = message.type;
    switch (messageType) {
      case MessageType.invocation:
        return _writeInvocation(message as InvocationMessage);
      case MessageType.streamInvocation:
        return _writeStreamInvocation(message as StreamInvocationMessage);
      case MessageType.streamItem:
        return _writeStreamItem(message as StreamItemMessage);
      case MessageType.completion:
        return _writeCompletion(message as CompletionMessage);
      case MessageType.ping:
        return _writePing();
      case MessageType.cancelInvocation:
        return _writeCancelInvocation(message as CancelInvocationMessage);
      default:
        throw SignalRException(
            message: "Invalid message type.",
            type: SignalRExceptionType.invalidPayload);
    }
  }

  /// Serializes a payload list with MessagePack and wraps it in a binary frame.
  static Uint8List _packAndFrame(List<dynamic> payload) {
    return BinaryMessageFormat.write(msgpack.serialize(payload));
  }

  static Uint8List _writeInvocation(InvocationMessage message) {
    final payload = <dynamic>[
      MessageType.invocation.index,
      message.headers.asMap,
      message.invocationId,
      message.target,
      message.arguments,
    ];
    if (message.streamIds != null && message.streamIds!.isNotEmpty) {
      payload.add(message.streamIds);
    }
    return _packAndFrame(payload);
  }

  static Uint8List _writeStreamInvocation(StreamInvocationMessage message) {
    final payload = <dynamic>[
      MessageType.streamInvocation.index,
      message.headers.asMap,
      message.invocationId,
      message.target,
      message.arguments,
    ];
    if (message.streamIds != null && message.streamIds!.isNotEmpty) {
      payload.add(message.streamIds);
    }
    return _packAndFrame(payload);
  }

  static Uint8List _writeStreamItem(StreamItemMessage message) {
    return _packAndFrame([
      MessageType.streamItem.index,
      message.headers.asMap,
      message.invocationId,
      message.item,
    ]);
  }

  static Uint8List _writeCompletion(CompletionMessage message) {
    final resultKind = (message.error != null)
        ? _errorResult
        : (message.result != null)
            ? _nonVoidResult
            : _voidResult;
    final payload = <dynamic>[
      MessageType.completion.index,
      message.headers.asMap,
      message.invocationId,
      resultKind,
    ];
    if (resultKind != _voidResult) {
      payload.add(resultKind == _errorResult ? message.error : message.result);
    }
    return _packAndFrame(payload);
  }

  static Uint8List _writeCancelInvocation(CancelInvocationMessage message) {
    return _packAndFrame([
      MessageType.cancelInvocation.index,
      message.headers.asMap,
      message.invocationId,
    ]);
  }

  static Uint8List _writePing() {
    return _packAndFrame([MessageType.ping.index]);
  }
}
