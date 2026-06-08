import 'dart:collection';

import 'package:logging/logging.dart';

import '../core/signalr_exception.dart';
import '../core/itransport.dart';

/// Defines the type of a Hub Message.
enum MessageType {
  /// MessageType is not defined.
  undefined, // = 0,
  /// Indicates the message is an Invocation message and implements the {@link @microsoft/signalr.InvocationMessage} interface.
  invocation, // = 1,
  /// Indicates the message is a StreamItem message and implements the {@link @microsoft/signalr.StreamItemMessage} interface.
  streamItem, // = 2,
  /// Indicates the message is a Completion message and implements the {@link @microsoft/signalr.CompletionMessage} interface.
  completion, // = 3,
  /// Indicates the message is a Stream Invocation message and implements the {@link @microsoft/signalr.StreamInvocationMessage} interface.
  streamInvocation, // = 4,
  /// Indicates the message is a Cancel Invocation message and implements the {@link @microsoft/signalr.CancelInvocationMessage} interface.
  cancelInvocation, // = 5,
  /// Indicates the message is a Ping message and implements the {@link @microsoft/signalr.PingMessage} interface.
  ping, // = 6,
  /// Indicates the message is a Close message and implements the {@link @microsoft/signalr.CloseMessage} interface.
  close, // = 7,
}

MessageType? parseMessageTypeFromString(int? value) {
  if (value == null) {
    return null;
  }

  switch (value) {
    case 1:
      return MessageType.invocation;
    case 2:
      return MessageType.streamItem;
    case 3:
      return MessageType.completion;
    case 4:
      return MessageType.streamInvocation;
    case 5:
      return MessageType.cancelInvocation;
    case 6:
      return MessageType.ping;
    case 7:
      return MessageType.close;
    default:
      throw SignalRException(
          message: 'A MessageType of $value is not supported.',
          type: SignalRExceptionType.signalr);
  }
}

/// String-keyed headers for hub messages.
class MessageHeaders {
  static const String AuthorizationHeaderName = "Authorization";

  final HashMap<String, String> _map = HashMap<String, String>();

  Iterable<String> get names => _map.keys;

  Map<String, String> get asMap => _map;

  bool get isEmpty => _map.isEmpty;

  String? getHeaderValue(String name) => _map[name];

  void setHeaderValue(String name, String value) {
    _map[name] = value;
  }

  void removeHeader(String name) {
    _map.remove(name);
  }

  void addMessageHeaders(MessageHeaders? newHeaders) {
    if (newHeaders == null || newHeaders.isEmpty) return;
    for (final name in newHeaders.names) {
      final value = newHeaders.getHeaderValue(name);
      if (value != null) {
        setHeaderValue(name, value);
      }
    }
  }

  @override
  String toString() {
    if (isEmpty) return '{}';
    final buf = StringBuffer();
    for (final name in names) {
      if (buf.isNotEmpty) buf.write(', ');
      buf.write('{ $name: ${_map[name]} }');
    }
    return buf.toString();
  }
}

/// Defines properties common to all Hub messages.
abstract class HubMessageBase {
  // Properties

  /// A [MessageType] value indicating the type of this message.
  final MessageType type;

  // Methods
  HubMessageBase(MessageType type) : type = type;
}

/// Defines properties common to all Hub messages relating to a specific invocation.
abstract class HubInvocationMessage extends HubMessageBase {
  // Properties
  /// A {@link @microsoft/signalr.MessageHeaders} dictionary containing headers attached to the message.
  final MessageHeaders headers;

  ///The ID of the invocation relating to this message.
  ///
  ///This is expected to be present for StreamInvocationMessage and CompletionMessage. It may
  ///be 'undefined' for an InvocationMessage if the sender does not expect a response.
  final String? invocationId;

  // Methods
  HubInvocationMessage(
      super.messageType, MessageHeaders? headers, String? invocationId)
      : headers = headers ?? MessageHeaders(),
        invocationId = invocationId;
}

/// A hub message representing a non-streaming invocation.
class InvocationMessage extends HubInvocationMessage {
  // Properites

  /// The target method name.
  final String? target;

  /// The target method arguments.
  final List<Object?>? arguments;

  /// The target method's stream IDs.
  final List<String>? streamIds;

  // Methods
  InvocationMessage(
      {String? target,
      List<Object?>? arguments,
      List<String>? streamIds,
      MessageHeaders? headers,
      String? invocationId})
      : target = target,
        arguments = arguments,
        streamIds = streamIds,
        super(MessageType.invocation, headers, invocationId);
  @override
  String toString() {
    return 'InvocationMessage - type: ${type.index}, headers: ${headers.toString()}, invocationId: $invocationId, target: $target, arguments: $arguments, streamIds: $streamIds';
  }
}

/// A hub message representing a streaming invocation.
class StreamInvocationMessage extends HubInvocationMessage {
  // Properites

  /// The target method name.
  final String? target;

  /// The target method arguments.
  final List<Object?>? arguments;

  /// The target method's stream IDs.
  final List<String>? streamIds;

  // Methods
  StreamInvocationMessage(
      {String? target,
      List<Object?>? arguments,
      List<String>? streamIds,
      MessageHeaders? headers,
      String? invocationId})
      : target = target,
        arguments = arguments,
        streamIds = streamIds,
        super(MessageType.streamInvocation, headers, invocationId);

  @override
  String toString() {
    return 'StreamInvocationMessage - type: ${type.index}, headers: ${headers.toString()}, invocationId: $invocationId, target: $target, arguments: $arguments, streamIds: $streamIds';
  }
}

/// A hub message representing a single item produced as part of a result stream.
class StreamItemMessage extends HubInvocationMessage {
  // Properites

  /// The item produced by the server.
  final Object? item;

  // Methods
  StreamItemMessage(
      {Object? item, MessageHeaders? headers, String? invocationId})
      : item = item,
        super(MessageType.streamItem, headers, invocationId);

  @override
  String toString() {
    return 'StreamItemMessage - type: ${type.index}, headers: ${headers.toString()}, invocationId: $invocationId, item: $item';
  }
}

/// A hub message representing the result of an invocation.
class CompletionMessage extends HubInvocationMessage {
  // Properties

  /// The error produced by the invocation, if any.
  ///
  /// Either CompletionMessage.error CompletionMessage.result must be defined, but not both.
  final String? error;

  /// The result produced by the invocation, if any.
  ///
  /// Either {@link @microsoft/signalr.CompletionMessage.error} or {@link @microsoft/signalr.CompletionMessage.result} must be defined, but not both.
  final Object? result;

  // Methods
  CompletionMessage(
      {String? error,
      Object? result,
      MessageHeaders? headers,
      String? invocationId})
      : error = error,
        result = result,
        super(MessageType.completion, headers, invocationId);
  @override
  String toString() {
    return 'CompletionMessage - type: ${type.index}, headers: ${headers.toString()}, invocationId: $invocationId, error: $error, result: $result';
  }
}

/// A hub message indicating that the sender is still active.
class PingMessage extends HubMessageBase {
  // Methods

  PingMessage() : super(MessageType.ping);
  @override
  String toString() {
    return 'PingMessage - type: ${type.index}';
  }
}

/// A hub message indicating that the sender is closing the connection.
///
/// If {@link @microsoft/signalr.CloseMessage.error} is defined, the sender is closing the connection due to an error.
///
class CloseMessage extends HubMessageBase {
  // Properites

  /// The error that triggered the close, if any.
  ///
  /// If this property is undefined, the connection was closed normally and without error.
  final String? error;

  /// If true, clients with automatic reconnects enabled should attempt to reconnect after receiving the CloseMessage. Otherwise, they should not. */
  final bool? allowReconnect;

  //Methods
  CloseMessage({String? error, bool? allowReconnect})
      : error = error,
        allowReconnect = allowReconnect,
        super(MessageType.close);

  @override
  String toString() {
    return 'CloseMessage - type: $type.index, allowReconnect: $allowReconnect, error: $error';
  }
}

/// A hub message sent to request that a streaming invocation be canceled.
class CancelInvocationMessage extends HubInvocationMessage {
  // Methods
  CancelInvocationMessage({MessageHeaders? headers, String? invocationId})
      : super(MessageType.cancelInvocation, headers, invocationId);

  @override
  String toString() {
    return 'CancelInvocationMessage - type: ${type.index}, headers: ${headers.toString()}, invocationId: $invocationId';
  }
}

/// A protocol abstraction for communicating with SignalR Hubs.
abstract class IHubProtocol {
  // Properties

  /// The name of the protocol. This is used by SignalR to resolve the protocol between the client and server.
  final String name;

  /// The version of the protocol.
  final int version;

  /// The TransferFormat of the protocol. */
  final TransferFormat transferFormat;

  // Methods
  IHubProtocol(String name, int number, TransferFormat transferFormat)
      : name = name,
        version = number,
        transferFormat = transferFormat;

  /// Creates an array of {@link @microsoft/signalr.HubMessage} objects from the specified serialized representation.
  ///
  /// If transferFormat is 'Text', the `input` parameter must be a string, otherwise it must be an ArrayBuffer.
  ///
  /// [input] A string (json), or Uint8List (binary) containing the serialized representation.
  /// [Logger] logger A logger that will be used to log messages that occur during parsing.

  List<HubMessageBase> parseMessages(Object input, Logger logger);

  /// Writes the specified HubMessage to a string or ArrayBuffer and returns it.
  ///
  /// If transferFormat is 'Text', the result of this method will be a string, otherwise it will be an ArrayBuffer.
  ///
  /// [message] The message to write.
  /// returns  A string or ArrayBuffer containing the serialized representation of the message.

  Object writeMessage(HubMessageBase message);
}
