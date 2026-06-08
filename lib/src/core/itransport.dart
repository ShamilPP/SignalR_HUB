import 'dart:async';

import 'signalr_exception.dart';

/// Specifies a specific HTTP transport type.
enum HttpTransportType {
  /// Specified no transport preference.
  none,

  /// Specifies the WebSockets transport.
  webSockets,

  /// Specifies the Server-Sent Events transport.
  serverSentEvents,

  /// Specifies the Long Polling transport.
  longPolling;

  bool get isNone => this == HttpTransportType.none;
  bool get isWebSockets => this == HttpTransportType.webSockets;
  bool get isServerSentEvents => this == HttpTransportType.serverSentEvents;
  bool get isLongPolling => this == HttpTransportType.longPolling;
}

HttpTransportType httpTransportTypeFromString(String? value) {
  if (value == null || value == "") {
    return HttpTransportType.none;
  }

  value = value.toUpperCase();
  switch (value) {
    case "WEBSOCKETS":
      return HttpTransportType.webSockets;
    case "SERVERSENTEVENTS":
      return HttpTransportType.serverSentEvents;
    case "LONGPOLLING":
      return HttpTransportType.longPolling;
    default:
      throw SignalRException(
          message: "$value is not a supported HttpTransportType",
          type: SignalRExceptionType.signalr);
  }
}

/// Specifies the transfer format for a connection.
enum TransferFormat {
  /// TransferFormat is not defined.
  undefined,

  /// Specifies that only text data will be transmitted over the connection.
  text,

  /// Specifies that binary data will be transmitted over the connection.
  binary,
}

TransferFormat getTransferFormatFromString(String? value) {
  if (value == null || value == "") {
    return TransferFormat.undefined;
  }

  value = value.toUpperCase();
  switch (value) {
    case "TEXT":
      return TransferFormat.text;
    case "BINARY":
      return TransferFormat.binary;
    default:
      throw SignalRException(
          message: "$value is not a supported TransferFormat",
          type: SignalRExceptionType.signalr);
  }
}

/// Data received call back.
/// data: the content. Either a string (json) or Uint8List (binary)
typedef OnReceive = void Function(Object? data);

typedef OnClose = void Function({Object? error});

typedef AccessTokenFactory = Future<String> Function();

/// An abstraction over the behavior of transports.
abstract class ITransport {
  Future<void> connect(String? url, TransferFormat transferFormat);

  /// data: the content. Either a string (json) or Uint8List (binary)
  Future<void> send(Object data);
  Future<void> stop();
  OnReceive? onReceive;
  OnClose? onClose;
}
