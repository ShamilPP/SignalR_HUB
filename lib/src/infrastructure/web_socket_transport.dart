import 'dart:async';
import 'dart:io' as io;
import 'package:flutter/foundation.dart';
import 'package:logging/logging.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import '../core/signalr_exception.dart';
import '../core/itransport.dart';
import '../protocol/ihub_protocol.dart';
import '../shared/utils.dart';

class WebSocketTransport implements ITransport {
  final Logger _logger;
  final AccessTokenFactory? _accessTokenFactory;
  final bool _logMessageContent;
  WebSocketChannel? _webSocket;
  StreamSubscription<Object?>? _webSocketListenSub;
  final MessageHeaders? _headers;
  final Future<WebSocketChannel> Function(
      Uri uri, Map<String, String>? headers)? _webSocketChannelFactory;

  bool _closing = false;
  bool _onCloseRaised = false;
  bool _connectFutureCompleted = false;

  @override
  OnClose? onClose;

  @override
  OnReceive? onReceive;

  WebSocketTransport(
    this._accessTokenFactory,
    this._logger,
    this._logMessageContent,
    this._headers,
    this._webSocketChannelFactory,
  );

  void _raiseOnClose([Exception? error]) {
    if (_onCloseRaised) return;
    _onCloseRaised = true;
    onClose?.call(error: error);
  }

  @override
  Future<void> connect(String? url, TransferFormat transferFormat) async {
    var connectUrl = url;
    if (connectUrl == null) {
      throw ArgumentError.notNull('url');
    }

    _logger.finest("(WebSockets transport) Connecting");

    var headers = _headers?.asMap ?? <String, String>{};

    final tokenFactory = _accessTokenFactory;
    if (tokenFactory != null) {
      final token = await tokenFactory();
      if (!isStringEmpty(token)) {
        if (kIsWeb) {
          final encodedToken = Uri.encodeComponent(token);
          connectUrl =
              "$connectUrl${connectUrl.contains('?') ? '&' : '?'}access_token=$encodedToken";
        } else {
          headers = Map<String, String>.from(headers);
          headers['Authorization'] = 'Bearer $token';
        }
      }
    }

    final websocketCompleter = Completer<void>();
    var opened = false;
    final wsUrl = normalizeWebSocketConnectUrl(connectUrl);
    final safeUrl = sanitizeUrlForLogging(wsUrl);
    _logger.finest("WebSocket try connecting to '$safeUrl'.");

    try {
      final WebSocketChannel channel;
      final uri = Uri.parse(wsUrl);
      if (_webSocketChannelFactory != null) {
        channel = await _webSocketChannelFactory!(uri, headers);
      } else if (kIsWeb) {
        channel = WebSocketChannel.connect(uri);
      } else {
        // ignore: close_sinks — closed via IOWebSocketChannel in _closeInternal()
        final webSocket = await io.WebSocket.connect(wsUrl, headers: headers);
        channel = IOWebSocketChannel(webSocket);
      }

      await channel.ready;

      _webSocket = channel;
      opened = true;
      if (!websocketCompleter.isCompleted) {
        websocketCompleter.complete();
      }
      _connectFutureCompleted = true;
      _logger.info("WebSocket connected to '$safeUrl'.");

      _webSocketListenSub = channel.stream.listen(
        (Object? message) {
          if (_logMessageContent && message is String) {
            _logger.finest(
              "(WebSockets transport) data received. message ${getDataDetail(message, _logMessageContent)}.",
            );
          } else {
            _logger.finest("(WebSockets transport) data received.");
          }
          final recv = onReceive;
          if (recv != null) {
            try {
              recv(message);
            } catch (error, st) {
              _logger.severe(
                "(WebSockets transport) error calling onReceive, error: $error",
              );
              unawaited(_closeInternal(
                error: SignalRException.handler(
                    error: error,
                    message: error.toString(),
                    type: SignalRExceptionType.signalr,
                    stackTrace: st),
              ));
            }
          }
        },
        onError: (Object? error, StackTrace st) {
          final ex = SignalRException.handler(
              error: error,
              message: error.toString(),
              type: SignalRExceptionType.signalr,
              stackTrace: st);
          if (!websocketCompleter.isCompleted) {
            websocketCompleter.completeError(ex);
          } else if (_connectFutureCompleted) {
            unawaited(_closeInternal(error: ex));
          }
        },
        onDone: () {
          if (opened) {
            if (_connectFutureCompleted) {
              unawaited(_closeInternal());
            }
          } else {
            if (!websocketCompleter.isCompleted) {
              websocketCompleter.completeError(
                SignalRException(
                    message: "There was an error with the transport.",
                    type: SignalRExceptionType.signalr),
              );
            }
          }
        },
        cancelOnError: false,
      );
    } catch (e, st) {
      if (!websocketCompleter.isCompleted) {
        websocketCompleter.completeError(SignalRException.handler(
            error: e,
            message: e.toString(),
            type: SignalRExceptionType.signalr,
            stackTrace: st));
      }
      _logger.severe("WebSocket connection to '$safeUrl' failed: $e");
    }

    return websocketCompleter.future;
  }

  Future<void> _closeInternal({Exception? error}) async {
    if (_closing) {
      if (error != null) _raiseOnClose(error);
      return;
    }
    _closing = true;

    await _webSocketListenSub?.cancel();
    _webSocketListenSub = null;

    try {
      await _webSocket?.sink.close();
    } catch (e) {
      _logger.severe("(WebSockets transport) sink.close error: $e");
    }
    _webSocket = null;

    _logger.finest("(WebSockets transport) socket closed.");
    _raiseOnClose(error);
  }

  @override
  Future<void> send(Object data) {
    final socket = _webSocket;
    if (socket != null) {
      _logger.finest(
        "(WebSockets transport) sending data. ${getDataDetail(data, true)}.",
      );

      if (data is String) {
        socket.sink.add(data);
      } else if (data is Uint8List) {
        socket.sink.add(data);
      } else {
        throw SignalRException(
            message: "Content type is not handled.",
            type: SignalRExceptionType.signalr);
      }

      return Future.value();
    }

    return Future.error(
      SignalRException(
          message: "WebSocket is not in the OPEN state",
          type: SignalRExceptionType.signalr),
    );
  }

  @override
  Future<void> stop() async {
    await _closeInternal();
  }
}
