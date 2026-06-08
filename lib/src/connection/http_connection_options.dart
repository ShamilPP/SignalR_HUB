import 'package:equatable/equatable.dart';
import 'package:logging/logging.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import '../core/itransport.dart';
import '../di/signalr_locator.dart';
import '../protocol/ihub_protocol.dart';
import '../protocol/signalr_http_client.dart';

/// Immutable options for HTTP-based transports.
class HttpConnectionOptions extends Equatable {
  final SignalRHttpClient? httpClient;
  final Object? transport;
  final Logger logger;
  final AccessTokenFactory? accessTokenFactory;
  final MessageHeaders? headers;
  final bool logMessageContent;
  final bool skipNegotiation;
  final int requestTimeout;
  final Future<WebSocketChannel> Function(
      Uri uri, Map<String, String>? headers)? webSocketChannelFactory;

  /// Maximum number of negotiate redirects before giving up. Default: 100.
  final int maxRedirects;

  /// Timeout in milliseconds for long polling requests. Default: 100000 (100s).
  final int longPollingTimeoutMs;

  HttpConnectionOptions({
    this.httpClient,
    this.transport,
    Logger? logger,
    this.accessTokenFactory,
    this.headers,
    this.logMessageContent = false,
    this.skipNegotiation = false,
    this.requestTimeout = 2000,
    this.maxRedirects = 100,
    this.longPollingTimeoutMs = 100000,
    this.webSocketChannelFactory,
  }) : logger = resolveSignalRLogger(logger);

  HttpConnectionOptions copyWith({
    SignalRHttpClient? httpClient,
    Object? transport,
    Logger? logger,
    AccessTokenFactory? accessTokenFactory,
    MessageHeaders? headers,
    bool? logMessageContent,
    bool? skipNegotiation,
    int? requestTimeout,
    int? maxRedirects,
    int? longPollingTimeoutMs,
    Future<WebSocketChannel> Function(Uri uri, Map<String, String>? headers)?
        webSocketChannelFactory,
  }) {
    return HttpConnectionOptions(
      httpClient: httpClient ?? this.httpClient,
      transport: transport ?? this.transport,
      logger: logger ?? this.logger,
      accessTokenFactory: accessTokenFactory ?? this.accessTokenFactory,
      headers: headers ?? this.headers,
      logMessageContent: logMessageContent ?? this.logMessageContent,
      skipNegotiation: skipNegotiation ?? this.skipNegotiation,
      requestTimeout: requestTimeout ?? this.requestTimeout,
      maxRedirects: maxRedirects ?? this.maxRedirects,
      longPollingTimeoutMs: longPollingTimeoutMs ?? this.longPollingTimeoutMs,
      webSocketChannelFactory:
          webSocketChannelFactory ?? this.webSocketChannelFactory,
    );
  }

  @override
  List<Object?> get props => [
        httpClient,
        transport,
        logger,
        accessTokenFactory,
        headers,
        logMessageContent,
        skipNegotiation,
        requestTimeout,
        maxRedirects,
        longPollingTimeoutMs,
        webSocketChannelFactory,
      ];
}
