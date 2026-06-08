import 'dart:async';
import 'dart:convert';

import 'package:logging/logging.dart';

import '../core/signalr_exception.dart';
import '../core/iconnection.dart';
import '../core/itransport.dart';
import '../infrastructure/long_polling_transport.dart';
import '../infrastructure/server_sent_events_transport.dart';
import '../infrastructure/web_socket_transport.dart';
import '../infrastructure/web_supporting_http_client.dart';
import '../protocol/ihub_protocol.dart';
import '../protocol/signalr_http_client.dart';
import 'http_connection_options.dart';
import 'negotiate_models.dart';
import 'transport_send_queue.dart';

class HttpConnection implements IConnection {
  ConnectionState? _connectionState;
  late bool _connectionStarted;
  late SignalRHttpClient _httpClient;
  final Logger _logger;
  late HttpConnectionOptions _options;
  ITransport? _transport;
  Future<void>? _startInternalPromise;
  Future<void>? _stopPromise;
  late Completer<void> _stopPromiseCompleter;
  Object? _stopError;
  AccessTokenFactory? _accessTokenFactory;
  TransportSendQueue? _sendQueue;

  @override
  ConnectionFeatures? features;

  @override
  String? baseUrl;

  @override
  String? connectionId;

  @override
  OnReceive? onreceive;

  @override
  OnClose? onclose;

  final int _negotiateVersion = 1;

  HttpConnection(String url, {required HttpConnectionOptions options})
      : _logger = options.logger {
    baseUrl = url;
    _options = options;
    _httpClient = options.httpClient ?? WebSupportingHttpClient(_logger);
    _connectionState = ConnectionState.disconnected;
    _connectionStarted = false;
  }

  @override
  Future<void> start({TransferFormat? transferFormat}) async {
    transferFormat = transferFormat ?? TransferFormat.binary;

    _logger.finer(
      "Starting connection with transfer format '$transferFormat'.",
    );

    if (_connectionState != ConnectionState.disconnected) {
      return Future.error(SignalRException(
        message:
            "Cannot start a connection that is not in the 'Disconnected' state.",
        type: SignalRExceptionType.signalr,
      ));
    }

    _connectionState = ConnectionState.connecting;

    _startInternalPromise = _startInternal(transferFormat);
    await _startInternalPromise;

    if (_connectionState == ConnectionState.disconnecting) {
      const message =
          "Failed to start the HttpConnection before stop() was called.";
      _logger.severe(message);
      await _stopPromise;
      return Future.error(SignalRException(
          message: message, type: SignalRExceptionType.signalr));
    } else if (_connectionState != ConnectionState.connected) {
      const message =
          "HttpConnection.startInternal completed gracefully but didn't enter the connection into the connected state!";
      _logger.severe(message);
      return Future.error(SignalRException(
          message: message, type: SignalRExceptionType.signalr));
    }

    _connectionStarted = true;
  }

  @override
  Future<void> send(Object? data) {
    if (_connectionState != ConnectionState.connected) {
      return Future.error(SignalRException(
        message:
            "Cannot send data if the connection is not in the 'Connected' State.",
        type: SignalRExceptionType.signalr,
      ));
    }

    final activeTransport = _transport;
    if (activeTransport == null) {
      return Future.error(
        SignalRException(
            message: 'Transport is not available in the Connected state.',
            type: SignalRExceptionType.signalr),
      );
    }
    var queue = _sendQueue;
    if (queue == null) {
      queue = TransportSendQueue(activeTransport);
      _sendQueue = queue;
    }
    return queue.send(data);
  }

  @override
  Future<void>? stop({Object? error}) async {
    final Exception? ex = SignalRException.tryHandler(
        error: error,
        message: error?.toString() ?? '',
        type: SignalRExceptionType.signalr,
        stackTrace: null);

    if (_connectionState == ConnectionState.disconnected) {
      _logger.finer(
        "Call to HttpConnection.stop($error) ignored because the connection is already in the disconnected state.",
      );
      return Future.value();
    }

    if (_connectionState == ConnectionState.disconnecting) {
      _logger.finer(
        "Call to HttpConnection.stop($error) ignored because the connection is already in the disconnecting state.",
      );
      return _stopPromise;
    }

    _connectionState = ConnectionState.disconnecting;

    _stopPromiseCompleter = Completer<void>();
    _stopPromise = _stopPromiseCompleter.future;

    await _stopInternal(error: ex);
    await _stopPromise;
  }

  Future<void> _stopInternal({Object? error}) async {
    _stopError = error;

    try {
      if (_startInternalPromise != null) {
        await _startInternalPromise;
      }
    } catch (_) {}

    final tr = _transport;
    if (tr != null) {
      try {
        await tr.stop();
      } catch (e) {
        _logger.severe("HttpConnection.transport.stop() threw error '$e'.");
        _stopConnection();
      }

      _transport = null;
    } else {
      _logger.finer(
        "HttpConnection.transport is undefined in HttpConnection.stop() because start() failed.",
      );
      _stopConnection();
    }
  }

  Future<void> _startInternal(TransferFormat transferFormat) async {
    final resolvedBase = baseUrl;
    if (resolvedBase == null || resolvedBase.isEmpty) {
      return Future.error(SignalRException(
        message: 'HttpConnection baseUrl is not set or is empty.',
        type: SignalRExceptionType.signalr,
      ));
    }
    var url = resolvedBase;
    _accessTokenFactory = _options.accessTokenFactory;

    try {
      if (_options.skipNegotiation) {
        if (_options.transport == HttpTransportType.webSockets) {
          _transport = _constructTransport(HttpTransportType.webSockets);
          await _startTransport(url, transferFormat);
        } else {
          throw SignalRException(
            message:
                "Negotiation can only be skipped when using the WebSocket transport directly.",
            type: SignalRExceptionType.signalr,
          );
        }
      } else {
        NegotiateResponse negotiateResponse;
        var redirects = 0;

        do {
          negotiateResponse = await _getNegotiationResponse(url);
          if (_connectionState == ConnectionState.disconnecting ||
              _connectionState == ConnectionState.disconnected) {
            throw SignalRException(
                message: "The connection was stopped during negotiation.",
                type: SignalRExceptionType.signalr);
          }

          if (negotiateResponse.isErrorResponse) {
            throw SignalRException(
                message: negotiateResponse.error ?? 'Unknown negotiation error',
                type: SignalRExceptionType.signalr);
          }

          if (negotiateResponse.isRedirectResponse) {
            final nextUrl = negotiateResponse.url;
            if (nextUrl == null || nextUrl.isEmpty) {
              throw SignalRException(
                  message: 'Negotiate redirect response missing url.',
                  type: SignalRExceptionType.signalr);
            }
            url = nextUrl;
          }

          if (negotiateResponse.hasAccessToken) {
            final accessToken = negotiateResponse.accessToken;
            _accessTokenFactory = () => Future<String>.value(accessToken);
          }

          redirects++;
        } while (negotiateResponse.isRedirectResponse &&
            redirects < _options.maxRedirects);

        if (redirects == _options.maxRedirects &&
            negotiateResponse.isRedirectResponse) {
          throw SignalRException(
              message: "Negotiate redirection limit exceeded.",
              type: SignalRExceptionType.signalr);
        }

        await _createTransport(
          url,
          _options.transport,
          negotiateResponse,
          transferFormat,
        );
      }

      if (_transport is LongPollingTransport) {
        final existing = features;
        if (existing == null) {
          features = ConnectionFeatures(true);
        } else {
          existing.inherentKeepAlive = true;
        }
      }

      if (_connectionState == ConnectionState.connecting) {
        _logger.finer("The HttpConnection connected successfully.");
        _connectionState = ConnectionState.connected;
      }
    } catch (e) {
      _logger.severe("Failed to start the connection: ${e.toString()}");
      _connectionState = ConnectionState.disconnected;
      _transport = null;
      return Future.error(e);
    }
  }

  Future<NegotiateResponse> _getNegotiationResponse(String url) async {
    final headers = MessageHeaders();
    headers.addMessageHeaders(_options.headers);

    final tokenFactory = _accessTokenFactory;
    if (tokenFactory != null) {
      final token = await tokenFactory();
      headers.setHeaderValue("Authorization", "Bearer $token");
    }

    final negotiateUrl = _resolveNegotiateUrl(url);
    _logger.finer("Sending negotiation request: $negotiateUrl");
    try {
      final options = SignalRHttpRequest(
        content: "",
        headers: headers,
        timeout: _options.requestTimeout,
      );
      final response = await _httpClient.post(negotiateUrl, options: options);

      if (response.statusCode != 200) {
        return Future.error(SignalRException(
          message:
              "Unexpected status code returned from negotiate ${response.statusCode}",
          type: SignalRExceptionType.http,
          statusCode: response.statusCode,
        ));
      }

      if (response.content is! String) {
        return Future.error(
          SignalRException(
              message: "Negotation response content must be a json.",
              type: SignalRExceptionType.invalidPayload),
        );
      }

      final negotiateResponse =
          NegotiateResponse.fromJson(json.decode(response.content as String));
      final negotiateVersion = negotiateResponse.negotiateVersion;
      if (negotiateVersion == null || negotiateVersion < 1) {
        negotiateResponse.connectionToken = negotiateResponse.connectionId;
      }
      return negotiateResponse;
    } catch (e) {
      _logger.severe(
        "Failed to complete negotiation with the server: ${e.toString()}",
      );
      return Future.error(e);
    }
  }

  String? _createConnectUrl(String? url, String? connectionToken) {
    if (connectionToken == null) {
      return url;
    }

    final base = url;
    if (base == null) {
      throw SignalRException(
        message:
            'Cannot build connect URL with connection token without a base url.',
        type: SignalRExceptionType.signalr,
      );
    }

    final uri = Uri.parse(base);
    return uri.replace(queryParameters: {
      ...uri.queryParameters,
      'id': connectionToken,
    }).toString();
  }

  Future<void> _createTransport(
    String? url,
    Object? requestedTransport,
    NegotiateResponse negotiateResponse,
    TransferFormat requestedTransferFormat,
  ) async {
    var connectUrl = _createConnectUrl(url, negotiateResponse.connectionToken);
    if (_isITransport(requestedTransport)) {
      _logger.finer(
        "Connection was provided an instance of ITransport, using that directly.",
      );
      _transport = requestedTransport as ITransport?;
      await _startTransport(connectUrl, requestedTransferFormat);

      connectionId = negotiateResponse.connectionId;
      return;
    }

    final List<Object> transportExceptions = [];
    final transports = negotiateResponse.availableTransports ?? [];
    NegotiateResponse? negotiate = negotiateResponse;
    for (var endpoint in transports) {
      _connectionState = ConnectionState.connecting;

      try {
        _transport = _resolveTransport(
          endpoint,
          requestedTransport as HttpTransportType?,
          requestedTransferFormat,
        );
      } catch (e) {
        transportExceptions.add("${endpoint.transport} failed: $e");
        continue;
      }

      if (negotiate == null) {
        final negotiateBaseUrl = url;
        if (negotiateBaseUrl == null) {
          return Future.error(SignalRException(
            message: 'Cannot re-negotiate: connection URL is missing.',
            type: SignalRExceptionType.signalr,
          ));
        }
        try {
          negotiate = await _getNegotiationResponse(negotiateBaseUrl);
        } catch (ex) {
          return Future.error(ex);
        }
        connectUrl = _createConnectUrl(url, negotiate.connectionToken);
      }

      try {
        await _startTransport(connectUrl, requestedTransferFormat);
        connectionId = negotiate.connectionId;
        return;
      } catch (ex) {
        _logger.severe(
          "Failed to start the transport '${endpoint.transport}': ${ex.toString()}",
        );
        negotiate = null;
        transportExceptions.add("${endpoint.transport} failed: $ex");

        if (_connectionState != ConnectionState.connecting) {
          const message =
              "Failed to select transport before stop() was called.";
          _logger.finer(message);
          return Future.error(SignalRException(
              message: message, type: SignalRExceptionType.signalr));
        }
      }
    }

    if (transportExceptions.isNotEmpty) {
      return Future.error(SignalRException(
        message:
            "Unable to connect to the server with any of the available transports. ${transportExceptions.join(" ")}",
        type: SignalRExceptionType.signalr,
      ));
    }
    return Future.error(SignalRException(
      message:
          "None of the transports supported by the client are supported by the server.",
      type: SignalRExceptionType.signalr,
    ));
  }

  ITransport _constructTransport(HttpTransportType transport) {
    switch (transport) {
      case HttpTransportType.webSockets:
        return WebSocketTransport(
          _accessTokenFactory,
          _logger,
          _options.logMessageContent,
          _options.headers,
          _options.webSocketChannelFactory,
        );
      case HttpTransportType.serverSentEvents:
        return ServerSentEventsTransport(
          _httpClient,
          _accessTokenFactory,
          _logger,
          _options.logMessageContent,
        );
      case HttpTransportType.longPolling:
        return LongPollingTransport(
          _httpClient,
          _accessTokenFactory,
          _logger,
          _options.logMessageContent,
          pollTimeoutMs: _options.longPollingTimeoutMs,
        );
      default:
        throw SignalRException(
            message: "Unknown transport: $transport.",
            type: SignalRExceptionType.signalr);
    }
  }

  Future<void> _startTransport(String? url, TransferFormat transferFormat) {
    final t = _transport;
    if (t == null) {
      return Future.error(
        SignalRException(
            message: 'Cannot start transport: transport is null.',
            type: SignalRExceptionType.signalr),
      );
    }
    t.onReceive = onreceive;
    t.onClose = _stopConnection;
    return t.connect(url, transferFormat);
  }

  ITransport _resolveTransport(
    AvailableTransport endpoint,
    HttpTransportType? requestedTransport,
    TransferFormat requestedTransferFormat,
  ) {
    final transport = endpoint.transport;
    if (transport == null) {
      _logger.finer(
        "Skipping transport '${endpoint.transport}' because it is not supported by this client.",
      );
      throw SignalRException(
        message:
            "Skipping transport '${endpoint.transport}' because it is not supported by this client.",
        type: SignalRExceptionType.signalr,
      );
    }
    if (transportMatches(requestedTransport, transport)) {
      final transferFormats = endpoint.transferFormats;
      if (transferFormats.contains(requestedTransferFormat)) {
        _logger.finer("Selecting transport '${transport.toString()}'.");
        return _constructTransport(transport);
      }
      _logger.finer(
        "Skipping transport '$transport' because it does not support the requested transfer format '$requestedTransferFormat'.",
      );
      throw SignalRException(
        message:
            "Skipping transport '$transport' because it does not support the requested transfer format '$requestedTransferFormat'.",
        type: SignalRExceptionType.signalr,
      );
    }
    _logger.finer(
      "Skipping transport '$transport' because it was disabled by the client.",
    );
    throw SignalRException(
      message:
          "Skipping transport '$transport' because it was disabled by the client.",
      type: SignalRExceptionType.signalr,
    );
  }

  bool _isITransport(Object? transport) {
    return transport is ITransport;
  }

  void _stopConnection({Object? error}) {
    _logger.finer(
      "HttpConnection.stopConnection(${error ?? "Unknown"}) called while in state $_connectionState.",
    );

    _transport = null;

    error = _stopError ?? error;
    _stopError = null;

    if (_connectionState == ConnectionState.disconnected) {
      _logger.finer(
        "Call to HttpConnection.stopConnection($error) was ignored because the connection is already in the disconnected state.",
      );
      return;
    }

    if (_connectionState == ConnectionState.connecting) {
      _logger.finer(
        "HttpConnection.stopConnection during Connecting; transitioning to Disconnected without throwing.",
      );
      _sendQueue?.stop()?.catchError((Object _) {
        _logger.severe("TransportSendQueue.stop() threw an error.");
      });
      _sendQueue = null;
      connectionId = null;
      _connectionState = ConnectionState.disconnected;
      return;
    }

    if (_connectionState == ConnectionState.disconnecting) {
      if (!_stopPromiseCompleter.isCompleted) {
        _stopPromiseCompleter.complete();
      }
    }

    if (error != null) {
      _logger.severe("Connection disconnected with error '$error'.");
    } else {
      _logger.info("Connection disconnected.");
    }

    _sendQueue?.stop()?.catchError((Object _) {
      _logger.severe("TransportSendQueue.stop() threw an error.");
    });
    _sendQueue = null;

    final client = _httpClient;
    if (client is WebSupportingHttpClient) {
      client.close();
    }

    connectionId = null;
    _connectionState = ConnectionState.disconnected;

    if (_connectionStarted) {
      _connectionStarted = false;

      try {
        onclose?.call(error: error);
      } catch (e) {
        _logger.severe("HttpConnection.onclose($error) threw error '$e'.");
      }
    }
  }

  String _resolveNegotiateUrl(String url) {
    final index = url.indexOf("?");
    var negotiateUrl = url.substring(0, index == -1 ? url.length : index);
    if (negotiateUrl[negotiateUrl.length - 1] != "/") {
      negotiateUrl += "/";
    }
    negotiateUrl += "negotiate";
    negotiateUrl += index == -1 ? "" : url.substring(index);

    if (!negotiateUrl.contains("negotiateVersion")) {
      negotiateUrl += index == -1 ? "?" : "&";
      negotiateUrl += "negotiateVersion=$_negotiateVersion";
    }
    return negotiateUrl;
  }

  static bool transportMatches(
    HttpTransportType? requestedTransport,
    HttpTransportType actualTransport,
  ) {
    return requestedTransport == null || actualTransport == requestedTransport;
  }
}
