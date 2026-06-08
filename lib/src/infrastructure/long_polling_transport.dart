import 'dart:async';

import 'package:logging/logging.dart';

import '../core/abort_controller.dart';
import '../core/signalr_exception.dart';
import '../core/itransport.dart';
import '../protocol/ihub_protocol.dart';
import '../protocol/signalr_http_client.dart';
import '../shared/utils.dart';

class LongPollingTransport implements ITransport {
  final SignalRHttpClient _httpClient;
  final AccessTokenFactory? _accessTokenFactory;
  final Logger _logger;
  final bool _logMessageContent;
  final AbortController _pollAbort;
  final int pollTimeoutMs;

  bool get pollAborted => _pollAbort.aborted;

  String? _url;
  late bool _running;
  Future<void>? _receiving;
  Exception? _closeError;
  bool _onCloseRaised = false;

  @override
  OnClose? onClose;

  @override
  OnReceive? onReceive;

  LongPollingTransport(
    SignalRHttpClient httpClient,
    AccessTokenFactory? accessTokenFactory,
    Logger logger,
    bool logMessageContent, {
    this.pollTimeoutMs = 100000,
  })  : _httpClient = httpClient,
        _accessTokenFactory = accessTokenFactory,
        _logger = logger,
        _logMessageContent = logMessageContent,
        _pollAbort = AbortController() {
    _running = false;
  }

  @override
  Future<void> connect(String? url, TransferFormat transferFormat) async {
    if (url == null || isStringEmpty(url)) {
      return Future.error(
        SignalRException(
            message: 'Long polling requires a non-empty url.',
            type: SignalRExceptionType.signalr),
      );
    }

    _url = url;

    _logger.finest("(LongPolling transport) Connecting");

    if (transferFormat == TransferFormat.binary) {
      throw SignalRException(
        message:
            "Binary protocols via Long Polling Transport is not supported.",
        type: SignalRExceptionType.signalr,
      );
    }

    final pollOptions = SignalRHttpRequest(
      abortSignal: _pollAbort.signal,
      headers: MessageHeaders(),
      timeout: pollTimeoutMs,
    );

    final token = await _getAccessToken();
    _updateHeaderToken(pollOptions, token);

    final pollUrl = urlWithCacheBuster(url);
    _logger.finest("(LongPolling transport) polling: $pollUrl");
    final response = await _httpClient.get(pollUrl, options: pollOptions);
    if (response.statusCode != 200) {
      _logger.severe(
        "(LongPolling transport) Unexpected response code: ${response.statusCode}",
      );

      _closeError = SignalRException(
          message: "${response.statusCode}: ${response.statusText ?? ""}",
          type: SignalRExceptionType.http,
          statusCode: response.statusCode);
      _running = false;
    } else {
      _running = true;
    }

    _receiving = poll(_url, pollOptions);
  }

  Future<void> poll(String? url, SignalRHttpRequest pollOptions) async {
    final baseUrl = url;
    if (baseUrl == null || baseUrl.isEmpty) {
      _logger.warning(
        '(LongPolling transport) poll called with empty url; stopping.',
      );
      _running = false;
      return;
    }

    try {
      while (_running) {
        final token = await _getAccessToken();
        _updateHeaderToken(pollOptions, token);

        try {
          final pollUrl = urlWithCacheBuster(baseUrl);
          _logger.finest("(LongPolling transport) polling: $pollUrl");
          final response = await _httpClient.get(pollUrl, options: pollOptions);

          if (response.statusCode == 204) {
            _logger.info("(LongPolling transport) Poll terminated by server");

            _running = false;
          } else if (response.statusCode != 200) {
            _logger.severe(
              "(LongPolling transport) Unexpected response code: ${response.statusCode}",
            );

            _closeError = SignalRException(
                message: "${response.statusCode}: ${response.statusText ?? ""}",
                type: SignalRExceptionType.http,
                statusCode: response.statusCode);
            _running = false;
          } else {
            final content = response.content;
            final hasText = content is String && !isStringEmpty(content);
            if (hasText) {
              _logger.finest("(LongPolling transport) data received");
              onReceive?.call(content);
            } else {
              _logger.finest(
                "(LongPolling transport) Poll timed out, reissuing.",
              );
            }
          }
        } catch (e, st) {
          if (!_running) {
            _logger.finest(
              "(LongPolling transport) Poll errored after shutdown: ${e.toString()}",
            );
          } else {
            if (e is SignalRException && e.type.isTimeout) {
              _logger.finest(
                "(LongPolling transport) Poll timed out, reissuing.",
              );
            } else {
              _closeError = SignalRException.handler(
                  error: e,
                  message: e.toString(),
                  type: SignalRExceptionType.signalr,
                  stackTrace: st);
              _running = false;
            }
          }
        }
      }
    } finally {
      _logger.finest("(LongPolling transport) Polling complete.");

      if (!pollAborted) {
        _raiseOnClose();
      }
    }
  }

  @override
  Future<void> send(Object data) async {
    if (!_running) {
      return Future.error(
        SignalRException(
            message: "Cannot send until the transport is connected",
            type: SignalRExceptionType.signalr),
      );
    }
    await sendMessage(
      _logger,
      "LongPolling",
      _httpClient,
      _url,
      _accessTokenFactory,
      data,
      _logMessageContent,
    );
  }

  @override
  Future<void> stop() async {
    _logger.finest("(LongPolling transport) Stopping polling.");

    _running = false;
    _pollAbort.abort();

    try {
      await _receiving;

      _logger
          .finest("(LongPolling transport) sending DELETE request to $_url.");

      final deleteOptions = SignalRHttpRequest();
      final token = await _getAccessToken();
      _updateHeaderToken(deleteOptions, token);
      await _httpClient.delete(_url, options: deleteOptions);

      _logger.finest("(LongPolling transport) DELETE request sent.");
    } finally {
      _logger.finest("(LongPolling transport) Stop finished.");
      _raiseOnClose();
    }
  }

  Future<String?> _getAccessToken() async {
    final factory = _accessTokenFactory;
    if (factory != null) {
      return factory();
    }
    return null;
  }

  void _updateHeaderToken(SignalRHttpRequest request, String? token) {
    final headers = request.headers ?? MessageHeaders();
    request.headers = headers;

    if (!isStringEmpty(token)) {
      headers.setHeaderValue("Authorization", "Bearer $token");
      return;
    }
    headers.removeHeader("Authorization");
  }

  void _raiseOnClose() {
    if (_onCloseRaised) return;
    final closeHandler = onClose;
    if (closeHandler == null) return;
    _onCloseRaised = true;

    var logMessage = "(LongPolling transport) Firing onclose event.";
    if (_closeError != null) {
      logMessage += " Error: $_closeError";
    }
    _logger.finest(logMessage);

    closeHandler(error: _closeError);
  }
}
