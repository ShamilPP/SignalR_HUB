import 'dart:async';

import 'package:http/http.dart';
import 'package:logging/logging.dart';

import '../core/signalr_exception.dart';
import '../protocol/ihub_protocol.dart';
import '../protocol/signalr_http_client.dart';
import '../shared/utils.dart';

typedef OnHttpClientCreateCallback = void Function(Client httpClient);

class WebSupportingHttpClient extends SignalRHttpClient {
  final Logger _logger;
  final OnHttpClientCreateCallback? _httpClientCreateCallback;
  Client? _persistentClient;

  WebSupportingHttpClient(
    this._logger, {
    OnHttpClientCreateCallback? httpClientCreateCallback,
  }) : _httpClientCreateCallback = httpClientCreateCallback;

  Client _getOrCreateClient() {
    if (_persistentClient != null) return _persistentClient!;
    final client = Client();
    _httpClientCreateCallback?.call(client);
    _persistentClient = client;
    return client;
  }

  /// Closes the underlying HTTP client. Call this when the connection is done.
  void close() {
    _persistentClient?.close();
    _persistentClient = null;
  }

  @override
  Future<SignalRHttpResponse> send(SignalRHttpRequest request) {
    final abortEarly = request.abortSignal;
    if (abortEarly != null && abortEarly.aborted) {
      return Future.error(SignalRException(
          message: "An abort occurred.", type: SignalRExceptionType.abort));
    }

    final method = request.method;
    if (method == null || method.isEmpty) {
      return Future.error(ArgumentError('No method defined.'));
    }

    final urlString = request.url;
    if (urlString == null || urlString.isEmpty) {
      return Future.error(ArgumentError('No url defined.'));
    }

    final uri = Uri.parse(urlString);
    final abortSignal = request.abortSignal;

    return Future<SignalRHttpResponse>(() async {
      final httpClient = _getOrCreateClient();

      final abortFuture = Future<void>(() {
        final completer = Completer<void>();
        final sig = abortSignal;
        if (sig != null) {
          sig.onabort = () {
            if (!completer.isCompleted) {
              completer.completeError(SignalRException(
                  message: "An abort occurred.",
                  type: SignalRExceptionType.abort));
            }
          };
        }
        return completer.future;
      });

      final isJson = request.content != null &&
          request.content is String &&
          (request.content as String).startsWith('{');

      final headers = MessageHeaders();
      headers.setHeaderValue('X-Requested-With', 'FlutterHttpClient');
      headers.setHeaderValue(
        'content-type',
        isJson ? 'application/json;charset=UTF-8' : 'text/plain;charset=UTF-8',
      );
      headers.addMessageHeaders(request.headers);

      final contentLen = request.content == null
          ? 0
          : (request.content is String
              ? (request.content as String).length
              : (request.content is List<int>
                  ? (request.content as List<int>).length
                  : httpContentSummary(request.content).length));
      _logger.finest(
        "HTTP send: url '$urlString', method: '$method' "
        "content summary: '${httpContentSummary(request.content)}' content length = $contentLen headers: '$headers'",
      );

      void clearAbortHandler() {
        abortSignal?.onabort = null;
      }

      try {
        final httpResp = await _raceHttpOrAbort(
          _sendHttpRequest(
            httpClient,
            method: method,
            uri: uri,
            body: request.content,
            headers: headers,
            timeoutMs: request.timeout,
          ),
          abortFuture,
        ) as Response;

        clearAbortHandler();

        if (httpResp.statusCode >= 200 && httpResp.statusCode < 300) {
          final contentTypeHeader = httpResp.headers['content-type'];
          final isJsonContent = contentTypeHeader == null ||
              contentTypeHeader.startsWith('application/json');
          if (!isJsonContent && isStringEmpty(uri.queryParameters['id'])) {
            throw ArgumentError(
              'Response Content-Type not supported: $contentTypeHeader',
            );
          }

          return SignalRHttpResponse(
            httpResp.statusCode,
            statusText: httpResp.reasonPhrase,
            content: httpResp.body,
          );
        }
        throw SignalRException(
          message: "${httpResp.statusCode}: ${httpResp.reasonPhrase}",
          type: SignalRExceptionType.http,
          statusCode: httpResp.statusCode,
        );
      } catch (e, st) {
        clearAbortHandler();
        throw SignalRException.handler(
            error: e,
            message: e.toString(),
            type: SignalRExceptionType.signalr,
            stackTrace: st);
      }
    });
  }

  Future<dynamic> _raceHttpOrAbort(
    Future<Response> httpFuture,
    Future<void> abortFuture,
  ) {
    return Future.any<dynamic>([httpFuture, abortFuture]);
  }

  Future<Response> _sendHttpRequest(
    Client httpClient, {
    required String method,
    required Uri uri,
    required MessageHeaders headers,
    Object? body,
    int? timeoutMs,
  }) {
    Future<Response> httpResponse;

    switch (method.toLowerCase()) {
      case 'post':
        httpResponse = httpClient.post(
          uri,
          body: body,
          headers: headers.asMap,
        );
        break;
      case 'put':
        httpResponse = httpClient.put(
          uri,
          body: body,
          headers: headers.asMap,
        );
        break;
      case 'delete':
        httpResponse = httpClient.delete(
          uri,
          body: body,
          headers: headers.asMap,
        );
        break;
      case 'get':
      default:
        httpResponse = httpClient.get(uri, headers: headers.asMap);
    }

    if (timeoutMs != null && timeoutMs > 0) {
      httpResponse = httpResponse.timeout(
        Duration(milliseconds: timeoutMs),
        onTimeout: () => throw SignalRException(
            message: "A timeout occurred.", type: SignalRExceptionType.timeout),
      );
    }

    return httpResponse;
  }
}
