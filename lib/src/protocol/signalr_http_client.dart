import 'dart:async';

import '../core/abort_controller.dart';
import 'ihub_protocol.dart';

/// Represents an HTTP request.
class SignalRHttpRequest {
  String? method;
  String? url;
  Object? content;
  MessageHeaders? headers;
  IAbortSignal? abortSignal;
  int? timeout;

  SignalRHttpRequest({
    this.method,
    this.url,
    this.content,
    this.headers,
    this.abortSignal,
    this.timeout,
  });
}

/// Represents an HTTP response.
class SignalRHttpResponse {
  final int statusCode;
  final String? statusText;
  final Object? content;

  SignalRHttpResponse(
    this.statusCode, {
    this.statusText = '',
    this.content,
  });
}

/// Abstraction over an HTTP client.
abstract class SignalRHttpClient {
  Future<SignalRHttpResponse> get(String url,
      {required SignalRHttpRequest options}) {
    options.method = 'GET';
    options.url = url;
    return send(options);
  }

  Future<SignalRHttpResponse> post(String? url,
      {required SignalRHttpRequest options}) {
    options.method = 'POST';
    options.url = url;
    return send(options);
  }

  Future<SignalRHttpResponse> delete(String? url,
      {required SignalRHttpRequest options}) {
    options.method = 'DELETE';
    options.url = url;
    return send(options);
  }

  Future<SignalRHttpResponse> send(SignalRHttpRequest request);
}
