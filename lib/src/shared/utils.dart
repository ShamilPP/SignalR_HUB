import 'dart:typed_data';

import 'package:logging/logging.dart';

import '../core/itransport.dart';
import '../protocol/ihub_protocol.dart';
import '../protocol/signalr_http_client.dart';

bool isIntEmpty(int? value) {
  return value == null;
}

bool isStringEmpty(String? value) {
  return value == null || value.isEmpty;
}

bool isListEmpty(List? value) {
  return value == null || value.isEmpty;
}

/// Appends a cache-buster using `?` or `&` depending on whether [url] already has a query string.
///
/// Returns [url] unchanged if it is empty (caller should reject invalid URLs earlier).
String urlWithCacheBuster(String url) {
  if (url.isEmpty) return url;
  final sep = url.contains('?') ? '&' : '?';
  return '$url${sep}_=${DateTime.now()}';
}

/// Normalizes an http(s) or ws(s) URL for a WebSocket handshake.
///
/// - Uses **ws** / **wss** schemes (RFC 6455).
/// - **Drops the fragment** (`#...`); fragments are not sent on WebSocket
///   connections and some stacks mishandle them.
/// - Omits **port** in the string when it is the default for the scheme, or
///   when it would be invalid (e.g. `0`), avoiding bogus URLs like
///   `https://host:0/path#` seen when naive string rewrites interact with
///   [Uri] parsing in the VM.
String normalizeWebSocketConnectUrl(String urlString) {
  final u = Uri.parse(urlString);
  if (u.host.isEmpty) {
    throw ArgumentError.value(
      urlString,
      'urlString',
      'URL must include a host (e.g. https://server/hub).',
    );
  }
  final scheme = u.scheme.toLowerCase();
  final String wsScheme;
  if (scheme == 'https' || scheme == 'wss') {
    wsScheme = 'wss';
  } else if (scheme == 'http' || scheme == 'ws') {
    wsScheme = 'ws';
  } else {
    throw ArgumentError.value(
      urlString,
      'urlString',
      'Expected http, https, ws, or wss scheme.',
    );
  }
  var p = u.port;
  if (u.hasPort && p == 0) {
    throw ArgumentError.value(
      urlString,
      'urlString',
      'URL port must be between 1 and 65535.',
    );
  }
  // When the port is omitted, [Uri.port] can be 0 before applying defaults.
  if (p == 0) {
    p = wsScheme == 'wss' ? 443 : 80;
  }
  if (p <= 0 || p > 65535) {
    throw ArgumentError.value(
      urlString,
      'urlString',
      'URL port must be between 1 and 65535.',
    );
  }
  final defaultForWs = wsScheme == 'wss' ? 443 : 80;
  final explicitPort = p == defaultForWs ? null : p;
  return Uri(
    scheme: wsScheme,
    userInfo: u.userInfo.isEmpty ? null : u.userInfo,
    host: u.host,
    port: explicitPort,
    path: u.path,
    query: u.hasQuery ? u.query : null,
  ).toString();
}

/// Removes sensitive query parameters (access_token) from a URL for safe logging.
String sanitizeUrlForLogging(String url) {
  try {
    final uri = Uri.parse(url);
    if (!uri.hasQuery) return url;
    final sanitized = Map<String, String>.from(uri.queryParameters);
    if (sanitized.containsKey('access_token')) {
      sanitized['access_token'] = '***';
    }
    return uri.replace(queryParameters: sanitized).toString();
  } catch (_) {
    return url;
  }
}

String getDataDetail(Object? data, bool includeContent) {
  var detail = "";
  if (data is Uint8List) {
    detail = "Binary data of length ${data.lengthInBytes}";
    if (includeContent) {
      detail += ". Content: '${formatArrayBuffer(data)}'";
    }
  } else if (data is String) {
    detail = "String data of length ${data.length}";
    if (includeContent) {
      detail += ". Content: '$data'";
    }
  } else if (data != null) {
    detail = "Data of type ${data.runtimeType}";
  }
  return detail;
}

/// Safe summary for HTTP logging (handles String and Uint8List).
String httpContentSummary(Object? content) {
  return getDataDetail(content, false);
}

String formatArrayBuffer(Uint8List data) {
  var str = "";
  for (final val in data) {
    final pad = val < 16 ? "0" : "";
    str += "0x$pad${val.toString()} ";
  }
  if (str.isEmpty) return "";
  return str.substring(0, str.length - 1);
}

Future<void> sendMessage(
  Logger logger,
  String transportName,
  SignalRHttpClient httpClient,
  String? url,
  AccessTokenFactory? accessTokenFactory,
  Object content,
  bool logMessageContent,
) async {
  final headers = MessageHeaders();
  final tokenFactory = accessTokenFactory;
  if (tokenFactory != null) {
    final token = await tokenFactory();
    if (!isStringEmpty(token)) {
      headers.setHeaderValue("Authorization", "Bearer $token");
    }
  }

  logger.finest("($transportName transport) sending data.");

  final req = SignalRHttpRequest(content: content, headers: headers);
  final response = await httpClient.post(url, options: req);

  logger.finest(
    "($transportName transport) request complete. Response status: ${response.statusCode}.",
  );
}
