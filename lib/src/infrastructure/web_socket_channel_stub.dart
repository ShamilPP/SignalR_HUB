import 'package:web_socket_channel/web_socket_channel.dart';

Future<WebSocketChannel> connectWebSocket(
    Uri uri, Map<String, String> headers) {
  throw UnsupportedError('Cannot connect without dart:html or dart:io');
}
