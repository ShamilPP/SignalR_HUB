import 'package:web_socket_channel/web_socket_channel.dart';

Future<WebSocketChannel> connectWebSocket(
    Uri uri, Map<String, String> headers) async {
  // Web sockets do not support custom headers
  return WebSocketChannel.connect(uri);
}
