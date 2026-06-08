// ignore_for_file: close_sinks
import 'dart:io';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

Future<WebSocketChannel> connectWebSocket(
    Uri uri, Map<String, String> headers) async {
  final webSocket = await WebSocket.connect(uri.toString(), headers: headers);
  return IOWebSocketChannel(webSocket);
}
