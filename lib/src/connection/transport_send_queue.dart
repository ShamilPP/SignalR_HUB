import 'dart:async';
import 'dart:typed_data';

import '../core/signalr_exception.dart';
import '../core/itransport.dart';

class TransportSendQueue {
  /// Default maximum number of messages that can be buffered before rejection.
  static const int defaultMaxBufferSize = 10000;

  final List<Object?> _buffer = [];
  late Completer<void> _sendBufferedData;
  bool _executing = true;
  Completer<void>? _transportResult;
  Future<void>? _sendLoopPromise;
  final ITransport transport;
  final int maxBufferSize;

  TransportSendQueue(this.transport,
      {this.maxBufferSize = defaultMaxBufferSize}) {
    _sendBufferedData = Completer<void>();
    _transportResult = Completer<void>();
    _sendLoopPromise = _sendLoop();
  }

  Future<void> send(Object? data) {
    _bufferData(data);
    var result = _transportResult;
    if (result == null) {
      result = Completer<void>();
      _transportResult = result;
    }
    return result.future;
  }

  Future<void>? stop() {
    _executing = false;
    if (!_sendBufferedData.isCompleted) _sendBufferedData.complete();
    return _sendLoopPromise;
  }

  void _bufferData(Object? data) {
    if (data is Uint8List && _buffer.isNotEmpty && _buffer[0] is! Uint8List) {
      throw SignalRException(
        message:
            "Expected data to be of type ${_buffer[0].runtimeType} but got Uint8List",
        type: SignalRExceptionType.signalr,
      );
    } else if (data is String && _buffer.isNotEmpty && _buffer[0] is! String) {
      throw SignalRException(
        message:
            "Expected data to be of type ${_buffer[0].runtimeType} but got String",
        type: SignalRExceptionType.signalr,
      );
    }

    if (_buffer.length >= maxBufferSize) {
      throw SignalRException(
          message:
              'Send buffer is full ($maxBufferSize messages). The transport may be too slow or disconnected.',
          type: SignalRExceptionType.signalr);
    }

    _buffer.add(data);
    if (!_sendBufferedData.isCompleted) _sendBufferedData.complete();
  }

  Future<void> _sendLoop() async {
    while (true) {
      await _sendBufferedData.future;

      if (!_executing) {
        final pending = _transportResult;
        if (pending != null && !pending.isCompleted) {
          pending.completeError(SignalRException(
              message: 'Connection stopped.',
              type: SignalRExceptionType.signalr));
        }
        break;
      }

      _sendBufferedData = Completer<void>();

      final transportResult = _transportResult;
      if (transportResult == null) break;
      _transportResult = null;

      if (_buffer.isEmpty) {
        if (!transportResult.isCompleted) {
          transportResult.complete();
        }
        continue;
      }

      final Object data;
      if (_buffer[0] is String) {
        final sb = StringBuffer();
        for (final item in _buffer) {
          sb.write(item);
        }
        data = sb.toString();
      } else {
        data = TransportSendQueue.concatBuffers(
          List<Uint8List?>.from(_buffer),
        );
      }

      _buffer.clear();

      try {
        await transport.send(data);
        if (!transportResult.isCompleted) transportResult.complete();
      } catch (error, st) {
        if (!transportResult.isCompleted) {
          transportResult.completeError(SignalRException.handler(
              error: error,
              message: error.toString(),
              type: SignalRExceptionType.signalr,
              stackTrace: st));
        }
      }
    }
  }

  static Uint8List concatBuffers(List<Uint8List?> arrayBuffers) {
    final builder = BytesBuilder(copy: false);
    for (final b in arrayBuffers) {
      if (b == null) {
        throw SignalRException(
            message: 'concatBuffers: null buffer segment',
            type: SignalRExceptionType.signalr);
      }
      builder.add(b);
    }
    return builder.toBytes();
  }
}
