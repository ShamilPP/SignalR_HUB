import 'dart:async';
import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:logging/logging.dart';
import 'package:signalr_hub/signalr_client.dart';
import 'package:signalr_hub/src/connection/transport_send_queue.dart';

/// A fake IConnection for testing HubConnection without real networking.
class FakeConnection extends IConnection {
  final Completer<void> _startCompleter = Completer<void>();
  bool startCalled = false;
  bool stopCalled = false;
  Exception? stopError;
  final List<Object?> sentMessages = [];

  @override
  Future<void> start({TransferFormat? transferFormat}) {
    startCalled = true;
    return _startCompleter.future;
  }

  void completeStart() => _startCompleter.complete();
  void failStart(Exception e) => _startCompleter.completeError(e);

  @override
  Future<void> send(Object? data) {
    sentMessages.add(data);
    return Future.value();
  }

  @override
  Future<void>? stop({Object? error}) {
    stopCalled = true;
    if (error is Exception) stopError = error;
    // Simulate transport closing by calling onclose
    onclose?.call(error: error is Exception ? error : null);
    return Future.value();
  }

  /// Simulate receiving data from the server.
  void receive(Object data) {
    onreceive?.call(data);
  }
}

/// Builds a handshake response (JSON text ending with record separator 0x1E).
String handshakeResponse({String? error}) {
  if (error != null) {
    return '{"error":"$error"}\u001e';
  }
  return '{}\u001e';
}

void main() {
  late FakeConnection fakeConnection;
  late Logger logger;

  setUp(() {
    fakeConnection = FakeConnection();
    logger = Logger.detached('test')..level = Level.OFF;
  });

  HubConnection createHub({IRetryPolicy? reconnectPolicy}) {
    return HubConnection.create(
      fakeConnection,
      logger,
      JsonHubProtocol(),
      reconnectPolicy: reconnectPolicy,
    );
  }

  group('HubConnection lifecycle', () {
    test('starts in Disconnected state', () {
      final hub = createHub();
      expect(hub.state, HubConnectionState.disconnected);
    });

    test('start() transitions to Connected on successful handshake', () async {
      final hub = createHub();

      // Start the hub — completeStart + handshake inline
      final startFuture = hub.start();
      fakeConnection.completeStart();

      // Let microtasks run so the hub sends the handshake and waits
      await Future.delayed(Duration.zero);
      fakeConnection.receive(handshakeResponse());

      await startFuture;
      expect(hub.state, HubConnectionState.connected);
    });

    test('stop() on already disconnected is a no-op', () async {
      final hub = createHub();
      // Should not throw
      await hub.stop();
      expect(hub.state, HubConnectionState.disconnected);
    });
  });

  /// Decodes the last message handed to the transport as a completion frame.
  ///
  /// Asserts along the way that it really was serialized by the hub protocol
  /// (a String terminated by the record separator) rather than passed through
  /// as a raw message object, which no transport accepts.
  Map<String, dynamic> lastCompletion() {
    final sent = fakeConnection.sentMessages.last;
    expect(sent, isA<String>(),
        reason: 'transports only accept String or Uint8List');
    final text = sent as String;
    expect(text, endsWith('\u001e'));
    final decoded =
        jsonDecode(text.substring(0, text.length - 1)) as Map<String, dynamic>;
    expect(decoded['type'], MessageType.completion.index);
    return decoded;
  }

  Future<HubConnection> startConnectedHub() async {
    final hub = createHub();
    final startFuture = hub.start();
    fakeConnection.completeStart();
    await Future.delayed(Duration.zero);
    fakeConnection.receive(handshakeResponse());
    await startFuture;
    return hub;
  }

  group('HubConnection.on / off', () {
    test('registers and invokes method handler', () async {
      final hub = await startConnectedHub();

      final received = <List<Object?>?>[];
      hub.on('TestMethod', (args) => received.add(args));

      // Simulate server sending an invocation
      fakeConnection.receive(
          '{"type":1,"target":"TestMethod","arguments":["hello",42]}\u001e');

      expect(received.length, 1);
      expect(received.first, ['hello', 42]);
    });

    test('sends a completion message when a client method returns a result',
        () async {
      final hub = await startConnectedHub();

      hub.on('TestMethod', (args) async {
        await Future.delayed(Duration.zero);
        return 'client result';
      });

      fakeConnection.receive(
          '{"type":1,"target":"TestMethod","invocationId":"42","arguments":[]}\u001e');

      await Future.delayed(Duration(milliseconds: 10));

      final completion = lastCompletion();
      expect(completion['invocationId'], '42');
      expect(completion['result'], 'client result');
      expect(completion.containsKey('error'), isFalse);
    });

    test('client result completion is serialized by the hub protocol',
        () async {
      // Regression: the completion used to be handed to the transport as a
      // raw CompletionMessage object, which every real transport rejects
      // with "Content type is not handled."
      final hub = await startConnectedHub();

      hub.on('TestMethod', (args) => 'ok');

      fakeConnection.receive(
          '{"type":1,"target":"TestMethod","invocationId":"7","arguments":[]}\u001e');
      await Future.delayed(Duration(milliseconds: 10));

      final sent = fakeConnection.sentMessages.last;
      expect(sent, isA<String>(),
          reason: 'transports only accept String or Uint8List');
      expect(sent as String, endsWith('\u001e'));
    });

    test('sends an error completion when a client method throws', () async {
      final hub = await startConnectedHub();

      hub.on('TestMethod', (args) => throw StateError('handler exploded'));

      fakeConnection.receive(
          '{"type":1,"target":"TestMethod","invocationId":"8","arguments":[]}\u001e');
      await Future.delayed(Duration(milliseconds: 10));

      final completion = lastCompletion();
      expect(completion['invocationId'], '8');
      expect(completion['error'], contains('handler exploded'));
      expect(completion.containsKey('result'), isFalse);
    });

    test('sends an error completion when a client method throws async',
        () async {
      final hub = await startConnectedHub();

      hub.on('TestMethod', (args) async {
        await Future.delayed(Duration.zero);
        throw StateError('async explosion');
      });

      fakeConnection.receive(
          '{"type":1,"target":"TestMethod","invocationId":"9","arguments":[]}\u001e');
      await Future.delayed(Duration(milliseconds: 10));

      expect(lastCompletion()['error'], contains('async explosion'));
    });

    test('sends an error completion when no client method is registered',
        () async {
      await startConnectedHub();

      fakeConnection.receive(
          '{"type":1,"target":"Missing","invocationId":"10","arguments":[]}\u001e');
      await Future.delayed(Duration(milliseconds: 10));

      final completion = lastCompletion();
      expect(completion['invocationId'], '10');
      expect(completion['error'], contains("'Missing' not found"));
    });

    test('sends a void completion when the handler returns nothing', () async {
      final hub = await startConnectedHub();

      var called = false;
      hub.on('TestMethod', (args) {
        called = true;
      });

      fakeConnection.receive(
          '{"type":1,"target":"TestMethod","invocationId":"11","arguments":[]}\u001e');
      await Future.delayed(Duration(milliseconds: 10));

      expect(called, isTrue);
      final completion = lastCompletion();
      expect(completion['invocationId'], '11');
      expect(completion.containsKey('result'), isFalse);
      expect(completion.containsKey('error'), isFalse);
    });

    test('with multiple handlers, the first result is sent to the server',
        () async {
      final hub = await startConnectedHub();

      final order = <String>[];
      hub.on('TestMethod', (args) {
        order.add('first');
        return 'first result';
      });
      hub.on('TestMethod', (args) {
        order.add('second');
        return 'second result';
      });

      fakeConnection.receive(
          '{"type":1,"target":"TestMethod","invocationId":"12","arguments":[]}\u001e');
      await Future.delayed(Duration(milliseconds: 10));

      // Every handler still runs; only the first one answers the server.
      expect(order, ['first', 'second']);
      expect(lastCompletion()['result'], 'first result');
    });

    test('does not send a completion when the server expects no response',
        () async {
      final hub = await startConnectedHub();

      hub.on('TestMethod', (args) => 'ignored result');

      // Only the handshake request has been sent so far.
      final sentBefore = fakeConnection.sentMessages.length;

      fakeConnection
          .receive('{"type":1,"target":"TestMethod","arguments":[]}\u001e');
      await Future.delayed(Duration(milliseconds: 10));

      // No invocationId means the server is not waiting on a result, so
      // the handler's return value is discarded and nothing is sent back.
      expect(fakeConnection.sentMessages.length, sentBefore);
    });

    test('a handler may unregister itself while being invoked', () async {
      final hub = await startConnectedHub();

      late MethodInvocationFunc handler;
      handler = (args) {
        hub.off('TestMethod', method: handler);
        return 'done';
      };
      hub.on('TestMethod', handler);

      fakeConnection.receive(
          '{"type":1,"target":"TestMethod","invocationId":"13","arguments":[]}\u001e');
      await Future.delayed(Duration(milliseconds: 10));

      expect(lastCompletion()['result'], 'done');
    });

    test('method name is case-insensitive', () async {
      final hub = await startConnectedHub();

      var called = false;
      hub.on('MyMethod', (_) => called = true);

      fakeConnection
          .receive('{"type":1,"target":"mymethod","arguments":[]}\u001e');
      expect(called, isTrue);
    });

    test('off removes handler', () async {
      final hub = await startConnectedHub();

      var callCount = 0;
      void handler(List<Object?>? args) => callCount++;

      hub.on('Foo', handler);
      fakeConnection.receive('{"type":1,"target":"Foo","arguments":[]}\u001e');
      expect(callCount, 1);

      hub.off('Foo', method: handler);
      fakeConnection.receive('{"type":1,"target":"Foo","arguments":[]}\u001e');
      expect(callCount, 1); // Not called again
    });
  });

  group('DefaultRetryPolicy with jitter', () {
    test('returns null after all retries exhausted', () {
      final policy = DefaultRetryPolicy();
      // 5th attempt (index 4) is null → stop
      final result = policy.nextRetryDelayInMilliseconds(
        RetryContext(60000, 4, Exception('test')),
      );
      expect(result, isNull);
    });

    test('first retry is immediate (0ms)', () {
      final policy = DefaultRetryPolicy();
      final result = policy.nextRetryDelayInMilliseconds(
        RetryContext(0, 0, Exception('test')),
      );
      expect(result, 0);
    });

    test('jitter adds randomness to non-zero delays', () {
      final policy = DefaultRetryPolicy(jitterFactor: 0.2);
      // Second retry (index 1) has base delay of 2000ms
      final results = <int>{};
      for (var i = 0; i < 20; i++) {
        final r = policy.nextRetryDelayInMilliseconds(
          RetryContext(0, 1, Exception('test')),
        );
        results.add(r!);
      }
      // All results should be >= 2000 and <= 2400 (20% jitter)
      for (final r in results) {
        expect(r, greaterThanOrEqualTo(2000));
        expect(r, lessThanOrEqualTo(2400));
      }
    });

    test('custom retry delays are respected', () {
      final policy =
          DefaultRetryPolicy(retryDelays: [100, 500], jitterFactor: 0.0);
      expect(
        policy.nextRetryDelayInMilliseconds(RetryContext(0, 0, Exception())),
        100,
      );
      expect(
        policy.nextRetryDelayInMilliseconds(RetryContext(0, 1, Exception())),
        500,
      );
      // After custom list + null sentinel
      expect(
        policy.nextRetryDelayInMilliseconds(RetryContext(0, 2, Exception())),
        isNull,
      );
    });
  });

  group('TransportSendQueue buffer limit', () {
    test('throws when buffer exceeds maxBufferSize', () async {
      final fakeTransport = _CompletingFakeTransport();
      final queue = TransportSendQueue(fakeTransport, maxBufferSize: 3);

      // Ignore errors from pending sends when queue is stopped
      queue.send('a').catchError((_) {});
      queue.send('b').catchError((_) {});
      queue.send('c').catchError((_) {});

      // 4th exceeds maxBufferSize of 3
      expect(
        () => queue.send('d'),
        throwsA(isA<SignalRException>()),
      );

      await queue.stop();
    });
  });

  group('URL sanitization', () {
    test('sanitizeUrlForLogging masks access_token', () {
      final url = 'wss://example.com/hub?id=123&access_token=secret123';
      final sanitized = sanitizeUrlForLogging(url);
      expect(sanitized, contains('access_token=%2A%2A%2A'));
      expect(sanitized, isNot(contains('secret123')));
    });

    test('sanitizeUrlForLogging leaves URLs without token unchanged', () {
      final url = 'wss://example.com/hub?id=123';
      final sanitized = sanitizeUrlForLogging(url);
      expect(sanitized, contains('id=123'));
    });
  });
}

/// A transport that completes sends instantly (for testing buffer limits).
class _CompletingFakeTransport implements ITransport {
  @override
  OnClose? onClose;
  @override
  OnReceive? onReceive;

  @override
  Future<void> connect(String? url, TransferFormat transferFormat) =>
      Future.value();

  @override
  Future<void> send(Object data) => Future.value();

  @override
  Future<void> stop() => Future.value();
}
