import 'dart:async';

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
