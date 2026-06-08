import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:logging/logging.dart';
import 'package:signalr_hub/signalr_client.dart';

void main() {
  group('TextMessageFormat.parse', () {
    test('empty input throws (no RangeError)', () {
      expect(
        () => TextMessageFormat.parse(''),
        throwsA(isA<Exception>()),
      );
    });

    test('incomplete message without record separator throws', () {
      expect(
        () => TextMessageFormat.parse('{"type":1}'),
        throwsA(isA<Exception>()),
      );
    });

    test('single complete frame parses', () {
      final written = TextMessageFormat.write('{"type":6}');
      final messages = TextMessageFormat.parse(written);
      expect(messages, ['{"type":6}']);
    });
  });

  group('DefaultRetryPolicy.nextRetryDelayInMilliseconds', () {
    test('returns null when previousRetryCount is out of range (high)', () {
      final policy = DefaultRetryPolicy();
      final ctx = RetryContext(0, 99,
          SignalRException(message: 'x', type: SignalRExceptionType.signalr));
      expect(policy.nextRetryDelayInMilliseconds(ctx), isNull);
    });

    test('returns null when previousRetryCount is negative', () {
      final policy = DefaultRetryPolicy();
      final ctx = RetryContext(0, -1,
          SignalRException(message: 'x', type: SignalRExceptionType.signalr));
      expect(policy.nextRetryDelayInMilliseconds(ctx), isNull);
    });

    test('custom delays: stops at appended null sentinel', () {
      final policy =
          DefaultRetryPolicy(retryDelays: [100, 200], jitterFactor: 0.0);
      expect(
          policy.nextRetryDelayInMilliseconds(
            RetryContext(
                0,
                0,
                SignalRException(
                    message: 'a', type: SignalRExceptionType.signalr)),
          ),
          100);
      expect(
          policy.nextRetryDelayInMilliseconds(
            RetryContext(
                0,
                1,
                SignalRException(
                    message: 'a', type: SignalRExceptionType.signalr)),
          ),
          200);
      expect(
          policy.nextRetryDelayInMilliseconds(
            RetryContext(
                0,
                2,
                SignalRException(
                    message: 'a', type: SignalRExceptionType.signalr)),
          ),
          isNull);
      expect(
          policy.nextRetryDelayInMilliseconds(
            RetryContext(
                0,
                3,
                SignalRException(
                    message: 'a', type: SignalRExceptionType.signalr)),
          ),
          isNull);
    });
  });

  group('parseMessageTypeFromString', () {
    test('unknown numeric type includes value in error', () {
      expect(
        () => parseMessageTypeFromString(99),
        throwsA(
          predicate<SignalRException>(
            (e) => e.toString().contains('99'),
          ),
        ),
      );
    });
  });

  group('normalizeWebSocketConnectUrl', () {
    test('https hub path becomes wss without :0 or fragment', () {
      expect(
        normalizeWebSocketConnectUrl(
          'https://www.example.com/Channels/Support',
        ),
        'wss://www.example.com/Channels/Support',
      );
    });

    test('strips fragment (not used in WebSocket handshake)', () {
      expect(
        normalizeWebSocketConnectUrl('https://host.example/hub#section'),
        'wss://host.example/hub',
      );
    });

    test('preserves explicit non-default port', () {
      expect(
        normalizeWebSocketConnectUrl('https://host.example:8443/hub'),
        'wss://host.example:8443/hub',
      );
    });

    test('ws input is normalized to ws and drops fragment', () {
      expect(
        normalizeWebSocketConnectUrl('ws://host.example/path#x'),
        'ws://host.example/path',
      );
    });

    test('rejects invalid port', () {
      expect(
        () => normalizeWebSocketConnectUrl('https://host:0/p'),
        throwsArgumentError,
      );
    });
  });

  group('urlWithCacheBuster', () {
    test('uses ? when url has no query string', () {
      const base = 'https://example.com/hub';
      final u = urlWithCacheBuster(base);
      expect(u.startsWith('$base?'), isTrue);
      expect(u.contains('_='), isTrue);
    });

    test('uses & when url already has query', () {
      const base = 'https://example.com/hub?id=abc';
      final u = urlWithCacheBuster(base);
      expect(u.startsWith('$base&'), isTrue);
      expect(u.contains('_='), isTrue);
    });

    test('empty url returns empty', () {
      expect(urlWithCacheBuster(''), '');
    });
  });

  group('JsonHubProtocol.createMessageHeadersFromJson', () {
    test('non-map headers become null (no throw)', () {
      expect(JsonHubProtocol.createMessageHeadersFromJson('not-a-map'), isNull);
      expect(JsonHubProtocol.createMessageHeadersFromJson(42), isNull);
    });

    test('map with non-string values stringifies keys and values', () {
      final h = JsonHubProtocol.createMessageHeadersFromJson(<String, dynamic>{
        'a': 1,
        'b': true,
      });
      expect(h, isNotNull);
      expect(h!.getHeaderValue('a'), '1');
      expect(h.getHeaderValue('b'), 'true');
    });
  });

  group('Nullable hub arguments — JSON', () {
    final log = Logger('edge_cases');
    final protocol = JsonHubProtocol();

    test('InvocationMessage write/parse preserves null in arguments', () {
      final msg = InvocationMessage(
        target: 'SomeMethod',
        arguments: ['value', null, 123],
        invocationId: '1',
      );
      final wire = protocol.writeMessage(msg);
      final parsed = protocol.parseMessages(wire, log);
      expect(parsed, hasLength(1));
      final back = parsed.single as InvocationMessage;
      expect(back.arguments, ['value', null, 123]);
    });

    test('StreamInvocationMessage write/parse preserves null in arguments', () {
      final msg = StreamInvocationMessage(
        target: 'StreamMethod',
        arguments: [null, 'x', 2],
        invocationId: '2',
        streamIds: const [],
      );
      final wire = protocol.writeMessage(msg);
      final parsed = protocol.parseMessages(wire, log);
      expect(parsed, hasLength(1));
      final back = parsed.single as StreamInvocationMessage;
      expect(back.arguments, [null, 'x', 2]);
    });
  });

  group('Nullable hub arguments — MessagePack', () {
    final log = Logger('edge_cases_mp');
    final protocol = MessagePackHubProtocol();

    test('InvocationMessage write/parse preserves null in arguments', () {
      final msg = InvocationMessage(
        target: 'M',
        arguments: ['a', null, 7],
        invocationId: '9',
      );
      final written = protocol.writeMessage(msg);
      expect(written, isA<Uint8List>());
      final parsed = protocol.parseMessages(written as Uint8List, log);
      expect(parsed, hasLength(1));
      final back = parsed.single as InvocationMessage;
      expect(back.arguments, ['a', null, 7]);
    });

    test('StreamInvocationMessage write/parse preserves null in arguments', () {
      final msg = StreamInvocationMessage(
        target: 'S',
        arguments: [null, 'ok'],
        invocationId: '10',
        streamIds: null,
      );
      final written = protocol.writeMessage(msg);
      expect(written, isA<Uint8List>());
      final parsed = protocol.parseMessages(written as Uint8List, log);
      expect(parsed, hasLength(1));
      final back = parsed.single as StreamInvocationMessage;
      expect(back.arguments, [null, 'ok']);
    });

    test('StreamInvocationMessage with streamIds roundtrips', () {
      final msg = StreamInvocationMessage(
        target: 'T',
        arguments: [1, null],
        invocationId: '11',
        streamIds: ['s1'],
      );
      final written = protocol.writeMessage(msg);
      final parsed = protocol.parseMessages(written as Uint8List, log);
      expect(parsed, hasLength(1));
      final back = parsed.single as StreamInvocationMessage;
      expect(back.arguments, [1, null]);
      expect(back.streamIds, ['s1']);
    });
  });
}
