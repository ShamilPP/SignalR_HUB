import 'package:flutter_test/flutter_test.dart';
import 'package:signalr_hub/signalr_client.dart';

void main() {
  group('SignalRException.handler', () {
    test('wraps dart Exception', () {
      final e = Exception('test');
      final wrapped = SignalRException.handler(
          error: e,
          message: 'test',
          type: SignalRExceptionType.signalr,
          stackTrace: null);
      expect(wrapped, isA<SignalRException>());
      expect(wrapped.original, same(e));
      expect(wrapped.type.isSignalr, isTrue);
    });

    test('wraps arbitrary object', () {
      final wrapped = SignalRException.handler(
          error: Object(),
          message: 'wrapped',
          type: SignalRExceptionType.signalr,
          stackTrace: null);
      expect(wrapped, isA<SignalRException>());
      expect(wrapped.original, isNotNull);
    });

    test('wraps nested SignalRException using caller type', () {
      final original = SignalRException(
          message: 'original',
          type: SignalRExceptionType.http,
          statusCode: 500);
      final result = SignalRException.handler(
          error: original,
          message: 'outer',
          type: SignalRExceptionType.signalr,
          stackTrace: null);
      expect(result.original, same(original));
      expect(result.type.isSignalr, isTrue);
      expect(result.message, 'outer');
    });

    test('tryHandler returns null for null error', () {
      expect(
        SignalRException.tryHandler(
            error: null,
            message: '',
            type: SignalRExceptionType.signalr,
            stackTrace: null),
        isNull,
      );
    });

    test('handler with null error still produces exception', () {
      final wrapped = SignalRException.handler(
          error: null,
          message: 'null error',
          type: SignalRExceptionType.signalr,
          stackTrace: null);
      expect(wrapped, isA<SignalRException>());
      expect(wrapped.type.isSignalr, isTrue);
    });

    test('toJson returns structured map', () {
      final ex =
          SignalRException(message: 'test', type: SignalRExceptionType.timeout);
      final json = ex.toJson();
      expect(json['type'], 'timeout');
      expect(json['message'], 'test');
    });
  });

  group('SignalRExceptionType getters', () {
    test('isTimeout', () {
      expect(SignalRExceptionType.timeout.isTimeout, isTrue);
      expect(SignalRExceptionType.http.isTimeout, isFalse);
    });
  });
}
