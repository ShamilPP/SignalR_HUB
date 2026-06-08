import 'dart:math';

abstract class IRetryPolicy {
  int? nextRetryDelayInMilliseconds(RetryContext retryContext);
}

class RetryContext {
  final int elapsedMilliseconds;
  final int previousRetryCount;
  final Exception retryReason;

  RetryContext(
    this.elapsedMilliseconds,
    this.previousRetryCount,
    this.retryReason,
  );
}

class DefaultRetryPolicy implements IRetryPolicy {
  late List<int?> _retryDelays;
  final Random _random = Random();

  /// Jitter factor applied to each delay (0.0 = no jitter, 1.0 = up to 100% extra).
  /// Default is 0.2 (up to 20% extra random delay).
  final double jitterFactor;

  static const List<int?> DEFAULT_RETRY_DELAYS_IN_MILLISECONDS = [
    0,
    2000,
    10000,
    30000,
    null,
  ];

  DefaultRetryPolicy({List<int>? retryDelays, this.jitterFactor = 0.2}) {
    _retryDelays = retryDelays != null
        ? [...retryDelays, null]
        : DEFAULT_RETRY_DELAYS_IN_MILLISECONDS;
  }

  @override
  int? nextRetryDelayInMilliseconds(RetryContext retryContext) {
    final i = retryContext.previousRetryCount;
    if (i < 0 || i >= _retryDelays.length) {
      return null;
    }
    final baseDelay = _retryDelays[i];
    if (baseDelay == null || baseDelay == 0) return baseDelay;
    final jitter = (_random.nextDouble() * baseDelay * jitterFactor).toInt();
    return baseDelay + jitter;
  }
}
