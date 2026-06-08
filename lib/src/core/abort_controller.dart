typedef OnAbort = void Function();

/// Signal that can be monitored to determine if a request has been aborted.
abstract class IAbortSignal {
  /// Whether the request has been aborted.
  bool get aborted;

  /// Invoked when [AbortController.abort] is called.
  OnAbort? onabort;
}

class AbortController implements IAbortSignal {
  bool _aborted = false;

  @override
  OnAbort? onabort;

  @override
  bool get aborted => _aborted;

  IAbortSignal get signal => this;

  void abort() {
    if (_aborted) return;
    _aborted = true;
    onabort?.call();
  }
}
