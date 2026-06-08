import 'dart:async';

import 'package:logging/logging.dart';

import '../core/signalr_exception.dart';
import '../core/iconnection.dart';
import '../core/iretry_policy.dart';
import '../protocol/handshake_protocol.dart';
import '../protocol/ihub_protocol.dart';
import '../shared/utils.dart';
import 'hub_connection_state.dart';

/// Connection to an ASP.NET Core SignalR hub.
///
/// Failures from [start], [invoke], [stream], and [stop] are surfaced as
/// [SignalRException]. Use [SignalRException.toJson] for
/// structured diagnostics.
class HubConnection {
  // Either a string (json) or Uint8List (binary);
  Object? _cachedPingMessage;
  final IConnection _connection;
  final Logger _logger;
  final IRetryPolicy _reconnectPolicy;
  final IHubProtocol _protocol;
  final HandshakeProtocol _handshakeProtocol;

  late Map<String?, InvocationEventCallback> _callbacks;
  late Map<String, List<MethodInvocationFunc>> _methods;
  int _invocationId = 0;
  final List<StreamSubscription<Object>> _streamSubscriptions = [];

  late List<ClosedCallback> _closedCallbacks;
  late List<ReconnectingCallback> _reconnectingCallbacks;
  late List<ReconnectedCallback> _reconnectedCallbacks;

  late bool _receivedHandshakeResponse;
  Completer<void>? _handshakeCompleter;
  Object? _stopDuringStartError;

  late final HubConnectionStateMaintainer _hubConnectionStateMaintainer;
  HubConnectionState get _connectionState =>
      _hubConnectionStateMaintainer.hubConnectionState;
  set _connectionState(HubConnectionState hubConnectionState) {
    _hubConnectionStateMaintainer.hubConnectionState = hubConnectionState;
  }

  // connectionStarted is tracked independently from connectionState, so we can check if the
  // connection ever did successfully transition from connecting to connected before disconnecting.
  late bool _connectionStarted;
  Future<void>? _startPromise;
  Future<void>? _stopPromise;

  // The type of these a) doesn't matter and b) varies when building in browser and node contexts
  // Since we're building the WebPack bundle directly from the TypeScript, this matters (previously
  // we built the bundle from the compiled JavaScript).
  Timer? _timeoutTimer;
  Timer? _pingServerTimer;

  /// The server timeout in milliseconds.
  ///
  /// If this timeout elapses without receiving any messages from the server, the connection will be terminated with an error.
  /// The default timeout value is 30,000 milliseconds (30 seconds).
  ///
  late int serverTimeoutInMilliseconds;

  /// Default interval at which to ping the server.
  ///
  /// The default value is 15,000 milliseconds (15 seconds).
  /// Allows the server to detect hard disconnects (like when a client unplugs their computer).
  ///
  late int keepAliveIntervalInMilliseconds;

  /// Indicates the state of the {@link HubConnection} to the server.
  HubConnectionState? get state => _connectionState;

  /// Emits upon changes of the {@link HubConnectionState}.
  Stream<HubConnectionState> get stateStream =>
      _hubConnectionStateMaintainer.hubConnectionStateStream;

  /// Represents the connection id of the {@link HubConnection} on the server. The connection id will be null when the connection is either
  /// in the disconnected state or if the negotiation step was skipped.
  String? get connectionId => _connection.connectionId;

  /// Indicates the url of the {@link HubConnection} to the server. */
  String? get baseUrl => _connection.baseUrl ?? "";

  /// Sets a new url for the HubConnection. Note that the url can only be changed when the connection is in either the Disconnected or
  /// Reconnecting states.
  /// @param {string} url The url to connect to.
  set baseUrl(String? url) {
    if (_connectionState != HubConnectionState.disconnected &&
        _connectionState != HubConnectionState.reconnecting) {
      throw SignalRException(
          message:
              "The HubConnection must be in the Disconnected or Reconnecting state to change the url.",
          type: SignalRExceptionType.signalr);
    }

    if (url == null) {
      throw SignalRException(
          message: "The HubConnection url must be a valid url.",
          type: SignalRExceptionType.signalr);
    }

    _connection.baseUrl = url;
  }

  static HubConnection create(
    IConnection connection,
    Logger logger,
    IHubProtocol protocol, {
    IRetryPolicy? reconnectPolicy,
  }) {
    return HubConnection(
      connection,
      logger,
      protocol,
      reconnectPolicy: reconnectPolicy,
    );
  }

  HubConnection(IConnection connection, Logger logger, IHubProtocol protocol,
      {IRetryPolicy? reconnectPolicy})
      : _connection = connection,
        _logger = logger,
        _protocol = protocol,
        _reconnectPolicy = reconnectPolicy ?? DefaultRetryPolicy(),
        _handshakeProtocol = HandshakeProtocol() {
    serverTimeoutInMilliseconds = DEFAULT_TIMEOUT_IN_MS;
    keepAliveIntervalInMilliseconds = DEFAULT_PING_INTERVAL_IN_MS;

    _connection.onreceive = _processIncomingData;
    _connection.onclose = _connectionClosed;

    _callbacks = {};
    _methods = {};
    _closedCallbacks = [];
    _reconnectingCallbacks = [];
    _reconnectedCallbacks = [];
    _receivedHandshakeResponse = false;
    _hubConnectionStateMaintainer =
        HubConnectionStateMaintainer(HubConnectionState.disconnected);
    _connectionStarted = false;

    _cachedPingMessage = _protocol.writeMessage(PingMessage());
  }

  /// Starts the connection.
  ///
  /// Returns a Promise that resolves when the connection has been successfully established, or rejects with an error.
  ///
  Future<void> start() async {
    final started = _startWithStateTransitions();
    _startPromise = started;
    return started;
  }

  Future<void> _startWithStateTransitions() async {
    if (_connectionState != HubConnectionState.disconnected) {
      return Future.error(SignalRException(
          message:
              "Cannot start a HubConnection that is not in the 'Disconnected' state.",
          type: SignalRExceptionType.signalr));
    }

    _connectionState = HubConnectionState.connecting;
    _logger.finer("Starting HubConnection.");

    try {
      await _startInternal();

      _connectionState = HubConnectionState.connected;
      _connectionStarted = true;
      _logger.finer("HubConnection connected successfully.");
    } catch (e) {
      _connectionState = HubConnectionState.disconnected;
      _logger.finer(
          "HubConnection failed to start successfully because of error '$e'.");
      return Future.error(e);
    }
  }

  _startInternal() async {
    _stopDuringStartError = null;
    _receivedHandshakeResponse = false;
    // Set up the promise before any connection is (re)started otherwise it could race with received messages
    _handshakeCompleter = Completer<void>();
    await _connection.start(transferFormat: _protocol.transferFormat);

    try {
      final handshakeRequest =
          HandshakeRequestMessage(_protocol.name, _protocol.version);

      _logger.finer("Sending handshake request.");

      await _sendMessage(
          _handshakeProtocol.writeHandshakeRequest(handshakeRequest));

      _logger.info("Using HubProtocol '${_protocol.name}'.");

      // defensively cleanup timeout in case we receive a message from the server before we finish start
      _cleanupTimeout();
      _resetTimeoutPeriod();
      _resetKeepAliveInterval();

      final handshakeDone = _handshakeCompleter;
      await handshakeDone?.future;

      // It's important to check the stopDuringStartError instead of just relying on the handshakePromise
      // being rejected on close, because this continuation can run after both the handshake completed successfully
      // and the connection was closed.
      final duringStart = _stopDuringStartError;
      if (duringStart != null) {
        // It's important to throw instead of returning a rejected promise, because we don't want to allow any state
        // transitions to occur between now and the calling code observing the exceptions. Returning a rejected promise
        // will cause the calling continuation to get scheduled to run later.
        throw duringStart;
      }
    } catch (e) {
      _logger.finer(
          "Hub handshake failed with error '$e' during start(). Stopping HubConnection.");

      _cancelAllTimers();

      // HttpConnection.stop() should not complete until after the onclose callback is invoked.
      // This will transition the HubConnection to the disconnected state before HttpConnection.stop() completes.
      await _connection.stop(
        error: SignalRException.handler(
            error: e,
            message: e.toString(),
            type: SignalRExceptionType.signalr,
            stackTrace: null),
      );
      rethrow;
    }
  }

  /// Stops the connection.
  ///
  /// Returns a Promise that resolves when the connection has been successfully terminated, or rejects with an error.
  ///
  Future<void> stop() async {
    // Capture the start promise before the connection might be restarted in an onclose callback.
    final startPromise = _startPromise;

    _stopPromise = _stopInternal();
    await _stopPromise;

    try {
      // Awaiting undefined continues immediately
      await startPromise;
    } catch (e) {
      // This exception is returned to the user as a rejected Promise from the start method.
    }
  }

  Future<void>? _stopInternal({Object? error}) async {
    if (_connectionState == HubConnectionState.disconnected) {
      _logger.finer(
          "Call to HubConnection.stop($error) ignored because it is already in the disconnected state.");
      return Future.value();
    }

    if (_connectionState == HubConnectionState.disconnecting) {
      _logger.finer(
          "Call to HubConnection.stop($error) ignored because the connection is already in the disconnecting state.");
      return _stopPromise;
    }

    final wasReconnecting = _connectionState == HubConnectionState.reconnecting;
    _connectionState = HubConnectionState.disconnecting;

    _logger.finer("Stopping HubConnection.");

    if (wasReconnecting) {
      _logger.finer(
          "Connection stopped during reconnect delay. Done reconnecting.");
      _completeClose();
      return Future.value();
    }

    _cancelStreamSubscriptions();
    _cancelAllTimers();
    _stopDuringStartError = error ??
        SignalRException(
            message:
                "The connection was stopped before the hub handshake could complete.",
            type: SignalRExceptionType.signalr);

    // HttpConnection.stop() should not complete until after either HttpConnection.start() fails
    // or the onclose callback is invoked. The onclose callback will transition the HubConnection
    // to the disconnected state if need be before HttpConnection.stop() completes.
    return _connection.stop(error: error);
  }

  /// Invokes a streaming hub method on the server using the specified name and arguments.
  ///
  /// T: The type of the items returned by the server.
  /// methodName: The name of the server method to invoke.
  /// args: The arguments used to invoke the server method.
  /// Returns an object that yields results from the server as they are received.
  ///
  Stream<Object?> stream(String methodName, List<Object?> args) {
    return streamControllable(methodName, args).stream;
  }

  /// Invokes a streaming hub method on the server using the specified name and arguments.
  ///
  /// T: The type of the items returned by the server.
  /// methodName: The name of the server method to invoke.
  /// args: The arguments used to invoke the server method.
  /// Returns a StreamControler object that yields results from the server as they are received.
  ///
  StreamController<Object?> streamControllable(
      String methodName, List<Object?> args) {
    final t = _replaceStreamingParams(args);
    final invocationDescriptor =
        _createStreamInvocation(methodName, args, t.keys.toList());

    late Future<void> promiseQueue;
    final StreamController streamController = StreamController<Object?>(
      onCancel: () {
        final cancelInvocation =
            _createCancelInvocation(invocationDescriptor.invocationId);
        _callbacks.remove(invocationDescriptor.invocationId);

        return promiseQueue.then((_) => _sendWithProtocol(cancelInvocation));
      },
    );

    _callbacks[invocationDescriptor.invocationId] =
        (HubMessageBase? invocationEvent, Object? error) {
      if (error != null) {
        streamController.addError(error);
        return;
      } else if (invocationEvent != null) {
        // invocationEvent will not be null when an error is not passed to the callback
        if (invocationEvent is CompletionMessage) {
          if (invocationEvent.error != null) {
            streamController.addError(SignalRException(
                message: invocationEvent.error ?? 'Unknown error',
                type: SignalRExceptionType.signalr));
          } else {
            streamController.close();
          }
        } else if (invocationEvent is StreamItemMessage) {
          streamController.add(invocationEvent.item);
        }
      }
    };

    promiseQueue = _sendWithProtocol(invocationDescriptor).catchError((e) {
      streamController.addError(e);
      _callbacks.remove(invocationDescriptor.invocationId);
    });

    _launchStreams(t, promiseQueue);

    return streamController;
  }

  Future<void> _sendMessage(Object? message) {
    _resetKeepAliveInterval();
    return _connection.send(message);
  }

  /// Sends a js object to the server.
  /// message: The object to serialize and send.
  ///
  Future<void> _sendWithProtocol(Object message) {
    return _sendMessage(_protocol.writeMessage(message as HubMessageBase));
  }

  /// Invokes a hub method on the server using the specified name and arguments. Does not wait for a response from the receiver.
  ///
  /// The Promise returned by this method resolves when the client has sent the invocation to the server. The server may still
  /// be processing the invocation.
  ///
  /// methodName: The name of the server method to invoke.
  /// args: The arguments used to invoke the server method.
  /// Returns a Promise that resolves when the invocation has been successfully sent, or rejects with an error.
  ///
  Future<void> send(String methodName, {List<Object?>? args}) {
    args = args ?? <Object?>[];
    final t = _replaceStreamingParams(args);
    final sendPromise = _sendWithProtocol(
        _createInvocation(methodName, args, true, t.keys.toList()));

    _launchStreams(t, sendPromise);
    return sendPromise;
  }

  /// Invokes a hub method on the server using the specified name and arguments.
  ///
  /// The Future returned by this method resolves when the server indicates it has finished invoking the method. When the Future
  /// resolves, the server has finished invoking the method. If the server method returns a result, it is produced as the result of
  /// resolving the Promise.
  ///
  /// methodName: The name of the server method to invoke.
  /// args: The arguments used to invoke the server method.
  /// Returns a Future that resolves with the result of the server method (if any), or rejects with an error.
  ///
  Future<Object?> invoke(String methodName, {List<Object?>? args}) {
    args = args ?? <Object?>[];
    final t = _replaceStreamingParams(args);
    final invocationDescriptor =
        _createInvocation(methodName, args, false, t.keys.toList());

    final completer = Completer<Object?>();

    _callbacks[invocationDescriptor.invocationId] =
        (HubMessageBase? invocationEvent, Object? error) {
      if (error != null) {
        if (!completer.isCompleted) completer.completeError(error);
        return;
      } else if (invocationEvent != null) {
        if (invocationEvent is CompletionMessage) {
          if (invocationEvent.error != null) {
            if (!completer.isCompleted) {
              completer.completeError(SignalRException(
                  message: invocationEvent.error ?? 'Unknown error',
                  type: SignalRExceptionType.signalr));
            }
          } else {
            if (!completer.isCompleted) {
              completer.complete(invocationEvent.result);
            }
          }
        } else {
          if (!completer.isCompleted) {
            completer.completeError(SignalRException(
                message: "Unexpected message type: ${invocationEvent.type}",
                type: SignalRExceptionType.signalr));
          }
        }
      }
    };

    final promiseQueue =
        _sendWithProtocol(invocationDescriptor).catchError((e) {
      if (!completer.isCompleted) completer.completeError(e);
      // invocationId will always have a value for a non-blocking invocation
      _callbacks.remove(invocationDescriptor.invocationId);
    });

    _launchStreams(t, promiseQueue);

    return completer.future;
  }

  ///  Registers a handler that will be invoked when the hub method with the specified method name is invoked.
  ///
  /// methodName: The name of the hub method to define.
  /// newMethod: The handler that will be raised when the hub method is invoked.
  ///
  void on(String methodName, MethodInvocationFunc newMethod) {
    if (isStringEmpty(methodName)) {
      return;
    }

    methodName = methodName.toLowerCase();
    final handlers =
        _methods.putIfAbsent(methodName, () => <MethodInvocationFunc>[]);

    // Preventing adding the same handler multiple times.
    if (handlers.contains(newMethod)) {
      return;
    }

    handlers.add(newMethod);
  }

  /// Removes the specified handler for the specified hub method.
  ///
  /// You must pass the exact same Function instance as was previously passed to HubConnection.on. Passing a different instance (even if the function
  /// body is the same) will not remove the handler.
  ///
  /// methodName: The name of the method to remove handlers for.
  /// method: The handler to remove. This must be the same Function instance as the one passed to {@link @microsoft/signalr.HubConnection.on}.
  /// If the method handler is omitted, all handlers for that method will be removed.
  ///
  void off(String methodName, {MethodInvocationFunc? method}) {
    if (isStringEmpty(methodName)) {
      return;
    }

    methodName = methodName.toLowerCase();
    final List<MethodInvocationFunc>? handlers = _methods[methodName];
    if (handlers == null) {
      return;
    }

    if (method != null) {
      final removeIdx = handlers.indexOf(method);
      if (removeIdx != -1) {
        handlers.removeAt(removeIdx);
        if (handlers.isEmpty) {
          _methods.remove(methodName);
        }
      }
    } else {
      _methods.remove(methodName);
    }
  }

  /// Registers a handler that will be invoked when the connection is closed.
  ///
  /// callback: The handler that will be invoked when the connection is closed. Optionally receives a single argument containing the error that caused the connection to close (if any).
  ///
  void onclose(ClosedCallback callback) {
    _closedCallbacks.add(callback);
  }

  /// Registers a handler that will be invoked when the connection starts reconnecting.
  ///
  /// callback: The handler that will be invoked when the connection starts reconnecting. Optionally receives a single argument containing the error that caused the connection to start reconnecting (if any).
  ///
  void onreconnecting(ReconnectingCallback callback) {
    _reconnectingCallbacks.add(callback);
  }

  /// Registers a handler that will be invoked when the connection successfully reconnects.
  ///
  /// callback: The handler that will be invoked when the connection successfully reconnects.
  ///
  void onreconnected(ReconnectedCallback callback) {
    _reconnectedCallbacks.add(callback);
  }

  void _processIncomingData(Object? data) {
    _cleanupTimeout();

    if (!_receivedHandshakeResponse) {
      data = _processHandshakeResponse(data);
      _receivedHandshakeResponse = true;
    }

    // Data may have all been read when processing handshake response
    if (data != null) {
      // Parse the messages
      final messages = _protocol.parseMessages(data, _logger);

      for (final message in messages) {
        _logger.finest("Handle message of type '${message.type}'.");
        switch (message.type) {
          case MessageType.invocation:
            _invokeClientMethod(message as InvocationMessage);
            break;
          case MessageType.streamItem:
          case MessageType.completion:
            final invocationMsg = message as HubInvocationMessage;
            final void Function(HubMessageBase, Exception?)? callback =
                _callbacks[invocationMsg.invocationId];
            if (callback != null) {
              if (message.type == MessageType.completion) {
                _callbacks.remove(invocationMsg.invocationId);
              }
              callback(message, null);
            }
            break;
          case MessageType.ping:
            // Don't care about pings
            break;
          case MessageType.close:
            _logger.info("Close message received from server.");
            final closeMessage = message as CloseMessage;

            final closeErr = closeMessage.error;
            final Exception? error = closeErr != null
                ? SignalRException(
                    message: "Server returned an error on close: $closeErr",
                    type: SignalRExceptionType.signalr)
                : null;

            if (closeMessage.allowReconnect == true) {
              // It feels wrong not to await connection.stop() here, but processIncomingData is called as part of an onreceive callback which is not async,
              // this is already the behavior for serverTimeout(), and HttpConnection.Stop() should catch and log all possible exceptions.

              _connection.stop(error: error);
            } else {
              // We cannot await stopInternal() here, but subsequent calls to stop() will await this if stopInternal() is still ongoing.
              _stopPromise = _stopInternal(error: error);
            }

            break;
          default:
            _logger.warning("Invalid message type: '${message.type}'");
            break;
        }
      }
    }

    _resetTimeoutPeriod();
  }

  /// data is either a string (json) or a Uint8List (binary)
  Object? _processHandshakeResponse(Object? data) {
    ParseHandshakeResponseResult handshakeResult;

    try {
      handshakeResult = _handshakeProtocol.parseHandshakeResponse(data);
    } catch (e) {
      final message = "Error parsing handshake response: '${e.toString()}'.";
      _logger.severe(message);
      return _failHandshake(SignalRException(
          message: message, type: SignalRExceptionType.signalr));
    }
    if (!isStringEmpty(handshakeResult.handshakeResponseMessage.error)) {
      final message =
          "Server returned handshake error: '${handshakeResult.handshakeResponseMessage.error}'";
      _logger.severe(message);
      return _failHandshake(SignalRException(
          message: message, type: SignalRExceptionType.signalr));
    } else {
      _logger.finer("Server handshake complete.");
    }

    _completeHandshakeIfPending();
    return handshakeResult.remainingData;
  }

  Never _failHandshake(Exception error) {
    final hs = _handshakeCompleter;
    if (hs != null && !hs.isCompleted) {
      hs.completeError(error);
    }
    _handshakeCompleter = null;
    throw error;
  }

  void _completeHandshakeIfPending() {
    final hs = _handshakeCompleter;
    if (hs != null && !hs.isCompleted) {
      hs.complete();
    }
    _handshakeCompleter = null;
  }

  void _resetKeepAliveInterval() {
    _cleanupPingTimer();
    _pingServerTimer =
        Timer.periodic(Duration(milliseconds: keepAliveIntervalInMilliseconds),
            (Timer t) async {
      if (_connectionState == HubConnectionState.connected) {
        try {
          await _sendMessage(_cachedPingMessage);
        } catch (e) {
          // We don't care about the error. It should be seen elsewhere in the client.
          // The connection is probably in a bad or closed state now, cleanup the timer so it stops triggering
          _cleanupPingTimer();
        }
      }
    });
  }

  void _resetTimeoutPeriod() {
    _cleanupTimeout();
    final inherent = _connection.features?.inherentKeepAlive;
    if (inherent != true) {
      // Set the timeout timer
      _timeoutTimer = Timer.periodic(
          Duration(milliseconds: serverTimeoutInMilliseconds), _serverTimeout);
    }
  }

  void _serverTimeout(Timer t) {
    final stopFuture = _connection.stop(
      error: SignalRException(
        message:
            "Server timeout elapsed without receiving a message from the server.",
        type: SignalRExceptionType.timeout,
      ),
    );
    if (stopFuture != null) {
      unawaited(stopFuture.catchError((Object e, StackTrace _) {
        _logger.finer("Could not shutdown connection after server timeout: $e");
      }));
    }
  }

  void _invokeClientMethod(InvocationMessage invocationMessage) {
    final target = invocationMessage.target;
    if (target == null) {
      _logger.warning('Invocation received with null target.');
      return;
    }
    final methods = _methods[target.toLowerCase()];
    if (methods != null) {
      for (var m in methods) {
        m(invocationMessage.arguments);
      }
      if (!isStringEmpty(invocationMessage.invocationId)) {
        // This is not supported in v1. So we return an error to avoid blocking the server waiting for the response.
        final message =
            "Server requested a response, which is not supported in this version of the client.";
        _logger.severe(message);

        // We don't need to wait on this Promise.
        _stopPromise = _stopInternal(
            error: SignalRException(
                message: message, type: SignalRExceptionType.signalr));
      }
    } else {
      _logger.warning("No client method with the name '$target' found.");
    }
  }

  void _connectionClosed({Object? error}) {
    _logger.finer(
        "HubConnection.connectionClosed($error) called while in state $_connectionState.");

    // Triggering this.handshakeRejecter is insufficient because it could already be resolved without the continuation having run yet.
    _stopDuringStartError = _stopDuringStartError ??
        error ??
        SignalRException(
            message:
                "The underlying connection was closed before the hub handshake could complete.",
            type: SignalRExceptionType.signalr);

    // If the handshake is in progress, start will be waiting for the handshake promise, so we complete it.
    // If it has already completed, this should just noop.
    final handshake = _handshakeCompleter;
    if (handshake != null && !handshake.isCompleted) {
      handshake.complete();
    }

    _cancelCallbacksWithError(error ??
        SignalRException(
            message:
                "Invocation canceled due to the underlying connection being closed.",
            type: SignalRExceptionType.signalr));

    _cancelStreamSubscriptions();
    _cancelAllTimers();

    if (_connectionState == HubConnectionState.disconnecting) {
      _completeClose(error: error);
    } else if (_connectionState == HubConnectionState.connected) {
      _reconnect(error: error);
    }

    // If none of the above if conditions were true were called the HubConnection must be in either:
    // 1. The Connecting state in which case the handshakeResolver will complete it and stopDuringStartError will fail it.
    // 2. The Reconnecting state in which case the handshakeResolver will complete it and stopDuringStartError will fail the current reconnect attempt
    //    and potentially continue the reconnect() loop.
    // 3. The Disconnected state in which case we're already done.
  }

  _completeClose({Object? error}) {
    if (_connectionStarted) {
      _connectionState = HubConnectionState.disconnected;
      _connectionStarted = false;

      try {
        for (var c in _closedCallbacks) {
          c(error: error);
        } // removed "this"
      } catch (e) {
        _logger.severe(
            "An onclose callback called with error '$error' threw error '$e'.");
      }
    }
  }

  _reconnect({Object? error}) async {
    final reconnectStartTime = DateTime.now();
    var previousReconnectAttempts = 0;
    Object retryError = error ??
        SignalRException(
            message: "Attempting to reconnect due to a unknown error.",
            type: SignalRExceptionType.signalr);

    var nextRetryDelay = _getNextRetryDelay(
        previousReconnectAttempts++, 0, retryError as Exception);

    if (nextRetryDelay == null) {
      _logger.finer(
          "Connection not reconnecting because the IRetryPolicy returned null on the first reconnect attempt.");
      _completeClose(error: error);
      return;
    }

    _connectionState = HubConnectionState.reconnecting;

    if (error != null) {
      _logger.info("Connection reconnecting because of error '$error'.");
    } else {
      _logger.info("Connection reconnecting.");
    }

    try {
      for (var c in _reconnectingCallbacks) {
        c(error: error);
      }
    } catch (e) {
      _logger.severe(
          "An onreconnecting callback called with error '$error' threw error '$e'.");
    }

    // Exit early if an onreconnecting callback called connection.stop().
    if (_connectionState != HubConnectionState.reconnecting) {
      _logger.finer(
          "Connection left the reconnecting state in onreconnecting callback. Done reconnecting.");
      return;
    }

    while (nextRetryDelay != null) {
      _logger.info(
          "Reconnect attempt number $previousReconnectAttempts will start in $nextRetryDelay ms.");

      await Future.delayed(Duration(milliseconds: nextRetryDelay));

      if (_connectionState != HubConnectionState.reconnecting) {
        _logger.finer(
            "Connection left the reconnecting state during reconnect delay. Done reconnecting.");
        return;
      }

      try {
        await _startInternal();

        _connectionState = HubConnectionState.connected;
        _logger.info("HubConnection reconnected successfully.");

        try {
          for (var c in _reconnectedCallbacks) {
            c(connectionId: _connection.connectionId);
          }
        } catch (e) {
          _logger.severe(
              "An onreconnected callback called with connectionId '${_connection.connectionId}' threw error '$e'.");
        }

        return;
      } catch (e) {
        _logger.info("Reconnect attempt failed because of error '$e'.");

        if (_connectionState != HubConnectionState.reconnecting) {
          _logger.finer(
              "Connection left the reconnecting state during reconnect attempt. Done reconnecting.");
          return;
        }

        retryError = SignalRException.handler(
            error: e,
            message: e.toString(),
            type: SignalRExceptionType.signalr,
            stackTrace: null);
        nextRetryDelay = _getNextRetryDelay(
            previousReconnectAttempts++,
            DateTime.now().difference(reconnectStartTime).inMilliseconds,
            retryError);
      }
    }

    _logger.info(
        "Reconnect retries have been exhausted after ${DateTime.now().difference(reconnectStartTime).inMilliseconds} ms and $previousReconnectAttempts failed attempts. Connection disconnecting.");

    _completeClose();
  }

  int? _getNextRetryDelay(
      int previousRetryCount, int elapsedMilliseconds, Exception retryReason) {
    try {
      return _reconnectPolicy.nextRetryDelayInMilliseconds(
          RetryContext(elapsedMilliseconds, previousRetryCount, retryReason));
    } catch (e) {
      _logger.severe(
          "IRetryPolicy.nextRetryDelayInMilliseconds($previousRetryCount, $elapsedMilliseconds) threw error '$e'.");
      return null;
    }
  }

  _cancelCallbacksWithError(Object error) {
    final Map<String?, void Function(HubMessageBase?, Object)> callbacks =
        _callbacks;
    _callbacks = {};

    callbacks.forEach((_, value) => value(null, error));
  }

  _cancelAllTimers() {
    _cleanupTimeout();
    _cleanupPingTimer();
  }

  _cancelStreamSubscriptions() {
    for (final sub in _streamSubscriptions) {
      sub.cancel();
    }
    _streamSubscriptions.clear();
  }

  void _cleanupPingTimer() {
    final t = _pingServerTimer;
    if (t != null) {
      t.cancel();
      _pingServerTimer = null;
    }
  }

  void _cleanupTimeout() {
    final t = _timeoutTimer;
    if (t != null) {
      t.cancel();
      _timeoutTimer = null;
    }
  }

  InvocationMessage _createInvocation(String methodName, List<Object?> args,
      bool nonblocking, List<String> streamIds) {
    if (nonblocking) {
      return InvocationMessage(
          target: methodName,
          arguments: args,
          streamIds: streamIds,
          headers: MessageHeaders(),
          invocationId: null);
    } else {
      final invocationId = _invocationId;
      _invocationId = _invocationId + 1;

      return InvocationMessage(
          target: methodName,
          arguments: args,
          streamIds: streamIds,
          headers: MessageHeaders(),
          invocationId: invocationId.toString());
    }
  }

  _launchStreams(
      Map<String, Stream<Object>> streams, Future<void>? promiseQueue) {
    if (streams.isEmpty) {
      return;
    }

    // Synchronize stream data so they arrive in-order on the server
    promiseQueue ??= Future.value();

    // We want to iterate over the keys, since the keys are the stream ids
    streams.forEach((id, stream) {
      final subscription = stream.listen((item) {
        promiseQueue = promiseQueue?.then(
            (_) => _sendWithProtocol(_createStreamItemMessage(id, item)));
      }, onDone: () {
        promiseQueue = promiseQueue
            ?.then((_) => _sendWithProtocol(_createCompletionMessage(id)));
      }, onError: (err) {
        final message = SignalRException.handler(
                error: err,
                message: err.toString(),
                type: SignalRExceptionType.signalr,
                stackTrace: null)
            .toString();

        promiseQueue = promiseQueue?.then((_) =>
            _sendWithProtocol(_createCompletionMessage(id, error: message)));
      });
      _streamSubscriptions.add(subscription);
    });
  }

  Map<String, Stream<Object>> _replaceStreamingParams(List<Object?> args) {
    final streams = <String, Stream<Object>>{};

    for (var i = 0; i < args.length; i++) {
      final argument = args[i];
      if (argument is Stream) {
        final streamId = _invocationId;
        _invocationId = _invocationId + 1;
        // Store the stream for later use
        streams[streamId.toString()] = argument as Stream<Object>;

        // remove stream from args
        args.removeAt(i);
      }
    }

    return streams;
  }

  /// isObservable

  StreamInvocationMessage _createStreamInvocation(
      String methodName, List<Object?> args, List<String> streamIds) {
    final invocationId = _invocationId;
    _invocationId = _invocationId + 1;

    return StreamInvocationMessage(
        target: methodName,
        arguments: args,
        streamIds: streamIds,
        headers: MessageHeaders(),
        invocationId: invocationId.toString());
  }

  CancelInvocationMessage _createCancelInvocation(String? id) {
    return CancelInvocationMessage(headers: MessageHeaders(), invocationId: id);
  }

  StreamItemMessage _createStreamItemMessage(String id, Object item) {
    return StreamItemMessage(
        item: item, headers: MessageHeaders(), invocationId: id);
  }

  CompletionMessage _createCompletionMessage(String id,
      {Object? error, Object? result}) {
    if (error != null) {
      return CompletionMessage(
          error: error as String?, headers: MessageHeaders(), invocationId: id);
    }
    if (result != null) {
      return CompletionMessage(
          result: result, headers: MessageHeaders(), invocationId: id);
    }
    return CompletionMessage(headers: MessageHeaders(), invocationId: id);
  }
}
