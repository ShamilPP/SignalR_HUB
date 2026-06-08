import 'dart:async';

import '../protocol/ihub_protocol.dart';

const int defaultHubTimeoutInMs = 30 * 1000;
const int defaultHubPingIntervalInMs = 15 * 1000;

/// @nodoc
const int DEFAULT_TIMEOUT_IN_MS = defaultHubTimeoutInMs;

/// @nodoc
const int DEFAULT_PING_INTERVAL_IN_MS = defaultHubPingIntervalInMs;

/// Emits [HubConnectionState] updates for [HubConnection].
class HubConnectionStateMaintainer {
  late StreamController<HubConnectionState> _controller;
  late HubConnectionState _state;

  HubConnectionStateMaintainer(HubConnectionState initial) {
    _controller = StreamController<HubConnectionState>.broadcast();
    _state = initial;
  }

  set hubConnectionState(HubConnectionState value) {
    _state = value;
    _controller.add(_state);
  }

  HubConnectionState get hubConnectionState => _state;

  Stream<HubConnectionState> get hubConnectionStateStream => _controller.stream;
}

/// Lifecycle state of [HubConnection].
enum HubConnectionState {
  disconnected,
  connecting,
  connected,
  disconnecting,
  reconnecting,
}

typedef InvocationEventCallback = void Function(
  HubMessageBase? invocationEvent,
  Object? error,
);
typedef MethodInvocationFunc = void Function(List<Object?>? arguments);
typedef ClosedCallback = void Function({Object? error});
typedef ReconnectingCallback = void Function({Object? error});
typedef ReconnectedCallback = void Function({String? connectionId});
