import 'package:logging/logging.dart';

import '../connection/http_connection.dart';
import '../connection/http_connection_options.dart';
import '../core/signalr_exception.dart';
import '../core/iretry_policy.dart';
import '../core/itransport.dart';
import '../di/signalr_locator.dart';
import '../protocol/ihub_protocol.dart';
import '../protocol/json_hub_protocol.dart';
import '../shared/utils.dart';
import 'hub_connection.dart';

/// Builder for [HubConnection].
class HubConnectionBuilder {
  IHubProtocol? _protocol;
  HttpConnectionOptions? _httpConnectionOptions;
  String? _url;
  Logger? _configureLoggingOverride;
  IRetryPolicy? _reconnectPolicy;

  /// Uses [logger] for the built connection and registers it on [signalRLocator].
  HubConnectionBuilder configureLogging(Logger logger) {
    registerSignalRLogger(logger);
    _configureLoggingOverride = logger;
    return this;
  }

  HubConnectionBuilder withUrl(
    String url, {
    HttpConnectionOptions? options,
    HttpTransportType? transportType,
  }) {
    if (isStringEmpty(url)) {
      throw SignalRException(
          message: 'HubConnectionBuilder.withUrl requires a non-empty url.',
          type: SignalRExceptionType.signalr);
    }
    if (options != null && transportType != null) {
      throw ArgumentError(
          'Cannot specify both options and transportType. Use options.transport instead.');
    }

    _url = url;

    if (options != null) {
      _httpConnectionOptions = options;
    } else {
      _httpConnectionOptions = HttpConnectionOptions(transport: transportType);
    }

    return this;
  }

  HubConnectionBuilder withHubProtocol(IHubProtocol protocol) {
    _protocol = protocol;
    return this;
  }

  HubConnectionBuilder withAutomaticReconnect({
    IRetryPolicy? reconnectPolicy,
    List<int>? retryDelays,
  }) {
    if (_reconnectPolicy != null) {
      throw StateError(
          'withAutomaticReconnect can only be called once per builder.');
    }

    if (reconnectPolicy == null && retryDelays == null) {
      _reconnectPolicy = DefaultRetryPolicy();
    } else if (retryDelays != null) {
      _reconnectPolicy = DefaultRetryPolicy(retryDelays: retryDelays);
    } else {
      _reconnectPolicy = reconnectPolicy;
    }

    return this;
  }

  HubConnection build() {
    final baseOptions = _httpConnectionOptions ?? HttpConnectionOptions();
    final Logger effectiveLogger =
        _configureLoggingOverride ?? baseOptions.logger;
    final httpConnectionOptions = baseOptions.copyWith(logger: effectiveLogger);

    final hubUrl = _url;
    if (hubUrl == null || hubUrl.isEmpty) {
      throw SignalRException(
        message:
            "The 'HubConnectionBuilder.withUrl' method must be called before building the connection.",
        type: SignalRExceptionType.signalr,
      );
    }

    final connection = HttpConnection(hubUrl, options: httpConnectionOptions);
    return HubConnection.create(
      connection,
      httpConnectionOptions.logger,
      _protocol ?? JsonHubProtocol(),
      reconnectPolicy: _reconnectPolicy,
    );
  }
}
