import 'package:get_it/get_it.dart';
import 'package:logging/logging.dart';

/// Dedicated [GetIt] scope for this package so host apps are not polluted.
final GetIt signalRLocator = GetIt.asNewInstance();

Logger _silentLogger() {
  final log = Logger.detached('signalr_hub');
  log.level = Level.OFF;
  return log;
}

/// Resolves the logger used by connections and transports.
///
/// [override] wins when non-null. Otherwise uses [signalRLocator] if a
/// [Logger] was registered via [registerSignalRLogger], or a silent logger.
Logger resolveSignalRLogger([Logger? override]) {
  if (override != null) return override;
  if (signalRLocator.isRegistered<Logger>()) {
    return signalRLocator<Logger>();
  }
  final silent = _silentLogger();
  signalRLocator.registerSingleton<Logger>(silent);
  return silent;
}

/// Registers the default [Logger] for [resolveSignalRLogger] (e.g. from
/// [HubConnectionBuilder.configureLogging]).
void registerSignalRLogger(Logger logger) {
  if (signalRLocator.isRegistered<Logger>()) {
    signalRLocator.unregister<Logger>();
  }
  signalRLocator.registerSingleton<Logger>(logger);
}
