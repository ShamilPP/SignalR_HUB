import 'package:flutter/foundation.dart';

enum ExceptionType {
  dartException,
  jsInteropObject,
  unknown;

  static ExceptionType exceptionType(Object? error) {
    if (error is Exception || error is Error) {
      return ExceptionType.dartException;
    } else if (kIsWeb) {
      return ExceptionType.jsInteropObject;
    } else {
      return ExceptionType.unknown;
    }
  }
}

enum SignalRExceptionType {
  http,
  abort,
  timeout,
  notImplemented,
  invalidPayload,
  signalr,
  unknown;

  bool get isSignalr => this == SignalRExceptionType.signalr;
  bool get isHttp => this == SignalRExceptionType.http;
  bool get isAbort => this == SignalRExceptionType.abort;
  bool get isTimeout => this == SignalRExceptionType.timeout;
  bool get isNotImplemented => this == SignalRExceptionType.notImplemented;
  bool get isInvalidPayload => this == SignalRExceptionType.invalidPayload;
  bool get isUnknown => this == SignalRExceptionType.unknown;
}

class SignalRException implements Exception {
  final String message;
  final Object? original;
  final int? statusCode;
  final StackTrace? stackTrace;
  final SignalRExceptionType type;
  SignalRException(
      {required this.message,
      this.original,
      this.stackTrace,
      this.statusCode,
      this.type = SignalRExceptionType.unknown});

  static SignalRException handler({
    int statusCode = 0,
    required Object? error,
    required String message,
    required SignalRExceptionType type,
    required StackTrace? stackTrace,
  }) {
    final exceptionType = ExceptionType.exceptionType(error);
    switch (exceptionType) {
      case ExceptionType.dartException:
        return SignalRException(
          type: type,
          original: error,
          message: message,
          stackTrace: stackTrace,
          statusCode: statusCode,
        );
      case ExceptionType.jsInteropObject:
        return SignalRException(
          type: type,
          stackTrace: stackTrace,
          statusCode: statusCode,
          original: Exception(error.toString()),
          message: "JAVASCRIPT ERROR TYPE: ${error.runtimeType}",
        );
      case ExceptionType.unknown:
        return SignalRException(
          type: type,
          original: error,
          statusCode: statusCode,
          stackTrace: stackTrace,
          message: "UNKNOWN ERROR TYPE: ${error.runtimeType}",
        );
    }
  }

  static SignalRException? tryHandler({
    int statusCode = 0,
    required Object? error,
    required String message,
    required SignalRExceptionType type,
    required StackTrace? stackTrace,
  }) {
    if (error != null) {
      final exceptionType = ExceptionType.exceptionType(error);
      switch (exceptionType) {
        case ExceptionType.dartException:
          return SignalRException(
            type: type,
            original: error,
            message: message,
            stackTrace: stackTrace,
            statusCode: statusCode,
          );
        case ExceptionType.jsInteropObject:
          return SignalRException(
            type: type,
            stackTrace: stackTrace,
            statusCode: statusCode,
            original: Exception(error.toString()),
            message: "JAVASCRIPT ERROR TYPE: ${error.runtimeType}",
          );
        case ExceptionType.unknown:
          return SignalRException(
            type: type,
            original: error,
            statusCode: statusCode,
            stackTrace: stackTrace,
            message: "UNKNOWN ERROR TYPE: ${error.runtimeType}",
          );
      }
    }
    return null;
  }

  Map<String, dynamic> toJson() {
    return {
      "type": type.name,
      "message": message,
      "statusCode": statusCode,
      "original": original.toString(),
      "stackTrace": stackTrace.toString(),
      "runtimeType": original.runtimeType.toString(),
    };
  }

  @override
  String toString() {
    return "TYPE: $type,\nSIGNALR EXCEPTION: $message,\nSTACK TRACE: $stackTrace";
  }
}
