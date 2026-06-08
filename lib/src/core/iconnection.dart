import 'dart:async';

import 'itransport.dart';

class ConnectionFeatures {
  bool? inherentKeepAlive;

  ConnectionFeatures(this.inherentKeepAlive);
}

abstract class IConnection {
  ConnectionFeatures? features;
  String? connectionId;

  String? baseUrl;

  Future<void> start({TransferFormat? transferFormat});
  Future<void> send(Object? data);
  Future<void>? stop({Object? error});

  OnReceive? onreceive;
  OnClose? onclose;

  IConnection() : features = ConnectionFeatures(null);
}
