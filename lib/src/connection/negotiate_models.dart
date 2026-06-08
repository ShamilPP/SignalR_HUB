import 'package:equatable/equatable.dart';

import '../core/itransport.dart';
import '../shared/utils.dart';

enum ConnectionState {
  connecting,
  connected,
  disconnected,
  disconnecting,
}

class NegotiateResponse {
  String? connectionId;
  String? connectionToken;
  int? negotiateVersion;
  List<AvailableTransport>? availableTransports;
  final String? url;
  final String? accessToken;
  final String? error;

  bool get hasConnectionId => !isStringEmpty(connectionId);

  bool get hasConnectionTokenId => !isStringEmpty(connectionToken);

  bool get hasNegotiateVersion => !isIntEmpty(negotiateVersion);

  bool get isConnectionResponse =>
      hasConnectionId && !isListEmpty(availableTransports);

  bool get isRedirectResponse => !isStringEmpty(url);

  bool get isErrorResponse => !isStringEmpty(error);

  bool get hasAccessToken => !isStringEmpty(accessToken);

  NegotiateResponse(
    this.connectionId,
    this.connectionToken,
    this.negotiateVersion,
    this.availableTransports,
    this.url,
    this.accessToken,
    this.error,
  );

  NegotiateResponse.fromJson(Map<String, dynamic> json)
      : connectionId = json['connectionId'],
        connectionToken = json['connectionToken'],
        negotiateVersion = json['negotiateVersion'],
        url = json['url'],
        accessToken = json['accessToken'],
        error = json['error'] {
    final out = <AvailableTransport>[];
    final List<dynamic>? transports = json['availableTransports'];
    if (transports != null) {
      for (var i = 0; i < transports.length; i++) {
        out.add(AvailableTransport.fromJson(transports[i]));
      }
    }
    availableTransports = out;
  }
}

class AvailableTransport extends Equatable {
  final HttpTransportType? transport;
  final List<TransferFormat> transferFormats;

  const AvailableTransport({
    this.transport,
    List<TransferFormat>? transferFormats,
  }) : transferFormats = transferFormats ?? const [];

  factory AvailableTransport.fromJson(Map<String, dynamic> json) {
    final parsedTransport = httpTransportTypeFromString(json['transport']);
    final formats = json['transferFormats'] as List<dynamic>?;
    final list = <TransferFormat>[];
    if (formats != null) {
      for (var i = 0; i < formats.length; i++) {
        list.add(getTransferFormatFromString(formats[i]));
      }
    }
    return AvailableTransport(
      transport: parsedTransport,
      transferFormats: list,
    );
  }

  @override
  List<Object?> get props => [transport, transferFormats];
}
