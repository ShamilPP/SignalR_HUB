import 'package:signalr_hub/text_message_format.dart';
import 'package:signalr_hub/ihub_protocol.dart';
import 'package:signalr_hub/json_hub_protocol.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:collection/collection.dart';
import 'package:logging/logging.dart';

Function deepEq = const DeepCollectionEquality().equals;

void main() {
  // Common

  group('Json hub protocol -> ', () {
    final headers = MessageHeaders();
    headers.setHeaderValue("foo", "bar");
    for (var e in [
      InvocationMessage(
        target: "myMethod",
        arguments: [
          42,
          true,
          "test",
          ["x1", "y2"],
        ],
        streamIds: [],
      ),
      InvocationMessage(
        target: "myMethod",
        arguments: [
          42,
          true,
          52.64, // with float
          "test",
          ["x1", "y2"],
        ],
        invocationId: "123", // with invocation id
        streamIds: [],
      ),
      InvocationMessage(
        target: "myMethod",
        headers: headers, //with headers
        arguments: [
          42,
          true,
          "test",
          ["x1", "y2"],
        ],
        invocationId: "123",
      )
    ]) {
      test('can write/read non-blocking Invocation message -> ', () {
        final invocation = e;
        final protocol = JsonHubProtocol();
        final writtenMsg = protocol.writeMessage(invocation);
        final parsedMessages =
            protocol.parseMessages(writtenMsg, Logger("JsonHubProtocol"));

        final equalityCheck =
            deepEq(parsedMessages.toString(), ([invocation]).toString());
        expect(equalityCheck, true);
      });
    }

    for (var e in ([
      [
        TextMessageFormat.write(
            '{"type":3, "invocationId": "abc", "error": "Err", "result": null, "headers": {}}'),
        CompletionMessage(
          error: "Err",
          headers: MessageHeaders(),
          invocationId: "abc",
          result: null,
        )
      ],
      [
        TextMessageFormat.write(
            '{"type":3, "invocationId": "abc", "result": "OK", "headers": {}}'),
        CompletionMessage(
          headers: MessageHeaders(),
          invocationId: "abc",
          result: "OK",
        )
      ],
      [
        TextMessageFormat.write(
            '{"type":3, "invocationId": "abc", "result": null, "headers": {}}'),
        CompletionMessage(
          headers: MessageHeaders(),
          invocationId: "abc",
          result: null,
        )
      ],
      [
        TextMessageFormat.write(
            '{"type":3, "invocationId": "abc", "result": null, "headers": {}, "extraParameter":"value"}'),
        CompletionMessage(
          headers: MessageHeaders(),
          invocationId: "abc",
          result: null,
        )
      ],
    ])) {
      test('Completion message -> ', () {
        final protocol = JsonHubProtocol();
        final payload = e[0];
        final expectedMessage = e[1];
        final parsedMessages =
            protocol.parseMessages(payload, Logger("JsonHubProtocol"));
        final equalityCheck =
            deepEq(parsedMessages.toString(), ([expectedMessage]).toString());
        expect(equalityCheck, true);
      });
    }

    for (var e in [
      [
        TextMessageFormat.write(
            '{"type":2, "invocationId": "abc", "headers": {}, "item": 8}'),
        StreamItemMessage(
          headers: MessageHeaders(),
          invocationId: "abc",
          item: 8,
        )
      ],
      [
        TextMessageFormat.write(
            '{"type":2, "invocationId": "abc", "headers": {}, "item": 1648135922951}'),
        StreamItemMessage(
            headers: MessageHeaders(),
            invocationId: "abc",
            item: DateTime.parse('2022-03-24T15:32:02.951Z')
                .millisecondsSinceEpoch)
      ],
      [
        TextMessageFormat.write(
            '{"type":2, "invocationId": "abc", "headers": {"foo": "bar"}, "item": 8}'),
        StreamItemMessage(
          invocationId: "abc",
          item: 8,
          headers: headers,
        )
      ],
      [TextMessageFormat.write('{"type":6}'), PingMessage()]
    ]) {
      test('StreamItem message -> ', () {
        final protocol = JsonHubProtocol();
        final payload = e[0];
        final expectedMessage = e[1];
        final parsedMessages =
            protocol.parseMessages(payload, Logger("JsonHubProtocol"));

        expect(parsedMessages.toString(), [expectedMessage].toString());
      });
    }
  });
}
