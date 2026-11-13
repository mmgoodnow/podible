import Foundation

struct AcquisitionChanges {
    private let client: PodibleClient
    private let session: URLSession

    // The server pings every 20s. URLSession's timeout measures time without data.
    private static let defaultSession = PodibleClient.makeSession(timeout: 50)

    init(client: PodibleClient, session: URLSession? = nil) {
        self.client = client
        self.session = session ?? Self.defaultSession
    }

    func observe(_ refresh: @MainActor () async throws -> Void) async throws {
        try await observe(path: "/api/trpc/acquisition.changes", as: Change.self) { event in
            if event?.type == "change" { try await refresh() }
        }
    }

    func observeTransfer(_ scope: TransferScope, receive: @MainActor (TransferEvent.SampleValue?) -> Void) async throws {
        let input = String(decoding: try JSONEncoder().encode(scope), as: UTF8.self)
        try await observe(path: "/api/trpc/acquisition.transfer", query: [URLQueryItem(name: "input", value: input)],
                          as: TransferEvent.self) { receive($0?.sample) }
    }

    private struct Change: Decodable, Sendable { let type: String }

    private func observe<Value: Decodable & Sendable>(path: String, query: [URLQueryItem] = [], as: Value.Type,
                                                      receive: @MainActor (Value?) async throws -> Void) async throws {
        var request = try client.authenticatedRequest(path: path, query: query)
        request.setValue("text/event-stream", forHTTPHeaderField: "Accept")
        let streamRequest = request
        let (changes, sink) = AsyncThrowingStream<Value?, Error>.makeStream(bufferingPolicy: .bufferingNewest(1))

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { await listen(streamRequest, sink: sink) }
            defer { group.cancelAll() }
            for try await value in changes {
                try Task.checkCancellation()
                try await receive(value)
            }
        }
    }

    private func listen<Value: Decodable & Sendable>(_ request: URLRequest, sink: AsyncThrowingStream<Value?, Error>.Continuation) async {
        var backoff = 1
        do {
            while true {
                try Task.checkCancellation()
                let started = ContinuousClock.now
                do {
                    try await read(request, sink: sink)
                } catch {
                    try Task.checkCancellation()
                    guard Self.isRetryable(error) else { throw error }
                }
                if case .terminated = sink.yield(nil) { return }
                if ContinuousClock.now - started >= .seconds(20) { backoff = 1 }
                try await Task.sleep(for: .seconds(backoff))
                backoff = min(backoff * 2, 30)
            }
        } catch {
            sink.finish(throwing: Task.isCancelled ? nil : error)
        }
    }

    private func read<Value: Decodable & Sendable>(_ request: URLRequest, sink: AsyncThrowingStream<Value?, Error>.Continuation) async throws {
        let (bytes, response) = try await session.bytes(for: request)
        let task = bytes.task
        defer { task.cancel() }
        guard let response = response as? HTTPURLResponse else { throw ClientError.invalidResponse }
        switch response.statusCode {
        case 200: break
        case 401: throw ClientError.unauthorized
        case 403: throw ClientError.rpc(-32003)
        case let code: throw ClientError.http(code)
        }
        guard response.mimeType?.lowercased() == "text/event-stream" else { throw ClientError.invalidResponse }

        try await withTaskCancellationHandler {
            var parser = AcquisitionEventParser<Value>()
            for try await byte in bytes {
                switch try parser.append(byte) {
                case .value(let value):
                    if case .terminated = sink.yield(value) { return }
                case .end: return
                case nil: break
                }
            }
        } onCancel: {
            task.cancel()
        }
    }

    private static func isRetryable(_ error: Error) -> Bool {
        switch error {
        case ClientError.http(408), ClientError.http(429), ClientError.http(500...599): true
        case let error as URLError:
            switch error.code {
            case .timedOut, .cannotFindHost, .cannotConnectToHost, .networkConnectionLost,
                 .dnsLookupFailed, .notConnectedToInternet, .resourceUnavailable: true
            default: false
            }
        default: false
        }
    }
}

private struct AcquisitionEventParser<Value: Decodable> {
    enum Event { case value(Value), end }
    private struct Failure: Decodable {
        struct Details: Decodable { let code: String }
        let data: Details
    }

    private var line: [UInt8] = []
    private var event = ""
    private var data: [String] = []
    private var frameSize = 0
    private var afterCR = false

    mutating func append(_ byte: UInt8) throws -> Event? {
        defer { afterCR = byte == 13 }
        if byte == 10 && afterCR { return nil }
        frameSize += 1
        guard frameSize <= 64 * 1024 else { throw ClientError.invalidResponse }
        guard byte == 10 || byte == 13 else { line.append(byte); return nil }
        if line.isEmpty {
            defer {
                event = ""
                data.removeAll(keepingCapacity: true)
                frameSize = 0
            }
            return try dispatch()
        }

        let text = String(decoding: line, as: UTF8.self)
        line.removeAll(keepingCapacity: true)
        let colon = text.firstIndex(of: ":") ?? text.endIndex
        var value = colon == text.endIndex ? Substring() : text[text.index(after: colon)...]
        if value.first == " " { value.removeFirst() }
        switch text[..<colon] {
        case "event": event = String(value)
        case "data": data.append(String(value))
        default: break
        }
        return nil
    }

    private func dispatch() throws -> Event? {
        guard !data.isEmpty else { return nil }
        switch event {
        case "", "message":
            return (try? JSONDecoder().decode(Value.self, from: Data(data.joined(separator: "\n").utf8))).map(Event.value)
        case "return": return .end
        case "serialized-error":
            let failure = try? JSONDecoder().decode(Failure.self, from: Data(data.joined(separator: "\n").utf8))
            switch failure?.data.code {
            case "UNAUTHORIZED": throw ClientError.unauthorized
            case "FORBIDDEN": throw ClientError.rpc(-32003)
            case "INTERNAL_SERVER_ERROR", "BAD_GATEWAY", "SERVICE_UNAVAILABLE", "GATEWAY_TIMEOUT",
                 "TOO_MANY_REQUESTS", "TIMEOUT": return .end
            default: throw ClientError.invalidResponse
            }
        default: return nil
        }
    }
}
