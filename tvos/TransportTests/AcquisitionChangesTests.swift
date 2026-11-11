import Foundation
import os
import Testing
@testable import PodibleTVTransport

@Suite(.timeLimit(.minutes(1)))
struct AcquisitionChangesTests {
    @Test(arguments: [false, true])
    func initialLiveAndReconnectChangesRefresh(serializedError: Bool) async throws {
        let fixture = Fixture()
        let (refreshes, refreshed) = AsyncStream<Void>.makeStream()
        var refresh = refreshes.makeAsyncIterator()
        var requests = fixture.requests.makeAsyncIterator()
        var stopped = fixture.stopped.makeAsyncIterator()
        let observation = Task { try await fixture.subscriber.observe { refreshed.yield(()) } }
        defer { observation.cancel() }

        let first = try #require(await requests.next()).protocolInstance
        #expect(first.request.url?.path == "/api/trpc/acquisition.changes")
        #expect(first.request.url?.query == nil)
        #expect(first.request.httpMethod == "GET")
        #expect(first.request.value(forHTTPHeaderField: "Authorization") == "Bearer test-token")
        #expect(first.request.value(forHTTPHeaderField: "Accept") == "text/event-stream")
        first.respond()
        first.send("event: connected\ndata: {}\n\n: keepalive\n\nevent: ping\ndata: \n\n")
        first.send("data: {\"type\":\"not-a-change\"}\n\ndata: {\"type\":\"change\"}")
        first.send("\r\n\r\n")
        #expect(await refresh.next() != nil)

        first.send("data: {\"type\":\"change\"}\n\n")
        #expect(await refresh.next() != nil)
        if serializedError {
            first.send("event: serialized-error\ndata: {\"data\":{\"code\":\"SERVICE_UNAVAILABLE\"}}\n\n")
        } else {
            first.complete()
        }

        let second = try #require(await requests.next()).protocolInstance
        #expect(second.request.value(forHTTPHeaderField: "Authorization") == "Bearer test-token")
        second.respond()
        second.send("data: {\"type\":\"change\"}\n\n")
        #expect(await refresh.next() != nil)

        observation.cancel()
        _ = await observation.result
        let firstStop = await stopped.next()
        let secondStop = firstStop == first.id ? await stopped.next() : firstStop
        #expect(secondStop == second.id)
        refreshed.finish()
        #expect(await refresh.next() == nil)
    }

    @Test(arguments: [401, 403, 200])
    func authorizationFailureStopsWithoutHydrating(status: Int) async throws {
        let fixture = Fixture()
        let observation = Task {
            try await fixture.subscriber.observe { Issue.record("Unauthorized stream refreshed detail") }
        }
        defer { observation.cancel() }
        var requests = fixture.requests.makeAsyncIterator()
        let stream = try #require(await requests.next()).protocolInstance
        stream.respond(status: status)
        if status == 200 {
            stream.send("event: connected\ndata: {}\n\n: comment\n\nevent: ping\ndata: \n\n")
            stream.send("event: serialized-error\ndata: {\"data\":{\"code\":\"UNAUTHORIZED\"}}\n\n")
        }

        switch await observation.result {
        case .failure(ClientError.unauthorized) where status != 403: break
        case .failure(ClientError.rpc(-32003)) where status == 403: break
        case let result: Issue.record("Unexpected authorization result: \(result)")
        }
    }

    @Test func cancellationBeforeHeadersClosesTheRequest() async throws {
        let fixture = Fixture()
        let observation = Task {
            try await fixture.subscriber.observe { Issue.record("Cancelled stream refreshed detail") }
        }
        defer { observation.cancel() }
        var requests = fixture.requests.makeAsyncIterator()
        var stopped = fixture.stopped.makeAsyncIterator()
        let stream = try #require(await requests.next()).protocolInstance
        observation.cancel()
        _ = await observation.result
        #expect(await stopped.next() == stream.id)
    }

    @Test func unauthorizedSnapshotClosesTheStream() async throws {
        let fixture = Fixture()
        let observation = Task {
            try await fixture.subscriber.observe { throw ClientError.unauthorized }
        }
        defer { observation.cancel() }
        var requests = fixture.requests.makeAsyncIterator()
        var stopped = fixture.stopped.makeAsyncIterator()
        let stream = try #require(await requests.next()).protocolInstance
        stream.respond()
        stream.send("data: {\"type\":\"change\"}\n\n")
        switch await observation.result {
        case .failure(ClientError.unauthorized): break
        case let result: Issue.record("Unexpected observation result: \(result)")
        }
        #expect(await stopped.next() == stream.id)
    }

    @Test func burstsLeaveOnlyOnePendingRefresh() async throws {
        let fixture = Fixture()
        let (refreshes, refreshed) = AsyncStream<Void>.makeStream()
        let (resume, release) = AsyncStream<Void>.makeStream()
        var refresh = refreshes.makeAsyncIterator()
        var requests = fixture.requests.makeAsyncIterator()
        var stopped = fixture.stopped.makeAsyncIterator()
        let observation = Task {
            try await fixture.subscriber.observe {
                refreshed.yield(())
                var wait = resume.makeAsyncIterator()
                _ = await wait.next()
            }
        }
        defer { observation.cancel() }
        let stream = try #require(await requests.next()).protocolInstance
        stream.respond()
        let change = "data: {\"type\":\"change\"}\n\n"
        stream.send(change)
        #expect(await refresh.next() != nil)

        stream.send(String(repeating: change, count: 30))
        stream.send("event: serialized-error\ndata: {\"data\":{\"code\":\"FORBIDDEN\"}}\n\n")
        #expect(await stopped.next() == stream.id)
        release.finish()
        _ = await observation.result
        refreshed.finish()
        #expect(await refresh.next() != nil)
        #expect(await refresh.next() == nil)
    }
}

private final class Fixture: Sendable {
    let requests: AsyncStream<ScriptedSSE.Handle>
    let stopped: AsyncStream<UUID>
    let subscriber: AcquisitionChanges
    private let session: URLSession
    private let host = "\(UUID().uuidString.lowercased()).invalid"

    init() {
        let (requests, request) = AsyncStream<ScriptedSSE.Handle>.makeStream()
        let (stopped, stop) = AsyncStream<UUID>.makeStream()
        self.requests = requests
        self.stopped = stopped
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [ScriptedSSE.self]
        let client = PodibleClient(baseURL: URL(string: "https://\(host)")!, token: "test-token")
        session = URLSession(configuration: configuration)
        subscriber = AcquisitionChanges(client: client, session: session)
        ScriptedSSE.scripts.withLock { $0[host] = (request, stop) }
    }

    deinit {
        session.invalidateAndCancel()
        ScriptedSSE.scripts.withLock { $0[host] = nil }
    }
}

private final class ScriptedSSE: URLProtocol, @unchecked Sendable {
    struct Handle: @unchecked Sendable { let protocolInstance: ScriptedSSE }
    typealias Script = (AsyncStream<Handle>.Continuation, AsyncStream<UUID>.Continuation)
    static let scripts = OSAllocatedUnfairLock(initialState: [String: Script]())
    let id = UUID()

    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() { script?.0.yield(Handle(protocolInstance: self)) }
    override func stopLoading() { script?.1.yield(id) }

    private var script: Script? {
        Self.scripts.withLock { $0[request.url!.host!] }
    }

    func respond(status: Int = 200) {
        let response = HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: "HTTP/1.1",
                                       headerFields: ["Content-Type": "text/event-stream; charset=utf-8"])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    }

    func send(_ chunk: String) { client?.urlProtocol(self, didLoad: Data(chunk.utf8)) }
    func complete() { client?.urlProtocolDidFinishLoading(self) }
}
