// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import NIOCore
import NIOPosix
import XCTest

@testable import Nats

/// A ping waiting for its PONG holds an `EventLoopPromise`. Dropping one without completing it
/// traps in debug builds ("leaking promise" in `EventLoopFuture.deinit`), which crashed apps that
/// tore a connection down with a keepalive ping still outstanding. Every queued ping must be
/// failed once its PONG can no longer arrive.
class PendingPingTests: XCTestCase {

    private var group: MultiThreadedEventLoopGroup!

    override func setUp() {
        group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
    }

    override func tearDown() {
        try? group.syncShutdownGracefully()
    }

    private func makeHandler() -> ConnectionHandler {
        ConnectionHandler(
            urls: [URL(string: "nats://127.0.0.1:1")!],
            reconnectWait: 2.0,
            maxReconnects: nil,
            retainServersOrder: false,
            ignoreDiscoveredServers: false,
            pingInterval: 60.0,
            auth: nil,
            requireTls: false,
            tlsFirst: false,
            clientCertificate: nil,
            clientKey: nil,
            rootCertificate: nil,
            retryOnFailedConnect: false
        )
    }

    private func assertConnectionClosed(
        _ ping: RttCommand, file: StaticString = #filePath, line: UInt = #line
    ) async {
        do {
            _ = try await ping.getRoundTripTime()
            XCTFail("ping should have failed", file: file, line: line)
        } catch NatsError.ClientError.connectionClosed {
        } catch {
            XCTFail("unexpected error: \(error)", file: file, line: line)
        }
    }

    func testDrainEmptiesQueueInOrder() {
        let queue = ConcurrentQueue<Int>()
        queue.enqueue(1)
        queue.enqueue(2)
        XCTAssertEqual(queue.drain(), [1, 2])
        XCTAssertNil(queue.dequeue())
    }

    /// The crash on the phone: the handler is released with a keepalive ping still queued and
    /// nothing else holding it. Before the fix this test process trapped in
    /// `EventLoopFuture.deinit` ("leaking promise"); reaching the end is the pass.
    func testReleasingHandlerWithUnansweredPingDoesNotTrap() {
        var handler: ConnectionHandler? = makeHandler()
        handler?.pingQueue.enqueue(RttCommand.make(on: group.next()))
        handler = nil
        XCTAssertNil(handler)
    }

    func testFailPendingPingsFailsEveryQueuedPing() async {
        let handler = makeHandler()
        let first = RttCommand.make(on: group.next())
        let second = RttCommand.make(on: group.next())
        handler.pingQueue.enqueue(first)
        handler.pingQueue.enqueue(second)
        handler.failPendingPings()
        await assertConnectionClosed(first)
        await assertConnectionClosed(second)
        XCTAssertNil(handler.pingQueue.dequeue())
    }

    /// A ping that could not be written gets no PONG; it fails with the write error.
    func testUnsentPingFails() async {
        let handler = makeHandler()  // never connected: no batch buffer, so the write throws
        let ping = RttCommand.make(on: group.next())
        await handler.sendPing(ping)
        do {
            _ = try await ping.getRoundTripTime()
            XCTFail("ping should have failed")
        } catch NatsError.ClientError.invalidConnection {
        } catch {
            XCTFail("unexpected error: \(error)")
        }
        XCTAssertNil(handler.pingQueue.dequeue())
    }

    func testClosingClientFailsQueuedPings() async throws {
        let handler = makeHandler()
        let ping = RttCommand.make(on: group.next())
        handler.pingQueue.enqueue(ping)
        try await handler.close()
        await assertConnectionClosed(ping)
    }
}
