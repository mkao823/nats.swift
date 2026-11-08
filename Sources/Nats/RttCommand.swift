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

import Foundation
import NIOCore

internal final class RttCommand: Sendable {
    let startTime = DispatchTime.now()
    let promise: EventLoopPromise<TimeInterval>?

    static func makeFrom(channel: Channel?) -> RttCommand {
        RttCommand(promise: channel?.eventLoop.makePromise(of: TimeInterval.self))
    }

    static func make(on eventLoop: EventLoop) -> RttCommand {
        RttCommand(promise: eventLoop.makePromise(of: TimeInterval.self))
    }

    private init(promise: EventLoopPromise<TimeInterval>?) {
        self.promise = promise
    }

    func setRoundTripTime() {
        let now = DispatchTime.now()
        let nanoTime = now.uptimeNanoseconds - startTime.uptimeNanoseconds
        let rtt = TimeInterval(nanoTime) / 1_000_000_000  // Convert nanos to seconds
        promise?.succeed(rtt)
    }

    /// Fails a ping that will never get its PONG. An `EventLoopPromise` dropped without being
    /// completed traps in debug builds ("leaking promise"), so every queued ping must end here or
    /// in ``setRoundTripTime()``.
    func fail(_ error: Error) {
        promise?.fail(error)
    }

    func getRoundTripTime() async throws -> TimeInterval {
        try await promise?.futureResult.get() ?? 0
    }
}
