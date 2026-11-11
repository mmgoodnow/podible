import XCTest

final class TrailerPlaybackTests: XCTestCase {
    @MainActor
    func testApprovedPlexTrailerAdvancesInNativePlayerAndReturnsToDetails() throws {
        let app = try launchFixture()
        let remote = XCUIRemote.shared
        let poster = app.buttons["poster.movie:tmdb:10378"]
        XCTAssertTrue(poster.waitForExistence(timeout: 10))
        if app.buttons["nav.Home"].hasFocus { remote.press(.right) }
        XCTAssertTrue(poster.hasFocus)
        remote.press(.select)

        let button = app.buttons["trailer.watch"]
        XCTAssertTrue(button.waitForExistence(timeout: 12))
        XCTAssertEqual(button.label, "Watch Trailer")
        let focused = XCTNSPredicateExpectation(predicate: NSPredicate(format: "hasFocus == YES"), object: button)
        XCTAssertEqual(XCTWaiter.wait(for: [focused], timeout: 4), .completed)
        remote.press(.select)

        let started = app.staticTexts["trailer.started"]
        XCTAssertTrue(started.waitForExistence(timeout: 20), "AVKit never advanced the trailer timeline")
        let attachment = XCTAttachment(screenshot: XCUIScreen.main.screenshot())
        attachment.name = "native-plex-trailer"
        attachment.lifetime = .keepAlways
        add(attachment)
        remote.press(.menu)
        XCTAssertTrue(button.waitForExistence(timeout: 5))
        XCTAssertFalse(started.exists)
        let restored = XCTNSPredicateExpectation(predicate: NSPredicate(format: "hasFocus == YES"), object: button)
        XCTAssertEqual(XCTWaiter.wait(for: [restored], timeout: 4), .completed)
    }

    @MainActor
    func testSeriesTrailerReturnsToDetailsOnCompletion() throws {
        let app = try launchFixture()
        let remote = XCUIRemote.shared
        let first = app.buttons["poster.movie:tmdb:10378"]
        XCTAssertTrue(first.waitForExistence(timeout: 10))
        if app.buttons["nav.Home"].hasFocus { remote.press(.right) }
        XCTAssertTrue(first.hasFocus)
        let show = app.buttons["poster.show:tmdb:2316"]
        for _ in 0..<3 where !show.hasFocus { remote.press(.right) }
        XCTAssertTrue(show.hasFocus)
        remote.press(.select)
        let button = app.buttons["trailer.watch"]
        XCTAssertTrue(button.waitForExistence(timeout: 12))
        let focused = XCTNSPredicateExpectation(predicate: NSPredicate(format: "hasFocus == YES"), object: button)
        XCTAssertEqual(XCTWaiter.wait(for: [focused], timeout: 4), .completed)
        remote.press(.select)
        let started = app.staticTexts["trailer.started"]
        XCTAssertTrue(started.waitForExistence(timeout: 20))
        let ended = XCTNSPredicateExpectation(predicate: NSPredicate(format: "exists == NO"), object: started)
        XCTAssertEqual(XCTWaiter.wait(for: [ended], timeout: 20), .completed)
        XCTAssertTrue(app.staticTexts["Seasons"].waitForExistence(timeout: 20))
    }

    @MainActor
    func testUnapprovedTitleDoesNotOfferPlayback() throws {
        let app = try launchFixture()
        let remote = XCUIRemote.shared
        let first = app.buttons["poster.movie:tmdb:10378"]
        XCTAssertTrue(first.waitForExistence(timeout: 10))
        if app.buttons["nav.Home"].hasFocus { remote.press(.right) }
        XCTAssertTrue(first.hasFocus)
        let target = app.buttons["poster.movie:tmdb:550"]
        for _ in 0..<3 where !target.hasFocus { remote.press(.right) }
        XCTAssertTrue(target.hasFocus)
        remote.press(.select)

        XCTAssertTrue(app.staticTexts["In Library"].waitForExistence(timeout: 8))
        XCTAssertFalse(app.buttons["trailer.watch"].waitForExistence(timeout: 3))
    }

    @MainActor
    private func launchFixture() throws -> XCUIApplication {
        continueAfterFailure = false
        guard let url = ProcessInfo.processInfo.environment["PODIBLE_TEST_TRAILER_URL"] else {
            throw XCTSkip("Start the loopback Plex trailer fixture to run this test.")
        }
        let app = XCUIApplication()
        app.launchEnvironment["PODIBLE_TEST_TRAILER_URL"] = url
        app.launchEnvironment["PODIBLE_TEST_TRAILER_TOKEN"] = "trailer-fixture-viewer"
        app.launch()
        return app
    }
}
