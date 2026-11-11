import XCTest

final class FocusTests: XCTestCase {
    @MainActor
    func testBrowseToSearchAndBack() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--demo"]
        app.launch()
        let remote = XCUIRemote.shared
        let firstPoster = app.buttons["poster.marrow"]
        let browse = app.buttons["nav.Home"]
        let search = app.buttons["nav.Search"]

        XCTAssertTrue(firstPoster.waitForExistence(timeout: 8))
        if browse.hasFocus { remote.press(.right) }
        assertFocused(firstPoster)
        capture("02-browse-movies")

        remote.press(.left)
        let sidebar = app.buttons.matching(NSPredicate(format: "identifier BEGINSWITH %@", "nav."))
        XCTAssertTrue(sidebar.allElementsBoundByIndex.contains(where: \.hasFocus))
        for _ in 0..<6 where !browse.hasFocus { remote.press(.up) }
        remote.press(.down)
        assertFocused(search)
        capture("03-navigation-focus")

        remote.press(.select)
        let query = app.textFields["search.query"]
        XCTAssertTrue(query.waitForExistence(timeout: 5))
        capture("04-search")

        assertFocused(query)
        capture("04b-search-input-focus")
        remote.press(.left)
        XCTAssertTrue(sidebar.allElementsBoundByIndex.contains(where: \.hasFocus))
        for _ in 0..<6 where !browse.hasFocus { remote.press(.up) }
        assertFocused(browse)
        remote.press(.select)
        if browse.hasFocus { remote.press(.right) }
        assertFocused(firstPoster)
        remote.press(.right)
        assertFocused(app.buttons["poster.after"])
        capture("05-browse-tv")

        remote.press(.select)
        XCTAssertTrue(app.staticTexts["Seasons"].waitForExistence(timeout: 5))
        capture("06-show-details")
        let firstSeason = app.buttons["season.1"]
        assertFocused(firstSeason)
        remote.press(.select)
        XCTAssertEqual(firstSeason.value as? String, "Selected")
        capture("07-season-selected")
        remote.press(.down)
        assertFocused(app.buttons["season.2"])
        remote.press(.down)
        assertFocused(app.buttons["season.request"])
        remote.press(.select)
        XCTAssertTrue(app.staticTexts["Request sent"].waitForExistence(timeout: 5))
        XCTAssertEqual(app.staticTexts["request.progress.status"].label, "Request received")
        capture("08-request-sent")
        remote.press(.menu)
        let showPoster = app.buttons["poster.after"]
        XCTAssertTrue(showPoster.waitForExistence(timeout: 5))
        assertFocused(showPoster)
        capture("09-back-to-shelf")
    }

    @MainActor
    func testRequestOnlyOneSpecial() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--demo"]
        app.launch()
        let remote = XCUIRemote.shared
        let firstPoster = app.buttons["poster.marrow"]
        XCTAssertTrue(firstPoster.waitForExistence(timeout: 8))
        if app.buttons["nav.Home"].hasFocus { remote.press(.right) }
        assertFocused(firstPoster)
        for _ in 0..<4 { remote.press(.right) }
        assertFocused(app.buttons["poster.north"])
        remote.press(.select)

        let firstSeason = app.buttons["season.1"]
        XCTAssertTrue(firstSeason.waitForExistence(timeout: 5))
        assertFocused(firstSeason)
        for _ in 0..<3 { remote.press(.down) }
        let special = app.buttons["special.tvdb:8001"]
        let other = app.buttons["special.tvdb:8002"]
        assertFocused(special)
        remote.press(.select)
        XCTAssertEqual(special.value as? String, "Selected")
        XCTAssertEqual(other.value as? String, "Not selected")
        remote.press(.down)
        assertFocused(other)
        remote.press(.down)
        assertFocused(app.buttons["season.request"])
        remote.press(.select)
        XCTAssertTrue(app.staticTexts["Request sent"].waitForExistence(timeout: 5))
        XCTAssertFalse(special.exists)
        XCTAssertEqual(other.value as? String, "Not selected")
        XCTAssertEqual(firstSeason.value as? String, "Not selected")
    }

    @MainActor
    private func assertFocused(_ element: XCUIElement, file: StaticString = #filePath, line: UInt = #line) {
        let focused = XCTNSPredicateExpectation(predicate: NSPredicate(format: "hasFocus == YES"), object: element)
        XCTAssertEqual(XCTWaiter.wait(for: [focused], timeout: 4), .completed, file: file, line: line)
    }

    @MainActor
    private func capture(_ name: String) {
        let attachment = XCTAttachment(screenshot: XCUIScreen.main.screenshot())
        attachment.name = name
        attachment.lifetime = .keepAlways
        add(attachment)
    }
}
