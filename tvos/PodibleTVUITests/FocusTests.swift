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
        let browse = app.buttons["nav.Browse"]
        let search = app.buttons["nav.Search"]

        XCTAssertTrue(firstPoster.waitForExistence(timeout: 8))
        if browse.hasFocus { remote.press(.down) }
        assertFocused(firstPoster)
        capture("02-browse-movies")

        remote.press(.up)
        assertFocused(browse)
        remote.press(.right)
        assertFocused(search)
        capture("03-navigation-focus")

        remote.press(.select)
        let query = app.textFields["search.query"]
        XCTAssertTrue(query.waitForExistence(timeout: 5))
        capture("04-search")

        remote.press(.down)
        assertFocused(query)
        capture("04b-search-input-focus")
        remote.press(.up)
        assertFocused(search)
        remote.press(.left)
        assertFocused(browse)
        remote.press(.select)
        if browse.hasFocus { remote.press(.down) }
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
        capture("08-request-sent")
        remote.press(.menu)
        let showPoster = app.buttons["poster.after"]
        XCTAssertTrue(showPoster.waitForExistence(timeout: 5))
        assertFocused(showPoster)
        capture("09-back-to-shelf")
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
