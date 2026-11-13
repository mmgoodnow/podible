import XCTest

final class FocusTests: XCTestCase {
    @MainActor
    func testExpandedBrowseRailsAndFloatingNavigation() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--demo", "--demo-rails"]
        app.launch()
        let remote = XCUIRemote.shared
        let first = app.buttons["poster.marrow"]
        let home = app.buttons["nav.Home"]
        let search = app.buttons["nav.Search"]
        XCTAssertTrue(first.waitForExistence(timeout: 8))
        if home.hasFocus { remote.press(.right) }
        assertFocused(first)
        let seventh = app.buttons["poster.rail-fixture-3"]
        XCTAssertTrue(seventh.exists, "The first rail shows a partial seventh card")
        capture("11-home-large-rails")
        let second = app.buttons["poster.isle"], third = app.buttons["poster.orchard"]
        let pitch = third.frame.minX - second.frame.minX
        let visibleCards = 1 + (app.frame.maxX - second.frame.minX) / pitch
        XCTAssertTrue((6.4...6.8).contains(visibleCards), "Expected about 6½ cards, got \(visibleCards)")
        XCTAssertTrue(app.staticTexts["TV Shows"].firstMatch.exists)
        XCTAssertGreaterThan(first.frame.minX, home.frame.maxX + 20)

        for _ in 0..<6 { remote.press(.right) }
        assertFocused(seventh)
        XCTAssertGreaterThan(seventh.frame.minX, home.frame.maxX + 15)
        XCTAssertLessThanOrEqual(seventh.frame.maxX, app.frame.maxX)
        for _ in 0..<6 { remote.press(.left) }
        assertFocused(first)
        XCTAssertGreaterThan(first.frame.minX, home.frame.maxX + 15)
        remote.press(.left)
        let sidebar = app.buttons.matching(NSPredicate(format: "identifier BEGINSWITH %@", "nav."))
        XCTAssertTrue(sidebar.allElementsBoundByIndex.contains(where: \.hasFocus))
        for _ in 0..<6 where !home.hasFocus { remote.press(.up) }
        assertFocused(home)
        capture("12-home-floating-navigation")
        remote.press(.down)
        assertFocused(search)
        remote.press(.select)
        XCTAssertTrue(app.searchFields["Search movies and TV"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.keyboards.firstMatch.exists)
    }

    @MainActor
    func testMovieDetailHero() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--demo"]
        app.launch()
        let remote = XCUIRemote.shared
        let poster = app.buttons["poster.marrow"]
        XCTAssertTrue(poster.waitForExistence(timeout: 8))
        if app.buttons["nav.Home"].hasFocus { remote.press(.right) }
        assertFocused(poster)
        remote.press(.select)
        XCTAssertTrue(app.buttons["Play Sample"].waitForExistence(timeout: 5))
        capture("10-movie-detail-hero")
    }

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
        let query = app.searchFields["Search movies and TV"]
        XCTAssertTrue(query.waitForExistence(timeout: 5))
        capture("04-search")
        XCTAssertTrue(app.staticTexts["Browse Titles"].exists)
        XCTAssertTrue(app.keyboards.firstMatch.exists && app.keys["a"].exists, "Search opens with the inline keyboard")
        for _ in 0..<12 where !sidebar.allElementsBoundByIndex.contains(where: \.hasFocus) { remote.press(.left) }
        if !sidebar.allElementsBoundByIndex.contains(where: \.hasFocus) { remote.press(.menu) }
        XCTAssertTrue(sidebar.allElementsBoundByIndex.contains(where: \.hasFocus), app.debugDescription)
        for _ in 0..<6 where !browse.hasFocus { remote.press(.up) }
        assertFocused(browse)
        remote.press(.select)
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

        for _ in 0..<3 where !sidebar.allElementsBoundByIndex.contains(where: \.hasFocus) { remote.press(.left) }
        for _ in 0..<6 where !browse.hasFocus { remote.press(.up) }
        assertFocused(browse)
        let requests = app.buttons["nav.Requests"]
        for _ in 0..<6 where !requests.hasFocus { remote.press(.down) }
        assertFocused(requests)
        remote.press(.select)
        assertFocused(app.buttons["requests.title.after"])
    }

    @MainActor
    func testSidebarEntersSettingsAndEmptyRequests() {
        continueAfterFailure = false
        let app = XCUIApplication()
        app.launchArguments = ["--demo"]
        app.launch()
        let remote = XCUIRemote.shared
        let firstPoster = app.buttons["poster.marrow"]
        let sidebar = app.buttons.matching(NSPredicate(format: "identifier BEGINSWITH %@", "nav."))
        XCTAssertTrue(firstPoster.waitForExistence(timeout: 8))
        if firstPoster.hasFocus { remote.press(.left) }
        XCTAssertTrue(sidebar.allElementsBoundByIndex.contains(where: \.hasFocus))

        let home = app.buttons["nav.Home"]
        for _ in 0..<6 where !home.hasFocus { remote.press(.up) }
        assertFocused(home)
        let requests = app.buttons["nav.Requests"]
        for _ in 0..<6 where !requests.hasFocus { remote.press(.down) }
        assertFocused(requests)
        remote.press(.select)
        assertFocused(app.buttons["Search for a title"])
        remote.press(.left)
        XCTAssertTrue(sidebar.allElementsBoundByIndex.contains(where: \.hasFocus))
        remote.press(.right)
        assertFocused(app.buttons["Search for a title"])
        remote.press(.left)
        XCTAssertTrue(sidebar.allElementsBoundByIndex.contains(where: \.hasFocus))

        let settings = app.buttons["nav.Settings"]
        for _ in 0..<6 where !settings.hasFocus { remote.press(.down) }
        assertFocused(settings)
        remote.press(.select)
        let server = app.textFields["device-sign-in.server"]
        assertFocused(server)
        remote.press(.left)
        XCTAssertTrue(sidebar.allElementsBoundByIndex.contains(where: \.hasFocus))
        remote.press(.right)
        XCTAssertTrue(server.hasFocus || app.buttons["Sign in with phone"].hasFocus, app.debugDescription)
        if !server.hasFocus { remote.press(.up) }
        assertFocused(server)
        remote.press(.down)
        assertFocused(app.buttons["Sign in with phone"])
        capture("13-settings-content-focus")
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
        XCTAssertEqual(XCTWaiter.wait(for: [focused], timeout: 4), .completed,
                       "Expected focus on \(element)", file: file, line: line)
    }

    @MainActor
    private func capture(_ name: String) {
        let attachment = XCTAttachment(screenshot: XCUIScreen.main.screenshot())
        attachment.name = name
        attachment.lifetime = .keepAlways
        add(attachment)
    }
}
