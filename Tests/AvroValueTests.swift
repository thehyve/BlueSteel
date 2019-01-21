//
//  AvroValueTests.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
@testable import BlueSteel

class AvroValueTests: XCTestCase {

    func testValidateName() {
        XCTAssertNotNil(try? Schema.validate(name: "a"))
        XCTAssertNil(try? Schema.validate(name: ""))
        XCTAssertNil(try? Schema.validate(name: "a[]"))
        XCTAssertNil(try? Schema.validate(name: "a.b"))
        XCTAssertNotNil(try? Schema.validate(name: "A9"))
        XCTAssertNotNil(try? Schema.validate(name: "A_9"))
        XCTAssertNil(try? Schema.validate(name: "9A"))
    }

    func testValidateNameSpace() {
        XCTAssertNotNil(try? Schema.validate(namespace: ""))
        XCTAssertNotNil(try? Schema.validate(namespace: "a"))
        XCTAssertNil(try? Schema.validate(namespace: "a[]"))
        XCTAssertNotNil(try? Schema.validate(namespace: "a.b"))
        XCTAssertNotNil(try? Schema.validate(namespace: "A9"))
        XCTAssertNotNil(try? Schema.validate(namespace: "A_9"))
        XCTAssertNil(try? Schema.validate(namespace: "9A"))
    }

    func testValidateFullName() {
        XCTAssertNil(try? Schema.validate(fullName: ""))
        XCTAssertNotNil(try? Schema.validate(fullName: "a"))
        XCTAssertNil(try? Schema.validate(fullName: "a[]"))
        XCTAssertNotNil(try? Schema.validate(fullName: "a.b"))
        XCTAssertNotNil(try? Schema.validate(fullName: "A9"))
        XCTAssertNotNil(try? Schema.validate(fullName: "A_9"))
        XCTAssertNil(try? Schema.validate(fullName: "9A"))
    }
}
