//
//  VarintTests.swift
//  BlueSteelTests
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
@testable import BlueSteel

class ZigZagIntTests: XCTestCase {

    func testToUIntExtremes() {
        for value in [-1, 0, 1, Int64.max, Int64.min] {
            testToUInt(forValue: value)
        }
    }

    func testToUIntRandom() {
        for _ in 0 ..< 100 {
            testToUInt(forValue: Int64(arc4random()) + Int64(Int32.min))
        }
    }

    private func testToUInt(forValue expected: Int64) {
        let testvarint = ZigZagInt(value: expected)
        let val = testvarint.value
        XCTAssertEqual(val, expected, "Expected \(expected). Got\(val)")
    }
}
