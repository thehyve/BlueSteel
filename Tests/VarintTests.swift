//
//  VarintTests.swift
//  BlueSteelTests
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
import BlueSteel

class VarintTests: XCTestCase {
    
    override func setUp() {
        super.setUp()
    }
    
    override func tearDown() {
        super.tearDown()
    }

    func testToUInt() {
        let expected = UInt64(arc4random())
        let testvarint = Varint(fromValue: expected)
        let val = testvarint.toUInt64()
        XCTAssertEqual(val, expected, "Expected -1. Got\(val)")
    }

    func testEncodeZigZag() {
        let val = UInt64(toZigZag: Int64(Int32.max))
        XCTAssertEqual(val, UInt64(UInt32.max) - UInt64(1), "\(val)")
    }

    func testDecodeZigZag() {
        let val = Int64(fromZigZag: UInt64.max)
        XCTAssertEqual(val, Int64.min, "\(val)")
    }
}
