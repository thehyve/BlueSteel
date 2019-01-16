//
//  AvroDecoderTests.swift
//  BlueSteelTests
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
import BlueSteel

class AvroDecoderTests: XCTestCase {
    var decoder: AvroDecoder!

    override func setUp() {
        super.setUp()
        decoder = BinaryAvroDecoder()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testDecodeInt() {
        let data = Data(bytes: [0x4, 0x96, 0xde, 0x87, 0x3, 0xcd, 0xcc, 0x4c, 0x40, 0x96, 0xde, 0x87, 0x3])
        let schema: Schema = .avroRecord("test", [
            .avroField("x", .avroInt, nil),
            .avroField("y", .avroInt, nil),
        ])
        let avroValue = try! decoder.decode(data, as: schema)
        guard let fields = avroValue.map else {
            XCTFail()
            return
        }
        
        guard let x = fields["x"]?.int else {
            XCTFail()
            return
        }
        XCTAssertEqual(x, 2, "Decode broken.")
        guard let y = fields["y"]?.int else {
            XCTFail()
            return
        }
        XCTAssertEqual(y, 3209099, "Decoder broken.")
    }

    func testDecodeLong() {
        XCTAssert(true, "Pass")
    }

    func testDecodeFloat() {
        XCTAssert(true, "Pass")
    }

    func testDecodeDouble() {
        XCTAssert(true, "Pass")
    }

    func testDecodeString() {
        XCTAssert(true, "Pass")
    }

}
