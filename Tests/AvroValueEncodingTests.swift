//
//  AvroValueEncodingTests.swift
//  BlueSteel
//
//  Created by Nikita Korchagin on 29/09/16.
//  Copyright (c) 2016 Gilt. All rights reserved.
//

import XCTest
import BlueSteel

extension Data {
    private static let hexAlphabet = "0123456789abcdef".unicodeScalars.map { $0 }

    public func hexEncodedString() -> String {
        return String(self.reduce(into: "".unicodeScalars, { (result, value) in
            result.append(Data.hexAlphabet[Int(value / 16)])
            result.append(Data.hexAlphabet[Int(value % 16)])
        }))
    }
}

class AvroValueEncodingTests: XCTestCase {
    let schema: Schema = .avroRecord("A", [
        .avroField("a", .avroInt, nil),
        .avroField("b", .avroEnum("B", ["opt1", "opt2"]), nil),
        .avroField("c", .avroLong, .avroLong(1)),
        .avroField("d", .avroMap(.avroBytes), nil),
        .avroField("f", .avroString, nil),
        .avroField("g", .avroUnion([.avroString, .avroInt]), nil),
        .avroField("h", .avroBytes, Data(bytes: [0xff]).toAvro()),
        .avroField("i", .avroFloat, nil)
        ])

    func testEnumEncoding() {
        let value: AvroValue = [
            "a": 64,
            "b": "opt1",
            "d": ["e": "ab"],
            "f": "\na",
            "g": ["int": 2],
            "i": 0.0,
        ]

        let encoder = GenericAvroEncoder(encoding: .binary)
        guard let encodedCorrectValue = try? encoder.encode(value, as: schema) else {
            XCTFail()
            return
        }
        XCTAssertEqual(encodedCorrectValue.hexEncodedString(), Data(bytes: [
            0x80, 0x01, // a
            0x0, // b
            0x2, // c
            0x2, // d count
            0x2, 0x65, // d key
            0x4, 0x61, 0x62, // d value
            0x00, // d end
            0x4, 0xa, 0x61, // "\na
            0x2, 0x4, // union int, value 2
            0x2, 0xff, // default h
            0x0, 0x0, 0x0, 0x0, // 0 float
            ]).hexEncodedString())
    }

    func testJsonEncoding() {
        let value: AvroValue = [
            "a": 1,
            "b": "opt1",
            "d": ["e": Data(bytes: [0xff])],
            "f": "\na\"\\",
            "g": ["int": 2],
            "i": 5.0,
        ]

        let encoder = GenericAvroEncoder(encoding: .json)

        guard let encodedCorrectValue = try? encoder.encode(value, as: schema), let stringValue = String(data: encodedCorrectValue, encoding: .utf8) else {
            XCTFail()
            return
        }
        XCTAssertEqual(stringValue, "{\"a\":1,\"b\":\"opt1\",\"c\":1,\"d\":{\"e\":\"ÿ\"},\"f\":\"\\u000Aa\\\"\\\\\",\"g\":{\"int\":2},\"h\":\"ÿ\",\"i\":5.0}")

        let decoder = JsonAvroDecoder()
        let decodedValue = try! decoder.decode(encodedCorrectValue, as: schema)
        let avroValue = try! AvroValue(value: value, as: schema)

        guard let fields = avroValue.map else {
            XCTFail()
            return
        }
        guard let decodedData = fields["d"]?.map?["e"]?.bytes else {
            XCTFail()
            return
        }
        XCTAssertEqual(decodedData.hexEncodedString(), "ff")

        XCTAssertEqual(decodedValue, avroValue)
    }
}
