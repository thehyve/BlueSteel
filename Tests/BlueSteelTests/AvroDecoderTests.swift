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
        let data = Data([0x4, 0x96, 0xde, 0x87, 0x3, 0xcd, 0xcc, 0x4c, 0x40, 0x96, 0xde, 0x87, 0x3])
        let schema: Schema = .avroRecord(name: "test", fields: [
            Schema.Field(name: "x", schema: .avroInt),
            Schema.Field(name: "y", schema: .avroInt),
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

    func testStringValue() {
        let avroBytes = Data([0x06, 0x66, 0x6f, 0x6f])
        let schema = try! Schema(json: "{ \"type\" : \"string\" }")

        if let avroValue = try? AvroValue(data: avroBytes, as: schema) {
            XCTAssertEqual(avroValue.string, "foo", "Strings don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testByteValue() {
        let avroBytes = Data([0x06, 0x66, 0x6f, 0x6f])
        let schema = try! Schema(json: "{ \"type\" : \"bytes\" }")

        if let avroValue = try? AvroValue(data: avroBytes, as: schema) {
            XCTAssertEqual(avroValue.bytes, Data([0x66, 0x6f, 0x6f]), "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testIntValue() {
        let avroBytes = Data([0x96, 0xde, 0x87, 0x3])
        let schema = try! Schema(json: "{ \"type\" : \"int\" }")

        if let avroValue = try? AvroValue(data: avroBytes, as: schema), let value = avroValue.int {
            XCTAssertEqual(Int(value), 3209099, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testLongValue() {
        let avroBytes = Data([0x96, 0xde, 0x87, 0x3])
        let schema = try! Schema(json: "{ \"type\" : \"long\" }")

        if let avroValue = try? AvroValue(data: avroBytes, as: schema), let value = avroValue.long {
            XCTAssertEqual(Int(value), 3209099, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testFloatValue() {
        let avroBytes = Data([0xc3, 0xf5, 0x48, 0x40])
        let schema = try! Schema(json: "{ \"type\" : \"float\" }")

        let expected: Float = 3.14
        if let avroValue = try? AvroValue(data: avroBytes, as: schema), let value = avroValue.float {
            XCTAssertEqual(value, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testDoubleValue() {
        let avroBytes = Data([0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x9, 0x40])
        let schema = try! Schema(json: "{ \"type\" : \"double\" }")

        let expected: Double = 3.14
        if let avroValue = ((try? AvroValue(data: avroBytes, as: schema).double) as Double??) {
            XCTAssertEqual(avroValue, expected, "Byte arrays don't match.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testBooleanValue() {
        let avroFalseBytes = Data([0x0])
        let avroTrueBytes = Data([0x1])

        let schema = try! Schema(json: "{ \"type\" : \"boolean\" }")

        if let avroValue = try? AvroValue(data: avroTrueBytes, as: schema), let value = avroValue.boolean {
            XCTAssert(value, "Value should be true.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }

        if let avroValue = try? AvroValue(data: avroFalseBytes, as: schema), let value = avroValue.boolean {
            XCTAssert(!value, "Value should be false.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }
    }

    func testArrayValue() {
        let avroBytes = Data([0x04, 0x06, 0x36, 0x00])
        let expected: [Int64] = [3, 27]
        let schema = try! Schema(json: "{ \"type\" : \"array\", \"items\" : \"long\" }")

        guard let avroValue = try? AvroValue(data: avroBytes, as: schema), let values = avroValue.array else {
            XCTAssert(false, "Failed. Nil value")
            return
        }
        XCTAssertEqual(values.count, 2, "Wrong number of elements in array.")
        for idx in 0...1 {
            guard let value = values[idx].long else {
                XCTAssert(false, "Failed. Nil value")
                return
            }
            XCTAssertEqual(value, expected[idx], "Unexpected value.")
        }
    }

    func testMapValue() {
        let avroBytes = Data([0x02, 0x06, 0x66, 0x6f, 0x6f, 0x36, 0x00])
        let expected: [Int64] = [27]
        let schema = try! Schema(json: "{ \"type\" : \"map\", \"values\" : \"long\" }")

        guard let avroValue = try? AvroValue(data: avroBytes, as: schema), let pairs = avroValue.map else {
            XCTAssert(false, "Failed. Nil value")
            return
        }
        XCTAssertEqual(pairs.count, 1, "Wrong number of elements in map.")

        guard let value = pairs["foo"]?.long else {
            XCTAssert(false, "Failed. Nil value")
            return
        }

        XCTAssertEqual(value, expected[0], "Unexpected value.")
    }

    func testEnumValue() {
        let avroBytes = Data([0x12])
        let schema = try! Schema(json: """
            { "type": "enum", "name": "ChannelKey", "doc": "Enum of valid channel keys.", "symbols":
                ["CityIphone", "CityMobileWeb", "GiltAndroid", "GiltcityCom", "GiltCom", "GiltIpad", "GiltIpadSafari", "GiltIphone", "GiltMobileWeb", "NoChannel" ]
            }
            """)

        guard let avroValue = try? AvroValue(data: avroBytes, as: schema) else {
            XCTAssert(false, "Failed. Nil value")
            return
        }

        switch avroValue {
        case .avroEnum(_, let index, let string):
            XCTAssertEqual(index, 9)
            XCTAssertEqual(string, "NoChannel")
        case _:
            XCTAssert(false, "Invalid avro value")
        }
    }

    func testUnionValue() {
        let avroBytes = Data([0x02, 0x02, 0x61])
        let schema = try! Schema(json: "{\"type\" : [\"null\",\"string\"] }")
        if let avroValue = try? AvroValue(data: avroBytes, as: schema) {
            XCTAssertEqual(avroValue.string, "a", "Unexpected string value.")
        } else {
            XCTAssert(false, "Failed. Nil value")
        }

    }
}
