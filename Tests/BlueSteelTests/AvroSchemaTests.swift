//
//  AvroSchemaTests.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
@testable import BlueSteel

class AvroSchemaTests: XCTestCase {

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testSchemaEquality(_ s1: String, s2: String) {
        let lhs = try? Schema(json: s1)
        let rhsEqual = try? Schema(json: s1)
        let rhsNotEqual = try? Schema(json: s2)

        XCTAssertEqual(lhs, rhsEqual, "Schemas should be equal")
        XCTAssertNotEqual(lhs, rhsNotEqual, "Schemas should not be equal")
    }

    func testPrimitive() {
        let schema = try! Schema(json: """
            { "type" : "long"}
            """)

        guard case .avroLong = schema else {
            XCTFail()
            return
        }
    }

    func testMap() {
        let schema = try! Schema(json: """
            { "type" : "map", "values" : "int" }
            """)

        guard case .avroMap(let valueSchema) = schema else {
            XCTFail()
            return
        }

        guard case .avroInt = valueSchema else {
            XCTFail()
            return
        }
    }

    func testMapEquality() {
        let lJsonSchema = "{ \"type\" : \"map\", \"values\" : \"bytes\" }"
        let rJsonSchema = "{ \"type\" : \"map\", \"values\" : \"string\" }"

        self.testSchemaEquality(lJsonSchema, s2: rJsonSchema)
    }

    func testArray() {
        let schema = try! Schema(json: """
            { "type" : "array", "items" : "double" }
            """)

        guard case .avroArray(let valueSchema) = schema else {
            XCTFail()
            return
        }

        guard case .avroDouble = valueSchema else {
            XCTFail()
            return
        }
    }

    func testArrayEquality() {
        let lJsonSchema = "{ \"type\" : \"array\", \"items\" : { \"type\" : \"map\", \"values\" : \"int\" } }"
        let rJsonSchema = "{ \"type\" : \"array\", \"items\" : { \"type\" : \"map\", \"values\" : \"long\" } }"

        self.testSchemaEquality(lJsonSchema, s2: rJsonSchema)
    }

    func testArrayMap() {
        let schema = try! Schema(json: """
            { "type" : "array", "items":
                { "type" : "map", "values" : "int" }
            }
            """)

        guard case .avroArray(.avroMap(.avroInt)) = schema else {
            XCTFail()
            return
        }
    }

    func testUnion() {
        let schema = try! Schema(json: """
            { "type" : [ "double", "int", "long", "float" ] }
            """)
        let expected: [Schema] = [.avroDouble, .avroInt, .avroLong, .avroFloat]

        guard case .avroUnion(let schemas) = schema else {
            XCTFail()
            return
        }

        XCTAssertEqual(expected, schemas)
    }

    func testUnionEquality() {
        let lJsonSchema = "{ \"type\" : [ \"double\", \"int\", \"long\", \"float\" ] }"
        let rJsonSchema = "{ \"type\" : [ \"double\", \"float\", \"int\", \"long\" ] }"

        self.testSchemaEquality(lJsonSchema, s2: rJsonSchema)
    }

    func testRecord() {
        let schema = try! Schema(json: """
            {
                "type" : "record",
                "name" : "AddToCartActionEvent",
                "doc" : "This event is fired when a user taps on the add to cart button.",
                "fields" : [
                    { "name" : "lookId", "type" : "long" },
                    { "name" : "productId", "type" : "long" },
                    { "name" : "quantity", "type" : "int" },
                    { "name" : "saleId", "type" : [ "null", "long" ], "default" : null },
                    { "name" : "skuId", "type" : "long" }
                ]
            }
            """)
        let expectedFields: [Schema.Field] = [
            Schema.Field(name: "lookId", schema: .avroLong),
            Schema.Field(name: "productId", schema: .avroLong),
            Schema.Field(name: "quantity", schema: .avroInt),
            Schema.Field(name: "saleId", schema: .avroUnion(options: [.avroNull, .avroLong]),
                         defaultValue: .avroUnion(schemaOptions: [.avroNull, .avroLong], index: 0, AvroValue.avroNull)),
            Schema.Field(name: "skuId", schema: .avroLong),
        ]

        guard case .avroRecord("AddToCartActionEvent", let fields) = schema else {
            XCTFail()
            return
        }

        XCTAssertEqual(expectedFields, fields)
    }

    func testRecordEquality() {
        let lJsonSchema = """
            {
                "type" : "record",
                "name" : "AddToCartActionEvent",
                "doc" : "This event is fired when a user taps on the add to cart button.",
                "fields" : [
                    { "name" : "lookId", "type" : "long" },
                    { "name" : "productId", "type" : "long" },
                    { "name" : "quantity", "type" : "int" },
                    { "name" : "saleId", "type" : [ "null", "long" ], "default" : null },
                    { "name" : "skuId", "type" : "long" }
                ]
            }
            """
        let rJsonSchema = """
            {
                "type" : "record",
                "name" : "AddToCartActionEvent",
                "doc" : "This event is fired when a user taps on the add to cart button.",
                "fields" : [
                    { "name" : "lookId", "type" : "long" },
                    { "name" : "productId", "type" : "long" },
                    { "name" : "quantity", "type" : "int" },
                    { "name" : "saleId", "type" : [ "null", "float" ], "default" : null },
                    { "name" : "skuId", "type" : "long" }
                ]
            }
            """

        self.testSchemaEquality(lJsonSchema, s2: rJsonSchema)
    }

    func testEnum() {
        let schema = try! Schema(json: """
            { "type" : "enum", "name" : "ChannelKey", "doc" : "Enum of valid channel keys.", "symbols" :
                [ "CityIphone", "CityMobileWeb", "GiltAndroid", "GiltcityCom", "GiltCom", "GiltIpad", "GiltIpadSafari", "GiltIphone", "GiltMobileWeb", "NoChannel" ]
            }
            """)

        let expectedSymbols = ["CityIphone", "CityMobileWeb", "GiltAndroid", "GiltcityCom", "GiltCom", "GiltIpad", "GiltIpadSafari", "GiltIphone", "GiltMobileWeb", "NoChannel"]

        guard case .avroEnum(let enumName, let symbols) = schema else {
            XCTFail()
            return
        }

        XCTAssertEqual(enumName, "ChannelKey", "Unexpected enum name.")
        XCTAssertEqual(expectedSymbols, symbols, "Symbols dont match.")
    }

    func testEnumEquality() {
        // Name Checks
        let originalJsonSchema = """
            { "type": "enum", "name": "ChannelKey", "doc": "Enum of valid channel keys.", "symbols":
                [ "CityIphone", "CityMobileWeb", "GiltAndroid", "GiltcityCom", "GiltCom", "GiltIpad", "GiltIpadSafari", "GiltIphone", "GiltMobileWeb", "NoChannel" ]
            }
            """
        let rnJsonSchema = """
            { "type": "enum", "name": "ChanelKey", "doc": "Enum of valid channel keys.", "symbols":
                [ "CityIphone", "CityMobileWeb", "GiltAndroid", "GiltcityCom", "GiltCom", "GiltIpad", "GiltIpadSafari", "GiltIphone", "GiltMobileWeb", "NoChannel" ]
            }
            """

        self.testSchemaEquality(originalJsonSchema, s2: rnJsonSchema)

        let rvJsonSchema = """
            { "type": "enum", "name": "ChannelKey", "doc": "Enum of valid channel keys.", "symbols":
                [ "CityIphone", "CityMobileWeb", "GiltAndroid", "GiltcityCom", "GilCom", "GiltIpad", "GiltIpadSafari", "GiltIphone", "GiltMobileWeb", "NoChannel" ]
            }
            """
        self.testSchemaEquality(originalJsonSchema, s2: rvJsonSchema)
    }

    func testFixed() {
        let schema = try! Schema(json: """
            { "type": "fixed", "name": "Uuid", "size": 16 }
            """)

        guard case .avroFixed(let fixedName, let size) = schema else {
            XCTFail()
            return
        }
        XCTAssertEqual("Uuid", fixedName, "Unexpected fixed name.")
        XCTAssertEqual(16, size, "Unexpected fixed size.")
    }

    func testFixedEquality() {
        let originalJsonSchema = """
            { "type": "fixed", "name": "Uuid", "size": 16 }
            """
        let rnJsonSchema = """
            { "type": "fixed", "name": "id", "size": 16 }
            """

        self.testSchemaEquality(originalJsonSchema, s2: rnJsonSchema)

        let rvJsonSchema = """
            { "type": "fixed", "name": "Uuid", "size": 10 }
            """

        self.testSchemaEquality(originalJsonSchema, s2: rvJsonSchema)
    }

    func testFingerprint() {
        let schema = try! Schema(json: """
            {
                "type":"record",
                "name":"StorePageViewedEvent",
                "namespace":"com.gilt.mobile.tapstream.v1",
                "doc":"This event is fired when a store is displayed.",
                "fields": [
                    {"name":"uuid","type":{"type":"fixed","name":"UUID","namespace":"gfc.avro","size":16},"doc":"the unique identifier of the event, as determined by the mobile app.\\n        this must be a version 1 uuid."},
                    {"name":"base","type": {
                        "type":"record",
                        "name":"MobileEvent",
                        "doc":"Fields common to all events generated by mobile apps.\\n      NOTE: this should not be sent as is, meant to be wrapped into some more specific type.",
                        "fields":[
                            {"name":"eventTs","type":"long","doc":"The unix timestamp at which the event occurred.\\n        This is in Gilt time (not device time)."},
                            {"name":"batchGuid","type":"gfc.avro.UUID","doc":"NOTE: This attribute should NOT be set by the client, it will be set by the server.\\n        The unique identifier assigned to a batch of events.\\n        Events that share this value were submitted by a client as part of the same batch.","default":"\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000"},
                            {"name":"channelKey","type": {
                                "type":"enum","name":"ChannelKey","doc":"Enum of valid channel keys.","symbols":
                                ["CityIphone","CityMobileWeb","GiltAndroid","GiltcityCom","GiltCom","GiltIpad","GiltIpadSafari","GiltIphone","GiltMobileWeb","NoChannel"]}},
                            {"name":"deviceTimeOffset","type":"long","doc":"Offset in milliseconds between the Gilt time and the device time (device time + offset == Gilt time)"},
                            {"name":"headers","type":{"type":"map","values":"string"},"doc":"The HTTP headers of the request the event was sent in.\\n        Multi-valued header values are tab-separated.\\n        NOTE: This attribute should NOT be set by the client, it will be set by the server.","default":{"a":"b"}},
                            {"name":"ipAddress","type":"string","doc":"IP address of the client.\\n        NOTE: This attribute should NOT be set by the client, it will be set by the server.","default":"0.0.0.0"},{"name":"sessionTs","type":"long","doc":"The unix timestamp of the current session."},
                            {"name":"testBucketId","type":"long","doc":"The test bucket identifier."},
                            {"name":"userAgent","type":"string","doc":"The user agent of the request.\\n        NOTE: This attribute should NOT be set by the client, it will be set by the server.","default":""},
                            {"name":"userGuid","type":["null","gfc.avro.UUID"],"doc":"The Gilt user_guid (optional).","default":null},
                            {"name":"visitorGuid","type":"gfc.avro.UUID","doc":"Generated on first app launch it never changes unless the app is uninstalled and re-installed."}
                        ]}},
                    {"name":"page","type": {
                        "type":"record",
                        "name":"PageViewedEvent",
                        "doc":"Fields common to all events of type page_viewed.\\n      NOTE: this should not be sent as is, meant to be wrapped into some more specific type.",
                        "fields":[
                            {"name":"deviceOrientation","type":
                                {"type":"enum","name":"DeviceOrientation","doc":"Enum of valid device orientations.","symbols":["Landscape","Portrait"]}}
                        ]}},
                    {"name":"storeKey","type":{"type":"enum","name":"StoreKey","doc":"Enum of valid store keys.","symbols":
                        ["Children","City","Gifts","Home","Men","MyGilt","Women","NoStore"]}},
                    {"name": "recursivePage", "type": "PageViewedEvent"}
                ]
            }
            """)

        guard let canonicalString = schema.jsonString() else {
            XCTFail()
            return
        }
        guard let canonicalSchema = try? Schema(json: canonicalString) else {
            XCTFail()
            return
        }
        Swift.print(canonicalSchema)
        XCTAssertEqual(schema, canonicalSchema)
    }

    func testSchemaParsing() {
        let schema: Schema = .avroRecord(name: "A", fields: [
            Schema.Field(name: "a", schema: .avroInt),
            Schema.Field(name: "b", schema: .avroEnum(name: "B", symbols: ["opt1", "opt2"])),
            Schema.Field(name: "c", schema: .avroLong, defaultValue: .avroLong(1)),
            Schema.Field(name: "d", schema: .avroMap(values: .avroBytes)),
            Schema.Field(name: "f", schema: .avroString),
            Schema.Field(name: "g", schema: .avroUnion(options: [.avroString, .avroInt])),
            Schema.Field(name: "h", schema: .avroBytes, defaultValue: .avroBytes(Data([0xff]))),
            Schema.Field(name: "i", schema: .avroFloat)
            ])

        let jsonString1 = """
            {
                "name": "A",
                "type": "record",
                "fields": [
                    {"name": "a", "type": "int"},
                    {"name": "b", "type": {"name": "B", "type": "enum", "symbols": ["opt1", "opt2"]}},
                    {"name": "c", "type": "long", "default": 1},
                    {"name": "d", "type": {"type": "map", "values": "bytes"}},
                    {"name": "f", "type": "string"},
                    {"name": "g", "type": ["string", "int"]},
                    {"name": "h", "type": "bytes", "default": "\\u00ff"},
                    {"name": "i", "type": "float"}
                ]
            }
            """
        let jsonString2 = """
            {
                "name": "A",
                "type": "record",
                "fields": [
                    {"name": "a", "type": "int"},
                    {"name": "b", "type": {"name": "B", "type": "enum", "symbols": ["opt1", "opt2"]}},
                    {"name": "c", "type": "long", "default": 1},
                    {"name": "d", "type": {"type": "map", "values": "bytes"}},
                    {"name": "f", "type": "string"},
                    {"name": "g", "type": ["string", "int"]},
                    {"name": "h", "type": "bytes", "default": "ÿ"},
                    {"name": "i", "type": "float"}
                ]
            }
            """

        let jsonSchema1 = try! Schema(json: jsonString1)
        let jsonSchema2 = try! Schema(json: jsonString1)
        XCTAssertEqual(schema, jsonSchema1)
        XCTAssertEqual(schema, jsonSchema2)
        XCTAssertEqual(schema.jsonString(), jsonString2.replacingOccurrences(of: "\\s", with: "", options: .regularExpression))
        XCTAssertEqual(schema.description, "A<record>([a: int, b: B<enum>([opt1, opt2]), c: long (default: 1L), d: map<bytes>, f: string, g: [string, int], h: bytes (default: 1 bytes), i: float])")
    }

    func testValidation() {
        do {
            try Schema.avroEnum(name: "", symbols: ["a"]).validate()
            XCTFail()
        } catch Schema.ValidationError.invalidName("", _) {
            // good
        } catch {
            XCTFail()
        }
        do {
            try Schema.avroEnum(name: "a", symbols: [""]).validate()
            XCTFail()
        } catch Schema.ValidationError.invalidName("", _) {
            // good
        } catch {
            XCTFail()
        }
        do {
            try Schema.avroEnum(name: "a", symbols: []).validate()
            XCTFail()
        } catch Schema.ValidationError.empty(_) {
            // good
        } catch {
            XCTFail()
        }
        do {
            try Schema.avroUnknown.validate()
            XCTFail()
        } catch Schema.ValidationError.invalidType(_) {
            // good
        } catch {
            XCTFail()
        }
        do {
            try Schema.avroUnion(options: []).validate()
            XCTFail()
        } catch Schema.ValidationError.empty(_) {
            // good
        } catch {
            XCTFail()
        }
        do {
            try Schema.avroUnion(options: [.avroUnknown]).validate()
            XCTFail()
        } catch Schema.ValidationError.invalidType(_) {
            // good
        } catch {
            XCTFail()
        }
        do {
            try Schema.avroFixed(name: "a", size: -123).validate()
            XCTFail()
        } catch Schema.ValidationError.empty(_) {
            // good
        } catch {
            XCTFail()
        }

        do {
            try Schema.avroEnum(name: "T", symbols: ["a", "b", "b", "c", "a", "d", "a"]).validate()
            XCTFail()
        } catch Schema.ValidationError.notUnique(let values, _) {
            XCTAssertEqual(values, ["b", "a"])
        } catch {
            XCTFail()
        }
    }
}
