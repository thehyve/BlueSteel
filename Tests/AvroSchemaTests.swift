//
//  AvroSchemaTests.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import XCTest
import BlueSteel

class AvroSchemaTests: XCTestCase {

    override func setUp() {
        super.setUp()
    }

    override func tearDown() {
        super.tearDown()
    }

    func testSchemaEquality(_ s1: String, s2: String) {
        let lhs = try? Schema(s1)
        let rhsEqual = try? Schema(s1)
        let rhsNotEqual = try? Schema(s2)

        XCTAssertEqual(lhs, rhsEqual, "Schemas should be equal")
        XCTAssertNotEqual(lhs, rhsNotEqual, "Schemas should not be equal")
    }

    func testPrimitive() {
        let jsonSchema = "{ \"type\" : \"long\"}"
        let schema = try! Schema(jsonSchema)

        guard case .avroLong = schema else {
            XCTFail()
            return
        }
    }

    func testMap() {
        let jsonSchema = "{ \"type\" : \"map\", \"values\" : \"int\" }"
        let schema = try! Schema(jsonSchema)

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
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : \"double\" }"
        let schema = try! Schema(jsonSchema)

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
        let jsonSchema = "{ \"type\" : \"array\", \"items\" : { \"type\" : \"map\", \"values\" : \"int\" } }"
        let schema = try! Schema(jsonSchema)

        guard case .avroArray(.avroMap(.avroInt)) = schema else {
            XCTFail()
            return
        }
    }

    func testUnion() {
        let jsonSchema = "{ \"type\" : [ \"double\", \"int\", \"long\", \"float\" ] }"
        let expected: [Schema] = [.avroDouble, .avroInt, .avroLong, .avroFloat]
        let schema = try! Schema(jsonSchema)
        
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

    func testUnionMap() {
        let jsonSchema = "{ \"type\" : [ { \"type\" : \"map\", \"values\" : \"int\" }, { \"type\" : \"map\", \"values\" : \"double\" } ] }"
        let expected: [Schema] = [.avroMap(.avroInt), .avroMap(.avroDouble)]
        let schema = try! Schema(jsonSchema)

        guard case .avroUnion(let subSchemas) = schema else {
            XCTFail()
            return
        }
        XCTAssertEqual(expected, subSchemas)
    }

    func testRecord() {
        let jsonSchema = "{ \"type\" : \"record\", \"name\" : \"AddToCartActionEvent\", " +
            "\"doc\" : \"This event is fired when a user taps on the add to cart button.\"," +
            "\"fields\" : [ { \"name\" : \"lookId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"productId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"quantity\", \"type\" : \"int\" }," +
            "{ \"name\" : \"saleId\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }," +
        "{ \"name\" : \"skuId\",\"type\" : \"long\" }]}"

        let expectedFields: [Schema] = [
            .avroField("lookId", .avroLong, nil),
            .avroField("productId", .avroLong, nil),
            .avroField("quantity", .avroInt, nil),
            .avroField("saleId", .avroUnion([.avroNull, .avroLong]), .avroNull),
            .avroField("skuId", .avroLong, nil),
        ]
        let schema = try! Schema(jsonSchema)
        
        guard case .avroRecord("AddToCartActionEvent", let fields) = schema else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(expectedFields, fields)
    }

    func testRecordEquality() {
        let lJsonSchema = "{ \"type\" : \"record\", \"name\" : \"AddToCartActionEvent\", " +
            "\"doc\" : \"This event is fired when a user taps on the add to cart button.\"," +
            "\"fields\" : [ { \"name\" : \"lookId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"productId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"quantity\", \"type\" : \"int\" }," +
            "{ \"name\" : \"saleId\", \"type\" : [ \"null\", \"long\" ], \"default\" : null }," +
        "{ \"name\" : \"skuId\",\"type\" : \"long\" }]}"
        let rJsonSchema = "{ \"type\" : \"record\", \"name\" : \"AddToCartActionEvent\", " +
            "\"doc\" : \"This event is fired when a user taps on the add to cart button.\"," +
            "\"fields\" : [ { \"name\" : \"lookId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"productId\", \"type\" : \"long\" }," +
            "{ \"name\" : \"quantity\", \"type\" : \"int\" }," +
            "{ \"name\" : \"saleId\", \"type\" : [ \"null\", \"float\" ], \"default\" : null }," +
        "{ \"name\" : \"skuId\",\"type\" : \"long\" }]}"

        self.testSchemaEquality(lJsonSchema, s2: rJsonSchema)
    }

    func testEnum() {
        let jsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"

        let expectedSymbols = ["CityIphone", "CityMobileWeb", "GiltAndroid", "GiltcityCom", "GiltCom", "GiltIpad", "GiltIpadSafari", "GiltIphone", "GiltMobileWeb", "NoChannel"]
        let schema = try! Schema(jsonSchema)

        guard case .avroEnum(let enumName, let symbols) = schema else {
            XCTFail()
            return
        }
        
        XCTAssertEqual(enumName, "ChannelKey", "Unexpected enum name.")
        XCTAssertEqual(expectedSymbols, symbols, "Symbols dont match.")
    }

    func testEnumEquality() {
        // Name Checks
        let lnJsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"
        let rnJsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChanelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"

        self.testSchemaEquality(lnJsonSchema, s2: rnJsonSchema)

        let lvJsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"
        let rvJsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GilCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"

        self.testSchemaEquality(lvJsonSchema, s2: rvJsonSchema)
    }

    func testFixed() {
        let jsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"Uuid\", \"size\" : 16 }"
        let schema = try! Schema(jsonSchema)

        guard case .avroFixed(let fixedName, let size) = schema else {
            XCTFail()
            return
        }
        XCTAssertEqual("Uuid", fixedName, "Unexpected fixed name.")
        XCTAssertEqual(16, size, "Unexpected fixed size.")
    }

    func testFixedEquality() {
        let lnJsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"Uuid\", \"size\" : 16 }"
        let rnJsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"id\", \"size\" : 16 }"

        self.testSchemaEquality(lnJsonSchema, s2: rnJsonSchema)

        let lvJsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"Uuid\", \"size\" : 16 }"
        let rvJsonSchema = "{ \"type\" : \"fixed\", \"name\" : \"Uuid\", \"size\" : 10 }"

        self.testSchemaEquality(lvJsonSchema, s2: rvJsonSchema)
    }

    func testFingerprint() {
        //let jsonSchema = "{ \"type\" : \"enum\", \"name\" : \"ChannelKey\", \"doc\" : \"Enum of valid channel keys.\", \"symbols\" : [ \"CityIphone\", \"CityMobileWeb\", \"GiltAndroid\", \"GiltcityCom\", \"GiltCom\", \"GiltIpad\", \"GiltIpadSafari\", \"GiltIphone\", \"GiltMobileWeb\", \"NoChannel\" ]}"
        let jsonSchema = "{\"type\":\"record\",\"name\":\"StorePageViewedEvent\",\"namespace\":\"com.gilt.mobile.tapstream.v1\",\"doc\":\"This event is fired when a store is displayed.\",\"fields\":[{\"name\":\"uuid\",\"type\":{\"type\":\"fixed\",\"name\":\"UUID\",\"namespace\":\"gfc.avro\",\"size\":16},\"doc\":\"the unique identifier of the event, as determined by the mobile app.\\n        this must be a version 1 uuid.\"},{\"name\":\"base\",\"type\":{\"type\":\"record\",\"name\":\"MobileEvent\",\"doc\":\"Fields common to all events generated by mobile apps.\\n      NOTE: this should not be sent as is, meant to be wrapped into some more specific type.\",\"fields\":[{\"name\":\"eventTs\",\"type\":\"long\",\"doc\":\"The unix timestamp at which the event occurred.\\n        This is in Gilt time (not device time).\"},{\"name\":\"batchGuid\",\"type\":\"gfc.avro.UUID\",\"doc\":\"NOTE: This attribute should NOT be set by the client, it will be set by the server.\\n        The unique identifier assigned to a batch of events.\\n        Events that share this value were submitted by a client as part of the same batch.\",\"default\":\"\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\\u0000\"},{\"name\":\"channelKey\",\"type\":{\"type\":\"enum\",\"name\":\"ChannelKey\",\"doc\":\"Enum of valid channel keys.\",\"symbols\":[\"CityIphone\",\"CityMobileWeb\",\"GiltAndroid\",\"GiltcityCom\",\"GiltCom\",\"GiltIpad\",\"GiltIpadSafari\",\"GiltIphone\",\"GiltMobileWeb\",\"NoChannel\"]}},{\"name\":\"deviceTimeOffset\",\"type\":\"long\",\"doc\":\"Offset in milliseconds between the Gilt time and the device time (device time + offset == Gilt time)\"},{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"doc\":\"The HTTP headers of the request the event was sent in.\\n        Multi-valued header values are tab-separated.\\n        NOTE: This attribute should NOT be set by the client, it will be set by the server.\",\"default\":{}},{\"name\":\"ipAddress\",\"type\":\"string\",\"doc\":\"IP address of the client.\\n        NOTE: This attribute should NOT be set by the client, it will be set by the server.\",\"default\":\"0.0.0.0\"},{\"name\":\"sessionTs\",\"type\":\"long\",\"doc\":\"The unix timestamp of the current session.\"},{\"name\":\"testBucketId\",\"type\":\"long\",\"doc\":\"The test bucket identifier.\"},{\"name\":\"userAgent\",\"type\":\"string\",\"doc\":\"The user agent of the request.\\n        NOTE: This attribute should NOT be set by the client, it will be set by the server.\",\"default\":\"\"},{\"name\":\"userGuid\",\"type\":[\"null\",\"gfc.avro.UUID\"],\"doc\":\"The Gilt user_guid (optional).\",\"default\":null},{\"name\":\"visitorGuid\",\"type\":\"gfc.avro.UUID\",\"doc\":\"Generated on first app launch it never changes unless the app is uninstalled and re-installed.\"}]}},{\"name\":\"page\",\"type\":{\"type\":\"record\",\"name\":\"PageViewedEvent\",\"doc\":\"Fields common to all events of type page_viewed.\\n      NOTE: this should not be sent as is, meant to be wrapped into some more specific type.\",\"fields\":[{\"name\":\"deviceOrientation\",\"type\":{\"type\":\"enum\",\"name\":\"DeviceOrientation\",\"doc\":\"Enum of valid device orientations.\",\"symbols\":[\"Landscape\",\"Portrait\"]}}]}},{\"name\":\"storeKey\",\"type\":{\"type\":\"enum\",\"name\":\"StoreKey\",\"doc\":\"Enum of valid store keys.\",\"symbols\":[\"Children\",\"City\",\"Gifts\",\"Home\",\"Men\",\"MyGilt\",\"Women\",\"NoStore\"]}}]}"

        let schema = try! Schema(jsonSchema)

        var existingTypes: Set<String> = []
        if let canonicalString = schema.canonicalString(&existingTypes) {
            Swift.print(canonicalString)
        }
    }
}
