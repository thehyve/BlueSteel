//
//  AvroValueConvertibleTests.swift
//  
//
//  Created by Yurii Dukhovnyi on 29.01.2021.
//

@testable import BlueSteel
import XCTest

class AvroValueConvertibleTests: XCTestCase {
    
    func testAvroValueConvertible__Array() throws {
        
        let schema = try Schema(
            json: """
                {
                    "name": "TestModel",
                    "type": "record",
                    "fields": [
                        {
                            "name": "values",
                            "default": null,
                            "type": [
                                "null",
                                {
                                    "items": "string",
                                    "type": "array"
                                }
                            ]
                        },
                        {
                            "name": "convertibleValues",
                            "default": null,
                            "type": [
                                "null",
                                {
                                    "items": "int",
                                    "type": "array"
                                }
                            ]
                        }
                    ]
                }
                """
        )
        
        // Defines test structure to verify correct conforming
        //  and constraints to the `AvroValueConvertible` protocol
        struct TestModel: AvroValueConvertible {
            let values: [String]?
            let convertibleValues: [AvroValueConvertible]?
            
            func toAvro() -> AvroValue {
                .avroRecord(.avroUnknown, [
                    "convertibleValues": convertibleValues?.toAvro(),
                    "values": values?.toAvro()
                ])
            }
        }
        
        let testModel = TestModel(values: ["a", "b", "c"], convertibleValues: [1, 2, 3])
        let avro = try AvroValue(value: testModel, as: schema)
        let data = try avro.encode(encoding: .json)
        
        XCTAssertEqual(
            data,
            "{\"values\":{\"array\":[\"a\",\"b\",\"c\"]},\"convertibleValues\":{\"array\":[1,2,3]}}".data(using: .utf8)!
        )
    }
}
