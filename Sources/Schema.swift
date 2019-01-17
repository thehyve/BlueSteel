//
//  Schema.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public enum Schema {
    case avroUnknown

    case avroNull
    case avroBoolean
    case avroInt
    case avroLong
    case avroFloat
    case avroDouble
    case avroString
    case avroBytes

    indirect case avroArray(Schema)
    indirect case avroMap(Schema)
    case avroUnion(Array<Schema>)

    // Named Types
    case avroFixed(String, Int)
    case avroEnum(String, Array<String>)
    case avroRecord(String, Array<Schema>)
    indirect case avroField(String, Schema, AvroValue?)

    static func assembleFullName(_ namespace:String?, name: String) -> String {
        if name.range(of: ".") == nil, let namespace = namespace {
            return namespace + "." + name
        } else {
            return name
        }
    }

    public init(json: Data) throws {
        var parser = Parser()
        self = try parser.parse(json)
    }

    public init(json: String) throws {
        var parser = Parser()
        self = try parser.parse(json)
    }

    public func jsonString() -> String? {
        var formatter = Formatter()
        return try? formatter.jsonString(self)
    }

    public var typeName: String {
        switch self {
        case .avroNull:
            return "null"
        case .avroBoolean:
            return "boolean"
        case .avroInt:
            return "int"
        case .avroString:
            return "string"
        case .avroBytes:
            return "bytes"
        case .avroDouble:
            return "double"
        case .avroFloat:
            return "float"
        case .avroMap(_):
            return "map"
        case .avroArray(_):
            return "array"
        case .avroFixed(let name, _),
             .avroEnum(let name, _),
             .avroRecord(let name, _),
             .avroField(let name, _, _):
            return name
        case .avroLong:
            return "long"
        case .avroUnion(_):
            return "union"
        case .avroUnknown:
            return "unknown"
        }
    }
}
