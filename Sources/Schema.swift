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

    indirect case avroArray(items: Schema)
    indirect case avroMap(values: Schema)
    case avroUnion(options: [Schema])

    // Named Types
    case avroFixed(name: String, size: Int)
    case avroEnum(name: String, symbols: [String])
    case avroRecord(name: String, fields: [Field])

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
             .avroRecord(let name, _):
            return name
        case .avroLong:
            return "long"
        case .avroUnion(_):
            return "union"
        case .avroUnknown:
            return "unknown"
        }
    }

    public struct Field {
        let name: String
        let schema: Schema
        var defaultValue: AvroValue? = nil

        public init(name: String, schema: Schema, defaultValue: AvroValue? = nil) {
            self.name = name
            self.schema = schema
            self.defaultValue = defaultValue
        }
    }
}
