//
//  Schema.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

/// Avro `Schema`. Use `init(json:)` for initialation from file.
/// Otherwise, directly initialize a case. Check the validity
/// of the schema with `validate(context:)` before using it with
/// `AvroValue`.
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

    private static let validNameExpression = try! NSRegularExpression(pattern: "^[A-Za-z_][A-Za-z0-9_]*$")

    /// Validates a schema single name according to the Avro specification. If the namespace
    /// may have been encoded in the name, use `validate(fullName:context:)` instead.
    /// - Throws: `Schema.ValidationError.invalidName` if the name does not match
    public static func validate(name: String, context: AvroCodingContext = AvroCodingContext()) throws {
        if validNameExpression.numberOfMatches(in: name, options: [], range: NSRange(location: 0, length: name.count)) != 1 {
            throw ValidationError.invalidName(name, context)
        }
    }

    /// Validates a schema namespace according to the Avro specification.
    /// - Throws: `Schema.ValidationError.invalidNamespace` if the name does not match
    public static func validate(namespace: String, context: AvroCodingContext = AvroCodingContext()) throws {
        guard !namespace.isEmpty else { return }
        do {
            try namespace.split(separator: ".").forEach { try validate(name: String($0), context: context) }
        } catch ValidationError.invalidName(let name, let context) {
            throw ValidationError.invalidNamespace(namespace, part: name, context)
        }
    }
    /// Validates a full schema namespace according to the Avro specification.
    /// - Throws: `Schema.ValidationError.invalidName` or `Schema.ValidationError.invalidNamespace` if
    ///           the name does not match
    public static func validate(fullName: String, context: AvroCodingContext = AvroCodingContext()) throws {
        guard !fullName.isEmpty else { throw ValidationError.invalidName(fullName, context) }
        try validate(namespace: fullName, context: context)
    }

    private static func validate(unique values: [String], context: AvroCodingContext) throws {
        var uniqueValues = Set<String>()
        var existingDuplicates = Set<String>()
        let duplicates = values.filter {
            uniqueValues.update(with: $0) != nil
                && existingDuplicates.update(with: $0) == nil
        }

        guard duplicates.isEmpty else {
            throw ValidationError.notUnique(duplicates, context)
        }
    }

    private static func validate(notEmpty count: Int, context: AvroCodingContext) throws {
        guard count > 0 else {
            throw ValidationError.empty(context)
        }
    }

    public enum ValidationError : Error {
        case invalidName(String, AvroCodingContext)
        case invalidNamespace(String, part: String, AvroCodingContext)
        case invalidFixedSize(Int, AvroCodingContext)
        case invalidType(AvroCodingContext)
        case empty(AvroCodingContext)
        case notUnique([String], AvroCodingContext)
    }

    /// Validates this schema according to the Avro specification.
    /// - Throws: `Schema.ValidationError` if the schema is not valid
    public func validate(context: AvroCodingContext = AvroCodingContext()) throws {
        switch self {
        case .avroArray(let itemSchema):
            try itemSchema.validate(context: context.nestedIn(type: "array"))

        case .avroMap(let valueSchema):
            try valueSchema.validate(context: context.nestedIn(type: "map"))

        case .avroUnion(let optionSchemas):
            try Schema.validate(notEmpty: optionSchemas.count, context: context)
            try Schema.validate(unique: optionSchemas.map { $0.typeName }, context: context)
            try optionSchemas.enumerated().forEach { option in
                try option.element.validate(context: context.nestedIn(type: "union[\(option.offset)]"))
            }

        case .avroEnum(let name, let symbols):
            try Schema.validate(fullName: name, context: context.nestedIn(type: "enum"))
            let context = context.nestedIn(type: name)
            try Schema.validate(notEmpty: symbols.count, context: context)
            try Schema.validate(unique: symbols, context: context)
            try symbols.forEach { try Schema.validate(name: $0, context: context) }

        case .avroFixed(let name, let size):
            try Schema.validate(fullName: name, context: context.nestedIn(type: "fixed"))
            let context = context.nestedIn(type: name)
            try Schema.validate(notEmpty: size, context: context)

        case .avroRecord(let name, let fields):
            try Schema.validate(fullName: name, context: context.nestedIn(type: "record"))
            let context = context.nestedIn(type: name)
            try Schema.validate(notEmpty: fields.count, context: context)
            try fields.forEach { field in
                try Schema.validate(name: field.name, context: context)
                try field.schema.validate(context: context.nestedIn(type: field.name))
            }
            try Schema.validate(unique: fields.map { $0.name }, context: context)

        case .avroUnknown:
            throw ValidationError.invalidType(context)

        default:
            break
        }
    }

    /// Single field schema in an Avro record.
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
