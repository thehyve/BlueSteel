//
//  AvroConvertible.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

/// Value that is convertable to avro
public protocol AvroValueConvertible {
    /// Converts data to Avro. Without any schema information, this may
    /// yield an invalid Avro value. To ensure the Avro value validates
    /// against an intended schema, call AvroValue.init(value:as:).
    ///
    /// - Returns: an Avro value with a possibly invalid schema
    func toAvro() -> AvroValue
}

public enum AvroConversionError: Error {
    case invalid
    case invalidDouble(Double)
    case invalidFixedCount(expected: Int, actual: Int)
    case conversionFailed
    case mismatch(AvroValue, Schema)
    case enumSymbolNotFound(String, symbols: [String])
    case fieldNotFound(String)
}

extension String: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroString(self)
    }
}

extension Bool: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroBoolean(self)
    }
}

extension Int: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroLong(Int64(self))
    }
}

extension Int32: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroInt(self)
    }
}

extension Int64: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroLong(self)
    }
}

extension Float: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroFloat(self)
    }
}

extension Double: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroDouble(self)
    }
}

extension Data: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroBytes(self)
    }
}

extension Dictionary: AvroValueConvertible where Key == String, Value: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroMap(valueSchema: .avroUnknown, self)
    }
}

extension Array: AvroValueConvertible where Element: AvroValueConvertible {

    public func toAvro() -> AvroValue {
        return AvroValue.avroArray(itemSchema: .avroUnknown, self)
    }
}

extension Array where Element == AvroValueConvertible {

    public func toAvro() -> AvroValue {
        return AvroValue.avroArray (itemSchema: .avroUnknown, self)
    }
}

extension NSNull: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroNull
    }
}

extension Optional: AvroValueConvertible where Wrapped: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        switch self {
        case .none:
            return .avroNull
        case let .some(wrappedValue):
            return wrappedValue.toAvro()
        }
    }
}

extension AvroValue: AvroValueConvertible {
    static let validNameExpression = try! NSRegularExpression(pattern: "^[A-Za-z_][A-Za-z0-9_]*$")

    public func toAvro() -> AvroValue {
        return self
    }

    private static func bytesToString(data: Data) throws -> String {
        guard let result = String(data: data, encoding: .utf8) else {
            throw AvroConversionError.conversionFailed
        }
        return result
    }

    private static func stringToBytes(string: String) throws -> Data {
        guard let result = string.data(using: .utf8, allowLossyConversion: false) else {
            throw AvroConversionError.conversionFailed
        }
        return result
    }

    static func validate(double: Double) throws {
        guard !double.isNaN && !double.isInfinite else {
            throw AvroConversionError.invalidDouble(double)
        }
    }

    static func validate(fixed data: Data, size: Int) throws {
        guard data.count == size else {
            throw AvroConversionError.invalidFixedCount(expected: size, actual: data.count)
        }
    }

    /// Converts given value to Avro using a schema. This conversion
    /// also checks the metadata of the schema, so the resulting value
    /// is completely valid and conforming to given schema. For example,
    /// union and enum values indexes are checked, the size of a fixed
    /// value, and the item schema of an array.
    ///
    /// The following conversions and validations are applied:
    /// - Numeric values are all converted.
    /// - Floating point numbers with infinite or NaN values are rejected.
    /// - Bytes and fixed are converted to String and vice-versa using
    ///   UTF-8 encoding.
    /// - Maps and records are converted to each other if possible. Missing
    ///   fields of a record are attempted to be filled with default values,
    ///   if any.
    /// - Enums are interchangable with strings.
    /// - The index of an enum is disregarded, only the value is taken to
    ///   correspond to one of the enum symbols.
    /// - A map with a single value, with the key corresponding to one of
    ///   the type names in a union, is converted to a value in that union.
    /// - Other values can be converted from or to a union using the first
    ///   schema that they match. First, a match is attempted using only
    ///   the type name, then a match is attempted by converting the value
    ///   to one of the union options.
    ///
    /// - Returns: fully instantiated AvroValue with a valid schema
    /// - Throws: `AvroConversionError` if the value cannot be converted
    ///           to given schema or if the given schema is invalid.
    ///
    public init(value: AvroValueConvertible, as schema: Schema) throws {
        let avroValue = value.toAvro()

        switch (avroValue, schema) {
        case (.avroBoolean(_), .avroBoolean),
             (.avroInt(_), .avroInt),
             (.avroLong(_), .avroLong),
             (.avroNull, .avroNull),
             (.avroString(_), .avroString),
             (.avroBytes(_), .avroBytes):
            self = avroValue

        case (.avroInt(let inner), .avroDouble):
            self = .avroDouble(Double(inner))
        case (.avroInt(let inner), .avroFloat):
            self = .avroFloat(Float(inner))
        case (.avroInt(let inner), .avroLong):
            self = .avroLong(Int64(inner))

        case (.avroLong(let inner), .avroDouble):
            self = .avroDouble(Double(inner))
        case (.avroLong(let inner), .avroFloat):
            self = .avroFloat(Float(inner))
        case (.avroLong(let inner), .avroInt):
            self = .avroInt(Int32(inner))

        case (.avroFloat(let inner), .avroFloat):
            try AvroValue.validate(double: Double(inner))
            self = avroValue
        case (.avroFloat(let inner), .avroDouble):
            try AvroValue.validate(double: Double(inner))
            self = .avroDouble(Double(inner))
        case (.avroFloat(let inner), .avroInt):
            self = .avroInt(Int32(inner))
        case (.avroFloat(let inner), .avroLong):
            self = .avroLong(Int64(inner))

        case (.avroDouble(let inner), .avroDouble):
            try AvroValue.validate(double: inner)
            self = avroValue
        case (.avroDouble(let inner), .avroInt):
            self = .avroInt(Int32(inner))
        case (.avroDouble(let inner), .avroFloat):
            try AvroValue.validate(double: inner)
            self = .avroFloat(Float(inner))
        case (.avroDouble(let inner), .avroLong):
            self = .avroLong(Int64(inner))

        case (.avroString(let inner), .avroBytes):
            let data = try  AvroValue.stringToBytes(string: inner)
            self = .avroBytes(data)
        case (.avroBytes(let inner), .avroString):
            let newValue = try AvroValue.bytesToString(data: inner)
            self = .avroString(newValue)

        case (.avroFixed(_, let inner), .avroFixed(_, let count)):
            try AvroValue.validate(fixed: inner, size: count)
            self = .avroFixed(schema, inner)
        case (.avroFixed(_, let inner), .avroBytes):
            self = .avroBytes(inner)
        case (.avroFixed(_, let inner), .avroString):
            let newValue = try AvroValue.bytesToString(data: inner)
            self = .avroString(newValue)

        case (.avroBytes(let inner), .avroFixed(_, let count)):
            try AvroValue.validate(fixed: inner, size: count)
            self = .avroFixed(schema, inner)
        case (.avroString(let inner), .avroFixed(_, let count)):
            let data = try AvroValue.stringToBytes(string: inner)
            try AvroValue.validate(fixed: data, size: count)
            self = .avroFixed(schema, data)

        case (.avroArray(_, let values), .avroArray(let subSchema)):
            let newValues = try values.map { try AvroValue(value: $0, as: subSchema) }
            self = .avroArray(itemSchema: subSchema, newValues)

        case (.avroMap(_, let values), .avroMap(let subSchema)),
             (.avroRecord(_, let values), .avroMap(let subSchema)):
            let newValues = try values.mapValues { try AvroValue(value: $0, as: subSchema) }
            self = .avroMap(valueSchema: subSchema, newValues)

        case (.avroEnum(_, let index, let value), .avroEnum(_, let symbols)):
            guard value == symbols[index] else {
                self = try AvroValue(value: AvroValue.avroString(value), as: schema)
                return
            }
            self = .avroEnum(schema, index: index, value)
        case (.avroString(let value), .avroEnum(_, let symbols)):
            guard let newIndex = symbols.firstIndex(of: value) else {
                throw AvroConversionError.enumSymbolNotFound(value, symbols: symbols)
            }
            self = .avroEnum(schema, index: newIndex, value)
        case (.avroEnum(_, _, let value), .avroString):
            self = .avroString(value)

        case (.avroRecord(_, let fields), .avroRecord(_, let subSchemas)),
             (.avroMap(_, let fields), .avroRecord(_, let subSchemas)):
            var newValue: [String: AvroValueConvertible] = [:]
            for field in subSchemas {
                if let fieldValue = fields[field.name] {
                    newValue[field.name] = try AvroValue(value: fieldValue, as: field.schema)
                } else if let fieldDefault = field.defaultValue {
                    newValue[field.name] = try AvroValue(value: fieldDefault, as: field.schema)
                } else {
                    throw AvroConversionError.fieldNotFound(field.name)
                }
            }
            self = .avroRecord(schema, newValue)

        case (.avroUnion(_, let index, let inner), .avroUnion(let subSchemas)):
            guard index < subSchemas.count,
                inner.toAvro().schema.typeName == subSchemas[index].typeName else
            {
                // try to convert any other way than the default way
                // e.g. can resolve a union if a record name changes but the
                // record schema is still the same
                self = try AvroValue(value: inner, as: schema)
                return
            }
            let newValue = try AvroValue(value: inner, as: subSchemas[index])
            self = .avroUnion(schemaOptions: subSchemas, index: index, newValue)

        case (.avroUnion(_, _, let inner), _):
            // try to map from union schema to only available schema
            self = try AvroValue(value: inner, as: schema)

        case (.avroMap(_, let innerValues), .avroUnion(let subSchemas)):
            let newValue: AvroValueConvertible
            let newIndex: Int
            if innerValues.count == 1,
                let (key, inner) = innerValues.first,
                let index = subSchemas.firstIndex(where: { $0.typeName == key })
            {
                newValue = inner
                newIndex = index
            } else {
                guard let index = subSchemas.firstIndex(where: { $0.typeName == "map" }) else {
                    throw AvroConversionError.mismatch(avroValue, schema)
                }
                newIndex = index
                newValue = avroValue
            }
            let newAvroValue = try AvroValue(value: newValue, as: subSchemas[newIndex])
            self = .avroUnion(schemaOptions: subSchemas, index: newIndex, newAvroValue)

        case (_, .avroUnion(let subSchemas)):
            // get an exact match
            if let newIndex = subSchemas.firstIndex(where: { $0.typeName == avroValue.schema.typeName }) {
                let newValue = try AvroValue(value: avroValue, as: subSchemas[newIndex])
                self = .avroUnion(schemaOptions: subSchemas, index: newIndex, newValue)
            } else {
                // get a convertible match
                for newIndex in 0 ..< subSchemas.count {
                    if let newValue = try? AvroValue(value: avroValue, as: subSchemas[newIndex]) {
                        self = .avroUnion(schemaOptions: subSchemas, index: newIndex, newValue)
                        return
                    }
                }
                // cannot convert to any of the schemas
                throw AvroConversionError.mismatch(avroValue, schema)
            }

        default:
            throw AvroConversionError.mismatch(avroValue, schema)
        }
    }

    public func encode(encoding: AvroEncoding = .binary) throws -> Data {
        let encoder = GenericAvroEncoder(encoding: encoding)
        return try encoder.encode(self)
    }
}

extension AvroValue: ExpressibleByNilLiteral {
    public init(nilLiteral: ()) {
        self = .avroNull
    }
}

extension AvroValue: ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) {
        self = .avroBoolean(value)
    }
}

extension AvroValue: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: IntegerLiteralType) {
        self = .avroLong(Int64(value))
    }
}

extension AvroValue: ExpressibleByFloatLiteral {
    public init(floatLiteral value: FloatLiteralType) {
        self = .avroDouble(value)
    }
}

extension AvroValue: ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        self = .avroString(value)
    }
}

extension AvroValue: ExpressibleByArrayLiteral {
    public init(arrayLiteral elements: AvroValueConvertible...) {
        self = .avroArray(itemSchema: .avroUnknown, elements)
    }
}

extension AvroValue: ExpressibleByDictionaryLiteral {
    public init(dictionaryLiteral elements: (String, AvroValueConvertible)...) {
        var pairs = [String: AvroValueConvertible](minimumCapacity: elements.count)
        for (key, value) in elements {
            pairs[key] = value
        }
        self = .avroMap(valueSchema: .avroUnknown, pairs)
    }
}
