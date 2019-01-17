//
//  AvroConvertible.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public protocol AvroValueConvertible {
    func toAvro() -> AvroValue

}

public enum AvroConversionError: Error {
    case invalid
    case conversionFailed
    case mismatch
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

extension Dictionary: AvroValueConvertible where Key == String, Value == AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroMap(valueSchema: .avroUnknown, self)
    }
}

extension Array: AvroValueConvertible where Element == AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroArray(itemSchema: .avroUnknown, self)
    }
}

extension NSNull: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroNull
    }
}

extension AvroValue: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return self
    }

    private static func bytesToString(data: Data) -> String? {
        return String(data: data, encoding: .utf8)
    }

    private static func stringToBytes(string: String) -> Data? {
        return string.data(using: .utf8, allowLossyConversion: false)
    }

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
            guard !inner.isNaN && !inner.isInfinite else {
                throw AvroConversionError.invalid
            }
            self = avroValue
        case (.avroFloat(let inner), .avroDouble):
            guard !inner.isNaN && !inner.isInfinite else {
                throw AvroConversionError.invalid
            }
            self = .avroDouble(Double(inner))
        case (.avroFloat(let inner), .avroInt):
            self = .avroInt(Int32(inner))
        case (.avroFloat(let inner), .avroLong):
            self = .avroLong(Int64(inner))

        case (.avroDouble(let inner), .avroDouble):
            guard !inner.isNaN && !inner.isInfinite else {
                throw AvroConversionError.invalid
            }
            self = avroValue
        case (.avroDouble(let inner), .avroInt):
            self = .avroInt(Int32(inner))
        case (.avroDouble(let inner), .avroFloat):
            guard !inner.isNaN && !inner.isInfinite else {
                throw AvroConversionError.invalid
            }
            self = .avroFloat(Float(inner))
        case (.avroDouble(let inner), .avroLong):
            self = .avroLong(Int64(inner))

        case (.avroString(let inner), .avroBytes):
            guard let data = AvroValue.stringToBytes(string: inner) else {
                throw AvroConversionError.conversionFailed
            }
            self = .avroBytes(data)
        case (.avroBytes(let inner), .avroString):
            guard let newValue = AvroValue.bytesToString(data: inner) else {
                throw AvroConversionError.conversionFailed
            }
            self = .avroString(newValue)

        case (.avroFixed(_, let inner), .avroFixed(_, let count)):
            guard inner.count == count else {
                throw AvroConversionError.invalid
            }
            self = .avroFixed(schema, inner)
        case (.avroFixed(_, let inner), .avroBytes):
            self = .avroBytes(inner)
        case (.avroFixed(_, let inner), .avroString):
            guard let newValue = AvroValue.bytesToString(data: inner) else {
                throw AvroConversionError.conversionFailed
            }
            self = .avroString(newValue)

        case (.avroBytes(let value), .avroFixed(_, let count)):
            guard count == value.count else {
                throw AvroConversionError.conversionFailed
            }
            self = .avroFixed(schema, value)
        case (.avroString(let inner), .avroFixed(_, let count)):
            guard let data = AvroValue.stringToBytes(string: inner), data.count == count else {
                throw AvroConversionError.conversionFailed
            }
            self = .avroFixed(schema, data)

        case (.avroArray(_, let values), .avroArray(let subSchema)):
            let newValues = try values.map { try AvroValue(value: $0, as: subSchema) }
            self = .avroArray(itemSchema: subSchema, newValues)

        case (.avroMap(_, let values), .avroMap(let subSchema)),
             (.avroRecord(_, let values), .avroMap(let subSchema)):
            let newValues = try values.mapValues { try AvroValue(value: $0, as: subSchema) }
            self = .avroMap(valueSchema: subSchema, newValues)

        case (.avroEnum(_, let index, let value), .avroEnum(_, let symbols)):
            if value == symbols[index] {
                self = .avroEnum(schema, index: index, value)
            } else {
                guard let newIndex = symbols.firstIndex(of: value) else {
                    throw AvroConversionError.enumSymbolNotFound(value, symbols: symbols)
                }
                self = .avroEnum(schema, index: newIndex, value)
            }
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
                    throw AvroConversionError.mismatch
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
                throw AvroConversionError.mismatch
            }

        default:
            throw AvroConversionError.mismatch
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
