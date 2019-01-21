//
//  AvroValue.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

/// Avro value. Use the various initializers from literals
/// for easy creation. Then make those values into valid
/// Avro values by using `init(value:as:)`. To ensure that the
/// resulting value is completely valid, also call
/// `Schema.validate(context:)` on the provided schema in that call.
public enum AvroValue {
    // Primitives
    case avroNull
    case avroBoolean(Bool)
    case avroInt(Int32)
    case avroLong(Int64)
    case avroFloat(Float)
    case avroDouble(Double)
    case avroBytes(Data)
    case avroString(String)

    // Complex Types
    case avroArray(itemSchema: Schema, [AvroValueConvertible])
    case avroMap(valueSchema: Schema, [String: AvroValueConvertible])
    indirect case avroUnion(schemaOptions: [Schema], index: Int, AvroValueConvertible)
    case avroRecord(Schema, [String: AvroValueConvertible])
    case avroEnum(Schema, index: Int, String)
    case avroFixed(Schema, Data)

    /// Schema of the record. This schema may
    /// be invalid until `AvroValue.init(value:as:)` is called.
    public var schema: Schema {
        switch self {
        case .avroNull:
            return .avroNull
        case .avroBoolean(_):
            return .avroBoolean
        case .avroInt(_):
            return .avroInt
        case .avroLong(_):
            return .avroLong
        case .avroFloat(_):
            return .avroFloat
        case .avroDouble(_):
            return .avroDouble
        case .avroBytes(_):
            return .avroBytes
        case .avroString(_):
            return .avroString
        // Complex Types
        case .avroArray(let itemSchema, _):
            return .avroArray(items: itemSchema)
        case .avroMap(let valueSchema, _):
            return .avroMap(values: valueSchema)
        case .avroUnion(let schemaOptions, _, _):
            return .avroUnion(options: schemaOptions)
        case .avroRecord(let innerSchema, _):
            return innerSchema
        case .avroEnum(let innerSchema, _, _):
            return innerSchema
        case .avroFixed(let innerSchema, _):
            return innerSchema
        }
    }

    /// Convert to an integer value if possible
    /// - Returns: 32-bit integer or `nil` if conversion is not possible
    public var int: Int32? {
        guard let castValue = try? AvroValue(value: self, as: .avroInt), case .avroInt(let result) = castValue else {
            return nil
        }
        return result
    }

    /// Convert to an bool value if possible
    /// - Returns: bool or `nil` if conversion is not possible
    public var boolean: Bool? {
        guard let castValue = try? AvroValue(value: self, as: .avroBoolean), case .avroBoolean(let result) = castValue else {
            return nil
        }
        return result
    }

    /// Convert to an string value if possible
    /// - Returns: string or `nil` if conversion is not possible
    public var string: String? {
        guard let castValue = try? AvroValue(value: self, as: .avroString), case .avroString(let result) = castValue else {
            return nil
        }
        return result
    }

    /// Convert to an long value if possible
    /// - Returns: 64-bit integer or `nil` if conversion is not possible
    public var long: Int64? {
        guard let castValue = try? AvroValue(value: self, as: .avroLong), case .avroLong(let result) = castValue else {
            return nil
        }
        return result
    }

    /// Convert to an float value if possible
    /// - Returns: 32-bit float or `nil` if conversion is not possible
    public var float: Float? {
        guard let castValue = try? AvroValue(value: self, as: .avroFloat), case .avroFloat(let result) = castValue else {
            return nil
        }
        return result
    }

    /// Convert to an double value if possible.
    /// - Returns: 64-bit double or `nil` if conversion is not possible
    public var double: Double? {
        guard let castValue = try? AvroValue(value: self, as: .avroDouble), case .avroDouble(let result) = castValue else {
            return nil
        }
        return result
    }

    /// Convert to bytes if possible.
    /// - Returns: byte data or `nil` if conversion is not possible
    public var bytes: Data? {
        guard let castValue = try? AvroValue(value: self, as: .avroBytes), case .avroBytes(let result) = castValue else {
            return nil
        }
        return result
    }

    /// Convert to a map if possible.
    /// - Returns: dicionary or `nil` if conversion is not possible
    public var map: [String: AvroValue]? {
        switch self {
        case .avroMap(_, let result),
             .avroRecord(_, let result):
            return result.mapValues { $0.toAvro() }

        case .avroUnion(_, _, let subValue):
            return subValue.toAvro().map

        default:
            return nil
        }
    }

    /// Convert to an array if possible.
    /// - Returns: array or `nil` if conversion is not possible
    public var array: [AvroValue]? {
        switch self {
        case .avroArray(_, let items):
            return items.map { $0.toAvro() }

        case .avroUnion(_, _, let subValue):
            return subValue.toAvro().array

        default:
            return nil
        }
    }

    /// Decodes data containing an Avro value using given schema.
    /// - Parameters:
    ///   - data: data where the Avro value is encoded
    ///   - as: schema to decode the Avro value with
    ///   - encoding: encoding that is used.
    /// - Throws: a `AvroCodingError` if the data cannot be decoded or it does
    ///           not match the schema.
    public init(data: Data, as schema: Schema, encoding: AvroEncoding = .binary) throws {
        let decoder: AvroDecoder
        switch encoding {
        case .binary:
            decoder = BinaryAvroDecoder()
        case .json:
            decoder = JsonAvroDecoder()
        }
        self = try decoder.decode(data, as: schema)
    }
}
