//
//  AvroValue.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public enum AvroValue {
    public enum SchemaError: Error {
        case invalid
        case conversionFailed
        case mismatch
        case enumSymbolNotFound(String, symbols: [String])
        case fieldNotFound(String)
    }

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
    indirect case avroArray(itemSchema: Schema, [AvroValueConvertible])
    indirect case avroMap(valueSchema: Schema, [String: AvroValueConvertible])
    indirect case avroUnion(schemaOptions: [Schema], index: Int, AvroValueConvertible)
    indirect case avroRecord(Schema, [String: AvroValueConvertible])
    case avroEnum(Schema, index: Int, String)
    case avroFixed(Schema, Data)
    
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
            return .avroArray(itemSchema)
        case .avroMap(let valueSchema, _):
            return .avroMap(valueSchema)
        case .avroUnion(let schemaOptions, _, _):
            return .avroUnion(schemaOptions)
        case .avroRecord(let innerSchema, _):
            return innerSchema
        case .avroEnum(let innerSchema, _, _):
            return innerSchema
        case .avroFixed(let innerSchema, _):
            return innerSchema
        }
    }

    public var int: Int32? {
        guard let castValue = try? AvroValue(value: self, as: .avroInt), case .avroInt(let result) = castValue else {
            return nil
        }
        return result
    }

    public var boolean: Bool? {
        guard let castValue = try? AvroValue(value: self, as: .avroBoolean), case .avroBoolean(let result) = castValue else {
            return nil
        }
        return result
    }

    public var string: String? {
        guard let castValue = try? AvroValue(value: self, as: .avroString), case .avroString(let result) = castValue else {
            return nil
        }
        return result
    }

    public var long: Int64? {
        guard let castValue = try? AvroValue(value: self, as: .avroLong), case .avroLong(let result) = castValue else {
            return nil
        }
        return result
    }

    public var float: Float? {
        guard let castValue = try? AvroValue(value: self, as: .avroFloat), case .avroFloat(let result) = castValue else {
            return nil
        }
        return result
    }

    public var double: Double? {
        guard let castValue = try? AvroValue(value: self, as: .avroDouble), case .avroDouble(let result) = castValue else {
            return nil
        }
        return result
    }
    
    public var bytes: Data? {
        guard let castValue = try? AvroValue(value: self, as: .avroBytes), case .avroBytes(let result) = castValue else {
            return nil
        }
        return result
    }
    
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

    public init(binaryData: Data, as schema: Schema) throws {
        let decoder = BinaryAvroDecoder()
        self = try decoder.decode(binaryData, as: schema)
    }
    public init(jsonData: Data, as schema: Schema) throws {
        let decoder = JsonAvroDecoder()
        self = try decoder.decode(jsonData, as: schema)
    }
}
