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
    
    public func encode(encoding: GenericAvroEncoder.Encoding = .binary) throws -> Data {
        let encoder = GenericAvroEncoder(encoding: encoding)
        return try encoder.encode(self)
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
    
    private static func stringToUnicodeScalars(string: String) -> Data {
        return Data(bytes: string.unicodeScalars.map { UInt8($0.value & 0xff) })
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
                throw SchemaError.invalid
            }
            self = avroValue
        case (.avroFloat(let inner), .avroDouble):
            guard !inner.isNaN && !inner.isInfinite else {
                throw SchemaError.invalid
            }
            self = .avroDouble(Double(inner))
        case (.avroFloat(let inner), .avroInt):
            self = .avroInt(Int32(inner))
        case (.avroFloat(let inner), .avroLong):
            self = .avroLong(Int64(inner))
            
        case (.avroDouble(let inner), .avroDouble):
            guard !inner.isNaN && !inner.isInfinite else {
                throw SchemaError.invalid
            }
            self = avroValue
        case (.avroDouble(let inner), .avroInt):
            self = .avroInt(Int32(inner))
        case (.avroDouble(let inner), .avroFloat):
            guard !inner.isNaN && !inner.isInfinite else {
                throw SchemaError.invalid
            }
            self = .avroFloat(Float(inner))
        case (.avroDouble(let inner), .avroLong):
            self = .avroLong(Int64(inner))
            
            
        case (.avroString(let inner), .avroBytes):
            guard let data = AvroValue.stringToBytes(string: inner) else {
                throw SchemaError.conversionFailed
            }
            self = .avroBytes(data)
        case (.avroBytes(let inner), .avroString):
            guard let newValue = AvroValue.bytesToString(data: inner) else {
                throw SchemaError.conversionFailed
            }
            self = .avroString(newValue)
            
        case (.avroFixed(_, let inner), .avroBytes):
            self = .avroBytes(inner)
        case (.avroFixed(_, let inner), .avroString):
            guard let newValue = AvroValue.bytesToString(data: inner) else {
                throw SchemaError.conversionFailed
            }
            self = .avroString(newValue)
            
        case (.avroBytes(let value), .avroFixed(_, let count)):
            guard count == value.count else {
                throw SchemaError.conversionFailed
            }
            self = .avroFixed(schema, value)
        case (.avroString(let inner), .avroFixed(_, let count)):
            guard let data = AvroValue.stringToBytes(string: inner), data.count == count else {
                throw SchemaError.conversionFailed
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
                    throw SchemaError.enumSymbolNotFound(value, symbols: symbols)
                }
                self = .avroEnum(schema, index: newIndex, value)
            }
        case (.avroString(let value), .avroEnum(_, let symbols)):
            guard let newIndex = symbols.firstIndex(of: value) else {
                throw SchemaError.enumSymbolNotFound(value, symbols: symbols)
            }
            self = .avroEnum(schema, index: newIndex, value)
        case (.avroEnum(_, _, let value), .avroString):
            self = .avroString(value)
        case (.avroRecord(_, let fields), .avroRecord(_, let subSchemas)),
             (.avroMap(_, let fields), .avroRecord(_, let subSchemas)):
            var newValue: [String: AvroValueConvertible] = [:]
            for subSchema in subSchemas {
                switch subSchema {
                case .avroField(let fieldName, let fieldSchema, let fieldDefault):
                    if let fieldValue = fields[fieldName] {
                        newValue[fieldName] = try AvroValue(value: fieldValue, as: fieldSchema)
                    } else if let fieldDefault = fieldDefault {
                        if case .avroString(let fieldDefaultString) = fieldDefault {
                            if case .avroBytes = fieldSchema {
                                newValue[fieldName] = AvroValue.avroBytes(AvroValue.stringToUnicodeScalars(string: fieldDefaultString))
                                continue
                            } else if case .avroFixed(_, _) = fieldSchema {
                                newValue[fieldName] = try AvroValue(value: AvroValue.stringToUnicodeScalars(string: fieldDefaultString), as: fieldSchema)
                                continue
                            }
                        }
                        newValue[fieldName] = try AvroValue(value: fieldDefault, as: fieldSchema)
                    } else {
                        throw SchemaError.fieldNotFound(fieldName)
                    }
                default:
                    throw SchemaError.invalid
                }
            }
            self = .avroRecord(schema, newValue)
        case (.avroUnion(_, let index, let inner), .avroUnion(let subSchemas)):
            guard index < subSchemas.count,
                inner.toAvro().schema.typeName == subSchemas[index].typeName else
            {
                self = try AvroValue(value: inner, as: schema)
                return
            }
            let newValue = try AvroValue(value: inner, as: subSchemas[index])
            self = .avroUnion(schemaOptions: subSchemas, index: index, newValue)
        case (.avroUnion(_, _, let inner), _):
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
                    throw SchemaError.mismatch
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
                throw SchemaError.mismatch
            }
        default:
            throw SchemaError.mismatch
        }
    }
}

extension AvroValue: Equatable {
    public static func ==(lhs: AvroValue, rhs: AvroValue) -> Bool {
        switch (lhs, rhs) {
        case (.avroNull, .avroNull):
            return true
        case (.avroBoolean(let lvalue), .avroBoolean(let rvalue)):
            return lvalue == rvalue
        case (.avroInt(let lvalue), .avroInt(let rvalue)):
            return lvalue == rvalue
        case (.avroLong(let lvalue), .avroLong(let rvalue)):
            return lvalue == rvalue
        case (.avroFloat(let lvalue), .avroFloat(let rvalue)):
            return lvalue == rvalue
        case (.avroDouble(let lvalue), .avroDouble(let rvalue)):
            return lvalue == rvalue
        case (.avroString(let lvalue), .avroString(let rvalue)):
            return lvalue == rvalue
        case (.avroBytes(let lvalue), .avroBytes(let rvalue)):
            return lvalue == rvalue
        case (.avroFixed(let lschema, let lvalue), .avroFixed(let rschema, let rvalue)):
            return lschema == rschema && lvalue == rvalue
        case (.avroMap(let lschema, let lvalues), .avroMap(let rschema, let rvalues)):
            guard lschema == rschema, lvalues.keys == rvalues.keys else { return false }
            for (key, lvalue) in lvalues {
                guard let rvalue = rvalues[key],
                    let lAvroValue = try? AvroValue(value: lvalue, as: lschema),
                    let rAvroValue = try? AvroValue(value: rvalue, as: rschema),
                    lAvroValue == rAvroValue else {
                    return false
                }
            }
            return true
    
        case (.avroRecord(.avroRecord(let lname, let lfieldSchemas), let lvalues), .avroRecord(.avroRecord(let rname, let rfieldSchemas), let rvalues)):
            guard lname == rname, lfieldSchemas == rfieldSchemas else { return false }
            for fieldSchema in lfieldSchemas {
                guard case .avroField(let fieldName, let innerSchema, _) = fieldSchema,
                    let lvalue = lvalues[fieldName],
                    let rvalue = rvalues[fieldName],
                    let lAvroValue = try? AvroValue(value: lvalue, as: innerSchema),
                    let rAvroValue = try? AvroValue(value: rvalue, as: innerSchema),
                    lAvroValue == rAvroValue else {
                        return false
                }
            }
            return true
            
        case (.avroArray(let lschema, let lvalues), .avroArray(let rschema, let rvalues)):
            guard lschema == rschema, lvalues.count == rvalues.count else { return false }
            for i in 0 ..< lvalues.count {
                guard let lAvroValue = try? AvroValue(value: lvalues[i], as: lschema),
                    let rAvroValue = try? AvroValue(value: rvalues[i], as: rschema),
                    lAvroValue == rAvroValue else {
                        return false
                }
            }
            return true

        case (.avroEnum(let lschema, let lindex, let lvalue), .avroEnum(let rschema, let rindex, let rvalue)):
            return lschema == rschema && lindex == rindex && lvalue == rvalue

        case (.avroUnion(let lschema, let lindex, let lvalue), .avroUnion(let rschema, let rindex, let rvalue)):
            guard lschema == rschema,
                lindex == rindex,
                let lAvroValue = try? AvroValue(value: lvalue, as: lschema[lindex]),
                let rAvroValue = try? AvroValue(value: rvalue, as: rschema[rindex]) else {
                    return false
            }
            return lAvroValue == rAvroValue
        default:
            return false
        }
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
    public init(dictionaryLiteral elements:(String, AvroValueConvertible)...) {
        var tmp = [String: AvroValueConvertible](minimumCapacity: elements.count)
        for kv in elements {
            tmp[kv.0] = kv.1
        }
        self = .avroMap(valueSchema: .avroUnknown, tmp)
    }
}
