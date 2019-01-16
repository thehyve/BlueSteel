//
//  Schema.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public enum AvroType: String {
    // Primitives
    case aNull = "null"
    case aBoolean = "boolean"
    case aInt = "int"
    case aLong = "long"
    case aFloat = "float"
    case aDouble = "double"
    case aString = "string"
    case aBytes = "bytes"

    // Complex
    case aEnum = "enum"
    case aFixed = "fixed"
    case aRecord = "record"
    case aArray = "array"
    case aMap = "map"
}

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

    public func canonicalString(_ existingTypes: inout Set<String>) -> String?
    {
        switch self {
        case .avroNull :
            return "\"null\""
        case .avroBoolean :
            return "\"boolean\""
        case .avroInt :
            return "\"int\""
        case .avroLong :
            return "\"long\""
        case .avroFloat :
            return "\"float\""
        case .avroDouble :
            return "\"double\""
        case .avroString :
            return "\"string\""
        case .avroBytes :
            return "\"bytes\""

        case .avroArray(let value) :
            if let arrayPCF = value.canonicalString(&existingTypes) {
                return "{\"type\":\"array\",\"items\":" + arrayPCF + "}"
            } else {
                return nil
            }

        case .avroMap(let value) :
            if let mapPCF = value.canonicalString(&existingTypes) {
                return "{\"type\":\"map\",\"values\":" + mapPCF + "}"
            } else {
                return nil
            }

        case .avroEnum(let name, let enumValues) :
            if existingTypes.insert(name).inserted {
                var str = "{\"name\":\"" + name + "\",\"type\":\"enum\",\"symbols\":["
                var first = true
                for val in enumValues {
                    if first {
                        str += "\"\(val)\""
                        first = false
                    } else {
                        str += ",\"\(val)\""
                    }
                }
                str += "]}"
                return str
            } else {
                return "\"" + name + "\""
            }

        case .avroRecord(let name, let fields) :
            if existingTypes.insert(name).inserted {
                var str = "{\"name\":\"" + name + "\",\"type\":\"record\",\"fields\":["
                var first = true
                for field in fields {
                    if !first {
                        str += ","
                    } else {
                        first = false
                    }

                    if let fieldPCF = field.canonicalString(&existingTypes) {
                        str += fieldPCF
                    } else {
                        return nil
                    }
                }
                str += "]}"
                return str
            } else {
                return "\"" + name + "\""
            }

        case .avroFixed(let name, let size) :
            if existingTypes.insert(name).inserted {
                return "{\"name\":\"" + name + "\",\"type\":\"fixed\",\"size\":\(size)}"
            } else {
                return "\"" + name + "\""
            }

        case .avroUnion(let unionSchemas) :
            var str = "["
            var first = true
            for uschema in unionSchemas {
                if !first {
                    str += ","
                } else {
                    first = false
                }

                if let unionPCF = uschema.canonicalString(&existingTypes) {
                    str += unionPCF
                } else {
                    return nil
                }
            }
            str += "]"
            return str

        case .avroField(let fieldName, let fieldType, let fieldDefault) :
            if let fieldPCF = fieldType.canonicalString(&existingTypes) {
                var str = "{\"name\":\"" + fieldName + "\",\"type\":" + fieldPCF
                if let fieldDefault = fieldDefault,
                    let avroData = try? fieldDefault.encode(encoding: .json),
                    let encodedValue = String(data: avroData, encoding: .utf8) {
                    str += ",\"default\":" + encodedValue
                }
                return str + "}"
            } else {
                print(fieldName)
                return nil
            }
        case .avroUnknown:
            return nil
        }
    }

    public init(_ json: Data) throws {
        var cached: [String: Schema] = [:]

        guard let jsonObject = (try JSONSerialization.jsonObject(with: json, options: [])) as? [String: Any] else {
            throw SchemaParsingError.notAnObject
            
        }

        self = try Schema(jsonObject, typeKey:"type", namespace: nil, cachedSchemas: &cached)
    }

    public init(_ json: String) throws {
        guard let schemaData = json.data(using: .utf8, allowLossyConversion: false) else { throw SchemaParsingError.unknownEncoding }
        
        try self.init(schemaData)
    }

    init(_ json: [String: Any], typeKey key: String, namespace ns: String?, cachedSchemas cache: inout [String: Schema]) throws {
        var schemaNamespace: String?
        if let jsonNamespace = json["namespace"] as? String {
            schemaNamespace = jsonNamespace
        } else {
            schemaNamespace = ns
        }

        if let typeString = json[key] as? String {
            let avroType = AvroType(rawValue: typeString)

            if let avroType = avroType {
                switch avroType {
                case .aBoolean :
                    self = .avroBoolean
                case .aInt :
                    self = .avroInt
                case .aLong :
                    self = .avroLong
                case .aFloat :
                    self = .avroFloat
                case .aDouble :
                    self = .avroDouble
                case .aString :
                    self = .avroString
                case .aNull :
                    self = .avroNull
                case .aBytes :
                    self = .avroBytes

                case .aMap :
                    let schema = try Schema(json, typeKey: "values", namespace: schemaNamespace, cachedSchemas: &cache)

                    self = .avroMap(schema)
                case .aArray :
                    let schema = try Schema(json, typeKey: "items", namespace: schemaNamespace, cachedSchemas: &cache)

                    self = .avroArray(schema)

                case .aRecord :
                    // Records must be named
                    guard let recordName = json["name"] as? String else {
                        throw SchemaParsingError.missingField("record name")
                    }
                    let fullRecordName = Schema.assembleFullName(schemaNamespace, name: recordName)

                    guard let fields = json["fields"] as? [[String: Any]] else {
                        throw SchemaParsingError.missingField("record fields")
                    }
                    var recordFields: [Schema] = []

                    for field in fields {
                        guard let fieldName = field["name"] as? String else {
                            throw SchemaParsingError.missingField("field name")
                        }
                        let schema = try Schema(field, typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)
                        
                        let fieldDefault: AvroValue?
                        
                        switch field["default"] {
                        case is NSNull:
                            fieldDefault = .avroNull
                        case let value as Int:
                            fieldDefault = .avroLong(Int64(value))
                        case let value as String:
                            fieldDefault = .avroString(value)
                        case let value as Double:
                            fieldDefault = .avroDouble(value)
                        default:
                            fieldDefault = nil
                        }

                        recordFields.append(.avroField(fieldName, schema, fieldDefault))
                    }
                    self = .avroRecord(fullRecordName, recordFields)
                    cache[fullRecordName] = self

                case .aEnum :
                    guard let enumName = json["name"] as? String else {
                        throw SchemaParsingError.missingField("enum name")
                    }
                    guard let symbols = json["symbols"] as? [Any] else {
                        throw SchemaParsingError.missingField("enum symbols")
                    }
                    var symbolStrings: [String] = []
                    for sym in symbols {
                        guard let symbol = sym as? String else {
                            throw SchemaParsingError.typeMismatch
                        }
                        symbolStrings.append(symbol)
                    }

                    let fullEnumName = Schema.assembleFullName(schemaNamespace, name: enumName)

                    self = .avroEnum(fullEnumName, symbolStrings)
                    cache[fullEnumName] = self

                case .aFixed:
                    guard let fixedName = json["name"] as? String else {
                        throw SchemaParsingError.missingField("fixed name")
                    }
                    guard let size = json["size"] as? Int else {
                        throw SchemaParsingError.missingField("fixed size")
                    }
                    let fullFixedName = Schema.assembleFullName(schemaNamespace, name: fixedName)
                    self = .avroFixed(fullFixedName, size)
                    cache[fullFixedName] = self
                }
            } else {
                // Schema type is invalid
                let fullTypeName = Schema.assembleFullName(schemaNamespace, name: typeString)
                
                guard let cachedSchema = cache[fullTypeName] else {
                    throw SchemaParsingError.unknownType(fullTypeName)
                }
                self = cachedSchema
            }
        }
        else if let dict = json[key] as? [String: Any] {
            self = try Schema(dict, typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)
        }
        else if let unionSchema = json[key] as? [Any] {
            // Union
            var schemas: [Schema] = []
            for def in unionSchema {
                let subSchema: Schema
                switch def {
                case let value as String:
                    subSchema = try Schema(["type": value], typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)
                case let value as [String: Any]:
                    subSchema = try Schema(value, typeKey: "type", namespace: schemaNamespace, cachedSchemas: &cache)
                default:
                    throw SchemaParsingError.typeMismatch
                }
                schemas.append(subSchema)
            }
            self = .avroUnion(schemas)
        } else {
            throw SchemaParsingError.typeMismatch
        }
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

enum SchemaParsingError: Error {
    case typeMismatch
    case notAnObject
    case unknownEncoding
    case unknownType(String)
    case missingField(String)
}

extension Schema: Equatable, Hashable {
    public static func ==(lhs: Schema, rhs: Schema) -> Bool {
        switch (lhs, rhs) {
        case (.avroBoolean, .avroBoolean),
             (.avroInt, .avroInt),
             (.avroLong, .avroLong),
             (.avroFloat, .avroFloat),
             (.avroDouble, .avroDouble),
             (.avroBytes, .avroBytes),
             (.avroString, .avroString),
             (.avroNull, .avroNull),
             (.avroUnknown, .avroUnknown):
            return true
        case (.avroArray(let lschema), .avroArray(let rschema)),
             (.avroMap(let lschema), .avroMap(let rschema)):
            return lschema == rschema
        case (.avroRecord(let lname, let lschema), .avroRecord(let rname, let rschema)):
            return lname == rname && lschema == rschema
        case (.avroField(let lname, let lschema, let ldefault), .avroField(let rname, let rschema, let rdefault)):
            return lname == rname && lschema == rschema && ldefault == rdefault
        case (.avroUnion(let lschemas), .avroUnion(let rschemas)):
            return lschemas == rschemas
        case (.avroEnum(let lname, let lsymbols), .avroEnum(let rname, let rsymbols)):
            return lname == rname && lsymbols == rsymbols
        case (.avroFixed(let lname, let lsize), .avroFixed(let rname, let rsize)):
            return lname == rname && lsize == rsize
        default:
            return false
        }
    }
    
    private func hash(into hasher: inout Hasher, excludingTypes: inout Set<String>) {
        switch self {
        case .avroNull:
            hasher.combine(1)
        case .avroBoolean:
            hasher.combine(2)
        case .avroInt:
            hasher.combine(3)
        case .avroLong:
            hasher.combine(4)
        case .avroFloat:
            hasher.combine(5)
        case .avroBytes:
            hasher.combine(6)
        case .avroString:
            hasher.combine(7)
        case .avroArray(let schema):
            hasher.combine(8)
            schema.hash(into: &hasher, excludingTypes: &excludingTypes)
        case .avroRecord(let name, let fields):
            hasher.combine(9)
            hasher.combine(name)
            if excludingTypes.insert(name).inserted {
                for field in fields {
                    field.hash(into: &hasher, excludingTypes: &excludingTypes)
                }
            }
        case .avroField(let name, let schema, let fieldDefault):
            hasher.combine(10)
            hasher.combine(name)
            schema.hash(into: &hasher, excludingTypes: &excludingTypes)
            if let fieldDefault = fieldDefault {
                switch fieldDefault {
                case .avroNull:
                    hasher.combine(1)
                case .avroLong(let value):
                    hasher.combine(2)
                    hasher.combine(value)
                case .avroDouble(let value):
                    hasher.combine(3)
                    hasher.combine(value)
                case .avroString(let value):
                    hasher.combine(4)
                    hasher.combine(value)
                default:
                    hasher.combine(5)
                }
            } else {
                hasher.combine(0)
            }
        case .avroEnum(let name, let symbols):
            hasher.combine(11)
            hasher.combine(name)
            if excludingTypes.insert(name).inserted {
                hasher.combine(symbols)
            }
        case .avroFixed(let name, let size):
            hasher.combine(12)
            hasher.combine(name)
            if excludingTypes.insert(name).inserted {
                hasher.combine(size)
            }
        case .avroUnion(let subSchemas):
            hasher.combine(13)
            for subSchema in subSchemas {
                subSchema.hash(into: &hasher, excludingTypes: &excludingTypes)
            }
        case .avroDouble:
            hasher.combine(14)
        case .avroMap(let schema):
            hasher.combine(15)
            schema.hash(into: &hasher, excludingTypes: &excludingTypes)
        case .avroUnknown:
            hasher.combine(16)
        }
    }
    
    public func hash(into hasher: inout Hasher) {
        var excludingTypes: Set<String> = []
        hash(into: &hasher, excludingTypes: &excludingTypes)
    }
}

