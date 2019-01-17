//
//  SchemaParser.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 17/01/2019.
//  Copyright © 2019 RADAR-base. All rights reserved.
//

import Foundation

fileprivate enum AvroType: String {
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

extension Schema {
    public struct Parser {
        public var namedTypes: [String: Schema]

        public init() {
            namedTypes = [:]
        }

        public init(existingTypes: [String: Schema]) {
            namedTypes = existingTypes
        }

        public mutating func parse(_ json: Data) throws -> Schema {
            let jsonAny = try JSONSerialization.jsonObject(with: json, options: [])
            guard let jsonObject = jsonAny as? [String: Any] else {
                throw SchemaCodingError.notAnObject
            }

            return try parse(jsonObject, typeKey:"type", namespace: nil)
        }

        public mutating func parse(_ json: String) throws -> Schema {
            guard let schemaData = json.data(using: .utf8, allowLossyConversion: false) else {
                throw SchemaCodingError.unknownEncoding
            }

            return try parse(schemaData)
        }

        mutating func parse(_ json: [String: Any], typeKey key: String, namespace ns: String?) throws -> Schema {
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
                        return .avroBoolean
                    case .aInt :
                        return .avroInt
                    case .aLong :
                        return .avroLong
                    case .aFloat :
                        return .avroFloat
                    case .aDouble :
                        return .avroDouble
                    case .aString :
                        return .avroString
                    case .aNull :
                        return .avroNull
                    case .aBytes :
                        return .avroBytes

                    case .aMap :
                        let schema = try parse(json, typeKey: "values", namespace: schemaNamespace)
                        return .avroMap(values: schema)

                    case .aArray :
                        let schema = try parse(json, typeKey: "items", namespace: schemaNamespace)
                        return .avroArray(items: schema)

                    case .aRecord :
                        // Records must be named
                        guard let recordName = json["name"] as? String else {
                            throw SchemaCodingError.missingField("record name")
                        }
                        let fullRecordName = Schema.assembleFullName(schemaNamespace, name: recordName)

                        guard let fields = json["fields"] as? [[String: Any]] else {
                            throw SchemaCodingError.missingField("record fields")
                        }
                        let recordFields: [Field] = try fields.map { field in
                            guard let fieldName = field["name"] as? String else {
                                throw SchemaCodingError.missingField("field name")
                            }
                            let schema = try parse(field, typeKey: "type", namespace: schemaNamespace)
                            var recordField = Field(name: fieldName, schema: schema)
                            if let fieldDefaultValue = field["default"] {
                                let decoder = JsonAvroDecoder()
                                recordField.defaultValue = try? decoder.decode(any: fieldDefaultValue, as: schema)
                            }

                            return recordField
                        }
                        let result = Schema.avroRecord(name: fullRecordName, fields: recordFields)
                        namedTypes[fullRecordName] = result
                        return result

                    case .aEnum :
                        guard let enumName = json["name"] as? String else {
                            throw SchemaCodingError.missingField("enum name")
                        }
                        guard let symbols = json["symbols"] as? [Any] else {
                            throw SchemaCodingError.missingField("enum symbols")
                        }
                        var symbolStrings: [String] = []
                        for sym in symbols {
                            guard let symbol = sym as? String else {
                                throw SchemaCodingError.typeMismatch
                            }
                            symbolStrings.append(symbol)
                        }

                        let fullEnumName = Schema.assembleFullName(schemaNamespace, name: enumName)

                        let result = Schema.avroEnum(name: fullEnumName, symbols: symbolStrings)
                        namedTypes[fullEnumName] = result
                        return result

                    case .aFixed:
                        guard let fixedName = json["name"] as? String else {
                            throw SchemaCodingError.missingField("fixed name")
                        }
                        guard let size = json["size"] as? Int else {
                            throw SchemaCodingError.missingField("fixed size")
                        }
                        let fullFixedName = Schema.assembleFullName(schemaNamespace, name: fixedName)
                        let result = Schema.avroFixed(name: fullFixedName, size: size)
                        namedTypes[fullFixedName] = result
                        return result
                    }
                } else {
                    // Schema type is invalid
                    let fullTypeName = Schema.assembleFullName(schemaNamespace, name: typeString)

                    guard let cachedSchema = namedTypes[fullTypeName] else {
                        throw SchemaCodingError.unknownType(fullTypeName)
                    }
                    return cachedSchema
                }
            }
            else if let dict = json[key] as? [String: Any] {
                return try parse(dict, typeKey: "type", namespace: schemaNamespace)
            }
            else if let unionSchema = json[key] as? [Any] {
                // Union
                let schemas: [Schema] = try unionSchema.map { def in
                    switch def {
                    case let value as String:
                        return try parse(["type": value], typeKey: "type", namespace: schemaNamespace)
                    case let value as [String: Any]:
                        return try parse(value, typeKey: "type", namespace: schemaNamespace)
                    default:
                        throw SchemaCodingError.typeMismatch
                    }
                }
                return .avroUnion(options: schemas)
            } else {
                throw SchemaCodingError.typeMismatch
            }
        }
    }
}
