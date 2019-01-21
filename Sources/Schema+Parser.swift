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
            self.init(existingTypes: [:])
        }

        public init(existingTypes: [String: Schema]) {
            namedTypes = existingTypes
        }

        public mutating func parse(_ json: Data) throws -> Schema {
            let jsonAny = try JSONSerialization.jsonObject(with: json, options: [])
            guard let jsonObject = jsonAny as? [String: Any] else {
                throw SchemaCodingError.notAnObject
            }

            return try parse(jsonObject, typeKey:"type", context: AvroCodingContext())
        }

        public mutating func parse(_ json: String) throws -> Schema {
            guard let schemaData = json.data(using: .utf8, allowLossyConversion: false) else {
                throw SchemaCodingError.unknownEncoding
            }

            return try parse(schemaData)
        }

        mutating func parse(_ json: [String: Any], typeKey key: String, context: AvroCodingContext) throws -> Schema {
            var context = context
            context.updateNamespace(jsonObject: json)

            switch json[key] {
            case let typeString as String:
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
                        let schema = try parse(json, typeKey: "values", context: context.nestedIn(type: "map"))
                        return .avroMap(values: schema)

                    case .aArray :
                        let schema = try parse(json, typeKey: "items", context: context.nestedIn(type: "array"))
                        return .avroArray(items: schema)

                    case .aRecord :
                        context = context.nestedIn(type: "record")
                        // Records must be named
                        let recordName = try Parser.getField(name: "name", from: json, context: context) as String
                        let fullRecordName = context.fullName(for: recordName)
                        context.replaceLast(type: fullRecordName)

                        let fields = try Parser.getField(name: "fields", from: json, context: context) as [[String: Any]]
                        let recordFields: [Field] = try fields.map { field in
                            let fieldName = try Parser.getField(name: "name", from: field, context: context) as String
                            let schema = try parse(field, typeKey: "type", context: context.nestedIn(type: fullRecordName, fieldName))
                            var recordField = Field(name: fieldName, schema: schema)
                            if let fieldDefaultValue = field["default"] {
                                let decoder = JsonAvroDecoder()
                                recordField.defaultValue = try? decoder.decode(any: fieldDefaultValue, as: schema)
                            }
                            return recordField
                        }
                        return namedType(name: fullRecordName, schema: .avroRecord(name: fullRecordName, fields: recordFields))

                    case .aEnum :
                        context = context.nestedIn(type: "enum")
                        let enumName = try Parser.getField(name: "name", from: json, context: context) as String
                        let fullEnumName = context.fullName(for: enumName)
                        context.replaceLast(type: fullEnumName)

                        let symbols = try Parser.getField(name: "symbols", from: json, context: context) as [String]

                        return namedType(name: fullEnumName, schema: .avroEnum(name: fullEnumName, symbols: symbols))

                    case .aFixed:
                        context = context.nestedIn(type: "fixed")
                        let fixedName = try Parser.getField(name: "name", from: json, context: context) as String
                        let fullFixedName = context.fullName(for: fixedName)
                        context.replaceLast(type: fullFixedName)
                        let size = try Parser.getField(name: "size", from: json, context: context) as Int

                        return namedType(name: fullFixedName, schema: .avroFixed(name: fullFixedName, size: size))
                    }
                } else {
                    // Schema type is invalid
                    let fullTypeName = context.fullName(for: typeString)

                    guard let cachedSchema = namedTypes[fullTypeName] else {
                        throw SchemaCodingError.unknownType(fullTypeName, context)
                    }
                    return cachedSchema
                }

            case let dict as [String: Any]:
                return try parse(dict, typeKey: "type", context: context)

            case let unionSchema as [Any]:
                // Union
                let schemas: [Schema] = try unionSchema.enumerated()
                    .map { subSchema in
                        switch subSchema.element {
                        case let value as String:
                            return try parse(["type": value], typeKey: "type", context: context.nestedIn(type: "union[\(subSchema.offset)]"))
                        case let value as [String: Any]:
                            return try parse(value, typeKey: "type", context: context.nestedIn(type: "union[\(subSchema.offset)]"))
                        default:
                            throw SchemaCodingError.typeMismatch(context.nestedIn(type: "union[\(subSchema.offset)]"))
                        }
                    }
                return .avroUnion(options: schemas)

            default:
                throw SchemaCodingError.typeMismatch(context.nestedIn(type: "unknown"))
            }
        }

        private mutating func namedType(name: String, schema: Schema) -> Schema {
            namedTypes[name] = schema
            return schema
        }

        private static func getField<T>(name field: String, from dict: [String: Any], context: AvroCodingContext) throws -> T {
            guard let value = dict[field] else {
                throw SchemaCodingError.missingField(field, context)
            }
            guard let typedValue = value as? T else {
                throw SchemaCodingError.typeMismatch(context)
            }
            return typedValue
        }

    }
}
