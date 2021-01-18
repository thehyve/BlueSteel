//
//  SchemaFormatter.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 17/01/2019.
//  Copyright © 2019 RADAR-base. All rights reserved.
//

import Foundation

extension Schema {
    /// Formats a Schema to string. It does not output named types more than once, in subsequent
    /// occurrences it only prints the name of the type.
    /// If multiple associated files are written, the types that have already been encountered
    /// can be passed to the formatter to prevent that they are redefined.
    public struct Formatter {

        /// Types that have already been encountered and do not have to printed again.
        public var existingTypes: Set<String>
        private var context: [String]

        public init () {
            self.init(existingTypeNames: [])
        }

        /// Initialize the Formatter with a set of type names already printed elsewhere.
        /// - Parameters:
        ///     - existingTypes: names of previously printed types.
        public init(existingTypeNames existingTypes: Set<String>) {
            self.existingTypes = existingTypes
            self.context = []
        }

        /// Format given schema as a valid JSON string.
        /// - Parameters:
        ///     - schema: schema to format.
        /// - Throws: `SchemaCodingError` if the schema is not valid or
        ///           `AvroConversionError` if the schema contains default
        ///           values that cannot be converted to valid Avro values.
        public mutating func jsonString(_ schema: Schema) throws -> String {
            return try jsonString(schema, context: AvroCodingContext())
        }

        private mutating func jsonString(_ schema: Schema, context: AvroCodingContext) throws -> String {
            switch schema {
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

            case .avroArray(let itemSchema):
                let itemString = try jsonString(itemSchema, context: context.nestedIn(type: "array"))
                return "{\"type\":\"array\",\"items\":\(itemString)}"

            case .avroMap(let valueSchema) :
                let valueString = try jsonString(valueSchema, context: context.nestedIn(type: "map"))
                return "{\"type\":\"map\",\"values\":\(valueString)}"

            case .avroEnum(let name, let symbols):
                if existingTypes.insert(name).inserted {
                    var context = context
                    let nsName = formatName(fullName: name, context: &context)
                    let symbolString = symbols.map { "\"\($0)\"" }.joined(separator: ",")
                    return "{\(nsName),\"type\":\"enum\",\"symbols\":[\(symbolString)]}"
                } else {
                    return "\"\(context.extractNamespace(fullName: name).readableName)\""
                }

            case .avroRecord(let name, let fields) :
                if existingTypes.insert(name).inserted {
                    var context = context.nestedIn(type: name)
                    let nsName = formatName(fullName: name, context: &context)
                    let fieldsString = try fields.map { field in
                        let fieldSchemaString = try jsonString(field.schema, context: context.nestedIn(type: field.name))
                        let defaultValueString: String
                        if let defaultValue = field.defaultValue {
                            let avroValue = try AvroValue(value: defaultValue, as: field.schema)
                            let avroData = try avroValue.encode(encoding: .json)
                            guard let encodedValue = String(data: avroData, encoding: .utf8) else {
                                throw SchemaCodingError.unknownEncoding
                            }
                            defaultValueString = ",\"default\":\(encodedValue)"
                        } else {
                            defaultValueString = ""
                        }

                        return "{\"name\":\"\(field.name)\",\"type\":\(fieldSchemaString)\(defaultValueString)}"
                        }.joined(separator: ",")
                    return "{\(nsName),\"type\":\"record\",\"fields\":[\(fieldsString)]}"
                } else {
                    return "\"\(context.extractNamespace(fullName: name).readableName)\""
                }

            case .avroFixed(let name, let size) :
                if existingTypes.insert(name).inserted {
                    var context = context
                    let nsName = formatName(fullName: name, context: &context)
                    return "{\(nsName),\"type\":\"fixed\",\"size\":\(size)}"
                } else {
                    return "\"\(context.extractNamespace(fullName: name).readableName)\""
                }

            case .avroUnion(let unionSchemas) :
                let unionString = try unionSchemas.enumerated()
                    .map { try jsonString($1, context: context.nestedIn(type: "union[\($0)]")) }
                    .joined(separator: ",")
                return "[\(unionString)]"

            case .avroUnknown:
                throw SchemaCodingError.unknownType("<unknown>", context)
            }
        }

        private func formatName(fullName: String, context: inout AvroCodingContext) -> String {
            let nsName = context.extractNamespace(fullName: fullName)

            if let namespace = nsName.namespace {
                context.namespace = namespace
                return "\"namespace\":\"\(namespace)\",\"name\":\"\(nsName.name)\""
            } else {
                return "\"name\":\"\(nsName.name)\""
            }
        }
    }
}
