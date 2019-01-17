//
//  SchemaFormatter.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 17/01/2019.
//  Copyright © 2019 RADAR-base. All rights reserved.
//

import Foundation

extension Schema {
    public struct Formatter {
        var existingTypes: Set<String>
        
        public init () {
            existingTypes = []
        }
        
        public init(existingTypeNames existingTypes: Set<String>) {
            self.existingTypes = existingTypes
        }
        
        public mutating func jsonString(_ schema: Schema) throws -> String {
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
                
            case .avroArray(let itemSchema) :
                let itemString = try jsonString(itemSchema)
                return "{\"type\":\"array\",\"items\":\(itemString)}"
                
            case .avroMap(let valueSchema) :
                let valueString = try jsonString(valueSchema)
                return "{\"type\":\"map\",\"values\":\(valueString)}"

            case .avroEnum(let name, let symbols) :
                if existingTypes.insert(name).inserted {
                    let symbolString = symbols.map { "\"\($0)\"" }.joined(separator: ",")
                    return "{\"name\":\"\(name)\",\"type\":\"enum\",\"symbols\":[\(symbolString)]}"
                } else {
                    return "\"\(name)\""
                }
                
            case .avroRecord(let name, let fields) :
                if existingTypes.insert(name).inserted {
                    let fieldString = try fields.map { try jsonString($0) }.joined(separator: ",")
                    return "{\"name\":\"\(name)\",\"type\":\"record\",\"fields\":[\(fieldString)]}"
                } else {
                    return "\"\(name)\""
                }
                
            case .avroFixed(let name, let size) :
                if existingTypes.insert(name).inserted {
                    return "{\"name\":\"\(name)\",\"type\":\"fixed\",\"size\":\(size)}"
                } else {
                    return "\"\(name)\""
                }
                
            case .avroUnion(let unionSchemas) :
                let unionString = try unionSchemas.map { try jsonString($0) }.joined(separator: ",")
                return "[\(unionString)]"
                
            case .avroField(let fieldName, let fieldType, let fieldDefault) :
                let fieldTypeString = try jsonString(fieldType)
                let fieldDefaultString: String
                if let fieldDefault = fieldDefault {
                    let avroData = try AvroValue(value: fieldDefault, as: fieldType).encode(encoding: .json)
                    guard let encodedValue = String(data: avroData, encoding: .utf8) else {
                        throw Parser.CodingError.unknownEncoding
                    }
                    fieldDefaultString = ",\"default\":\(encodedValue)"
                } else {
                    fieldDefaultString = ""
                }

                return "{\"name\":\"\(fieldName)\",\"type\":\(fieldTypeString)\(fieldDefaultString)}"
            case .avroUnknown:
                throw Parser.CodingError.unknownType("unknown")
            }
        }
    }
}
