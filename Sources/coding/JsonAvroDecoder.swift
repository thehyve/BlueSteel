//
//  JsonAvroDecoder.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 16/01/2019.
//  Copyright © 2019 Gilt Groupe. All rights reserved.
//

import Foundation

open class JsonAvroDecoder : AvroDecoder {
    public init() {
    }

    public func decode(_ data: Data, as schema: Schema) throws -> AvroValue {
        let rawValue = try JSONSerialization.jsonObject(with: data, options: [.allowFragments])
        return try decode(any: rawValue, as: schema)
    }

    public func decode(any value: Any, as schema: Schema) throws -> AvroValue {
        let convertible = try decodeToConvertible(any: value, as: schema)
        return try AvroValue(value: convertible, as: schema)
    }

    private func decodeToConvertible(any: Any, as schema: Schema) throws -> AvroValueConvertible {
        switch any {
        case let result as NSNull:
            return result
        case let result as Int:
            return result
        case let result as String:
            switch schema {
            case .avroBytes, .avroFixed(_, _):
                return Data(bytes: result.unicodeScalars.map { UInt8($0.value & 0xff) })
            default:
                return result
            }
        case let result as Double:
            return result
        case let result as [Any]:
            var avroValues: [AvroValueConvertible] = []
            guard case .avroArray(let itemSchema) = schema else { throw AvroCodingError.schemaMismatch }
            avroValues.reserveCapacity(result.count)
            for value in result {
                let decodedResult = try decode(any: value, as: itemSchema)
                avroValues.append(decodedResult)
            }
            return avroValues
        case let result as [String: Any]:
            switch schema {
            case .avroMap(let valueSchema):
                var avroValues: [String: AvroValueConvertible] = [:]
                avroValues.reserveCapacity(result.count)
                for (key, value) in result {
                    let decodedResult = try decode(any: value, as: valueSchema)
                    avroValues[key] = decodedResult
                }
                return avroValues
            case .avroUnion(let subSchemas):
                guard result.count == 1,
                    let (key, value) = result.first,
                    let index = subSchemas.firstIndex(where: { $0.typeName == key }) else {
                        throw AvroCodingError.unionSizeMismatch
                }
                let newValue = try decode(any: value, as: subSchemas[index])
                return AvroValue.avroUnion(schemaOptions: subSchemas, index: index, newValue)
            case .avroRecord(_, let fieldSchemas):
                var avroValues: [String: AvroValueConvertible] = [:]
                avroValues.reserveCapacity(result.count)
                for field in fieldSchemas {
                    guard let value = result[field.name] else { continue }
                    avroValues[field.name] = try decode(any: value, as: field.schema)
                }
                return avroValues
            default:
                throw AvroCodingError.schemaMismatch
            }
        default:
            throw AvroCodingError.schemaMismatch
        }
    }
}
