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
        let decodedValue = try decode(any: rawValue, as: schema)
        return try AvroValue(value: decodedValue, as: schema)
    }
    
    public func decode(any: Any, as schema: Schema) throws -> AvroValueConvertible {
        switch any {
        case let result as NSNull:
            return result
        case let result as Int:
            return result
        case let result as String:
            switch schema {
            case .avroBytes, .avroFixed:
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
                for fSchema in fieldSchemas {
                    guard case .avroField(let fieldName, let subSchema, _) = fSchema else {
                        throw AvroCodingError.schemaMismatch
                        
                    }
                    guard let value = result[fieldName] else { continue }
                    let decodedResult = try decode(any: value, as: subSchema)
                    avroValues[fieldName] = decodedResult
                }
                return avroValues
            }
        default:
            throw AvroCodingError.schemaMismatch
        }
    }
}

open class BinaryAvroDecoderContext {
    let data: Data
    var offset: Int
    
    public init(_ data: Data) {
        self.data = data
        self.offset = 0
    }
    
    open func decodeNull() {
        // Nulls aren't actually encoded.
        return
    }
    
    open func decodeBoolean() -> Bool? {
        guard offset + 1 <= data.count  else { return nil }
        
        defer { offset += 1 }
        return data[offset] > 0
    }
    
    open func decodeDouble() -> Double? {
        guard offset + 8 <= data.count else { return nil }
        
        defer { offset += 8 }
        let bitValue: UInt64 = data.advanced(by: offset).withUnsafeBytes { $0.pointee }
        var value = UInt64(littleEndian: bitValue)
        
        return withUnsafePointer(to: &value) { ptr in
            ptr.withMemoryRebound(to: Double.self, capacity: 1) { $0.pointee }
        }
    }
    
    open func decodeFloat() -> Float? {
        guard offset + 4 <= data.count else { return nil }
        
        defer { offset += 4 }
        let bitValue: UInt32 = data.advanced(by: offset).withUnsafeBytes { $0.pointee }
        var value = UInt32(littleEndian: bitValue)
        
        return withUnsafePointer(to: &value, { (ptr: UnsafePointer<UInt32>) -> Float in
            ptr.withMemoryRebound(to: Float.self, capacity: 1) { $0.pointee }
        })
    }
    
    open func decodeInt() -> Int32? {
        guard offset + 1 <= data.count,
            let x = Varint(fromData: data.subdata(in: offset ..< min(offset + 4, data.count))),
            x.count > 0 else { return nil }
        offset += x.count
        return Int32(fromZigZag: x.toUInt64())
    }
    
    open func decodeLong() -> Int64? {
        guard offset + 1 <= data.count,
            let x = Varint(fromData: data.subdata(in: offset ..< min(offset + 8, data.count))),
            x.count > 0 else { return nil }
        offset += x.count
        return Int64(fromZigZag: x.toUInt64())
    }
    
    open func decodeBytes() -> Data? {
        guard let size = decodeLong(), Int(size) + offset <= data.count, size != 0 else {
            return nil
        }
        defer { offset += Int(size) }
        return data.subdata(in: offset ..< offset + Int(size))
    }
    
    open func decodeString() -> String? {
        guard let rawString = decodeBytes() else {
            return nil
        }
        return String(bytes: rawString, encoding: .utf8)
        
    }
    
    open func decodeFixed(_ size: Int) -> Data? {
        guard size + offset <= data.count else {
            return nil
        }
        defer { offset += size }
        return data.subdata(in: offset ..< offset + size)
    }
}
