//
//  BinaryAvroDecoder.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 16/01/2019.
//  Copyright © 2019 Gilt Groupe. All rights reserved.
//

import Foundation

/// Decoder for the binary Avro encoding.
open class BinaryAvroDecoder : AvroDecoder {
    public init() {
    }

    open func decode(_ data: Data, as schema: Schema) throws -> AvroValue {
        let deserializer = BinaryAvroDeserializer(data)

        return try decode(with: deserializer, as: schema)
    }

    private func decode(with deserializer: BinaryAvroDeserializer, as schema: Schema) throws -> AvroValue {
        switch schema {
        case .avroNull :
            return .avroNull

        case .avroBoolean :
            guard let decoded = deserializer.decodeBoolean() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroBoolean(decoded)

        case .avroInt :
            guard let decoded = deserializer.decodeInt() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroInt(decoded)

        case .avroLong :
            guard let decoded = deserializer.decodeLong() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroLong(decoded)

        case .avroFloat :
            guard let decoded = deserializer.decodeFloat() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroFloat(decoded)

        case .avroDouble :
            guard let decoded = deserializer.decodeDouble() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroDouble(decoded)

        case .avroString:
            guard let decoded = deserializer.decodeString() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroString(decoded)

        case .avroBytes:
            guard let decoded = deserializer.decodeBytes() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroBytes(decoded)

        case .avroArray(let itemsSchema):
            var values: [AvroValue] = []
            while let count = try decoderBlockCount(with: deserializer) {
                if count == 0 {
                    return .avroArray(itemSchema: itemsSchema, values)
                }
                values.reserveCapacity(values.count + count)
                for _ in 0 ..< count {
                    values.append(try decode(with: deserializer, as: itemsSchema))
                }
            }
            throw AvroCodingError.arraySizeMismatch

        case .avroMap(let valuesSchema) :
            var pairs: [String: AvroValue] = [:]
            while let count = try decoderBlockCount(with: deserializer) {
                if count == 0 {
                    return .avroMap(valueSchema: valuesSchema, pairs)
                }
                pairs.reserveCapacity(pairs.count + count)
                for _ in 0 ..< count {
                    guard let key = deserializer.decodeString() else {
                        throw AvroCodingError.mapKeyTypeMismatch
                    }
                    pairs[key] = try decode(with: deserializer, as: valuesSchema)
                }
            }
            throw AvroCodingError.mapSizeMismatch

        case .avroEnum(_, let enumValues) :
            guard let index = deserializer.decodeInt(), Int(index) < enumValues.count else {
                throw AvroCodingError.enumMismatch
            }
            return .avroEnum(schema, index: Int(index), enumValues[Int(index)])

        case .avroRecord(_, let fields) :
            var pairs: [String: AvroValue] = [:]
            for field in fields {
                pairs[field.name] = try decode(with: deserializer, as: field.schema)
            }
            return .avroRecord(schema, pairs)

        case .avroFixed(_, let size) :
            guard let bytes = deserializer.decodeFixed(size) else {
                throw AvroCodingError.fixedSizeMismatch
            }
            return .avroFixed(schema, bytes)

        case .avroUnion(let schemas) :
            guard let index = deserializer.decodeLong(), Int(index) < schemas.count else {
                throw AvroCodingError.unionSizeMismatch
            }
            let unionValue = try decode(with: deserializer, as: schemas[Int(index)])
            return .avroUnion(schemaOptions: schemas, index: Int(index), unionValue)

        default :
            throw AvroCodingError.schemaMismatch
        }
    }

    private func decoderBlockCount(with deserializer: BinaryAvroDeserializer) throws -> Int? {
        guard let count = deserializer.decodeLong() else { return nil }
        if count < 0 {
            guard deserializer.decodeLong() != nil else { throw AvroCodingError.schemaMismatch }
            return -Int(count)
        } else {
            return Int(count)
        }
    }
}

fileprivate class BinaryAvroDeserializer {
    let data: Data
    var offset: Int

    init(_ data: Data) {
        self.data = data
        self.offset = 0
    }

    func decodeNull() {
        // Nulls aren't actually encoded.
        return
    }

    func decodeBoolean() -> Bool? {
        guard offset + 1 <= data.count  else { return nil }

        defer { offset += 1 }
        return data[offset] > 0
    }

    func decodeDouble() -> Double? {
        guard offset + 8 <= data.count else { return nil }

        defer { offset += 8 }
        let bitValue: UInt64 = data.advanced(by: offset).withUnsafeBytes { $0.load(as: UInt64.self) }
        var value = UInt64(littleEndian: bitValue)

        return withUnsafePointer(to: &value) { ptr in
            ptr.withMemoryRebound(to: Double.self, capacity: 1) { $0.pointee }
        }
    }

    func decodeFloat() -> Float? {
        guard offset + 4 <= data.count else { return nil }

        defer { offset += 4 }
        let bitValue: UInt32 = data.advanced(by: offset).withUnsafeBytes { $0.load(as: UInt32.self) }
        var value = UInt32(littleEndian: bitValue)

        return withUnsafePointer(to: &value, { (ptr: UnsafePointer<UInt32>) -> Float in
            ptr.withMemoryRebound(to: Float.self, capacity: 1) { $0.pointee }
        })
    }

    func decodeInt() -> Int32? {
        guard offset + 1 <= data.count,
            let x = ZigZagInt(from: data.subdata(in: offset ..< min(offset + 4, data.count))),
            x.count > 0 else { return nil }
        offset += x.count
        return Int32(x.value)
    }

    func decodeLong() -> Int64? {
        guard offset + 1 <= data.count,
            let x = ZigZagInt(from: data.subdata(in: offset ..< min(offset + 8, data.count))),
            x.count > 0 else { return nil }
        offset += x.count
        return x.value
    }

    func decodeBytes() -> Data? {
        guard let size = decodeLong(), Int(size) + offset <= data.count, size != 0 else {
            return nil
        }
        defer { offset += Int(size) }
        return data.subdata(in: offset ..< offset + Int(size))
    }

    func decodeString() -> String? {
        guard let rawString = decodeBytes() else {
            return nil
        }
        return String(bytes: rawString, encoding: .utf8)

    }

    func decodeFixed(_ size: Int) -> Data? {
        guard size + offset <= data.count else {
            return nil
        }
        defer { offset += size }
        return data.subdata(in: offset ..< offset + size)
    }
}
