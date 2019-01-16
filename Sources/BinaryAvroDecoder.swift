//
//  BinaryAvroDecoder.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 16/01/2019.
//  Copyright © 2019 Gilt Groupe. All rights reserved.
//

import Foundation

open class BinaryAvroDecoder : AvroDecoder {
    public init() {
    }
    
    open func decode(_ data: Data, as schema: Schema) throws -> AvroValue {
        let context = BinaryAvroDecoderContext(data)
        
        return try decode(context: context, as: schema)
    }
    open func decode(context: BinaryAvroDecoderContext, as schema: Schema) throws -> AvroValue {
        switch schema {
        case .avroNull :
            return .avroNull
            
        case .avroBoolean :
            guard let decoded = context.decodeBoolean() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroBoolean(decoded)
            
        case .avroInt :
            guard let decoded = context.decodeInt() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroInt(decoded)
            
        case .avroLong :
            guard let decoded = context.decodeLong() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroLong(decoded)
            
        case .avroFloat :
            guard let decoded = context.decodeFloat() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroFloat(decoded)
            
        case .avroDouble :
            guard let decoded = context.decodeDouble() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroDouble(decoded)
            
        case .avroString:
            guard let decoded = context.decodeString() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroString(decoded)
            
        case .avroBytes:
            guard let decoded = context.decodeBytes() else {
                throw AvroCodingError.schemaMismatch
            }
            return .avroBytes(decoded)
            
        case .avroArray(let itemsSchema):
            var values: [AvroValue] = []
            while let count = try blockCount(context: context) {
                if count == 0 {
                    return .avroArray(itemSchema: itemsSchema, values)
                }
                values.reserveCapacity(values.count + count)
                for _ in 0 ..< count {
                    values.append(try decode(context: context, as: itemsSchema))
                }
            }
            throw AvroCodingError.arraySizeMismatch
            
        case .avroMap(let valuesSchema) :
            var pairs: [String: AvroValue] = [:]
            while let count = try blockCount(context: context) {
                if count == 0 {
                    return .avroMap(valueSchema: valuesSchema, pairs)
                }
                pairs.reserveCapacity(pairs.count + count)
                for _ in 0 ..< count {
                    guard let key = context.decodeString() else {
                        throw AvroCodingError.mapKeyTypeMismatch
                    }
                    pairs[key] = try decode(context: context, as: valuesSchema)
                }
            }
            throw AvroCodingError.mapSizeMismatch
            
        case .avroEnum(_, let enumValues) :
            guard let index = context.decodeInt(), Int(index) < enumValues.count else {
                throw AvroCodingError.enumMismatch
            }
            return .avroEnum(schema, index: Int(index), enumValues[Int(index)])
            
        case .avroRecord(_, let fields) :
            var pairs: [String: AvroValue] = [:]
            for field in fields {
                guard case .avroField(let key, let fieldSchema, _) = field else {
                    throw AvroCodingError.schemaMismatch
                }
                pairs[key] = try decode(context: context, as: fieldSchema)
            }
            return .avroRecord(schema, pairs)
            
        case .avroFixed(_, let size) :
            guard let bytes = context.decodeFixed(size) else {
                throw AvroCodingError.fixedSizeMismatch
            }
            return .avroFixed(schema, bytes)
            
        case .avroUnion(let schemas) :
            guard let index = context.decodeLong(), Int(index) < schemas.count else {
                throw AvroCodingError.unionSizeMismatch
            }
            let unionValue = try decode(context: context, as: schemas[Int(index)])
            return .avroUnion(schemaOptions: schemas, index: Int(index), unionValue)
            
        default :
            throw AvroCodingError.schemaMismatch
        }
    }
    
    private func blockCount(context: BinaryAvroDecoderContext) throws -> Int? {
        guard let count = context.decodeLong() else { return nil }
        if count < 0 {
            guard context.decodeLong() != nil else { throw AvroCodingError.schemaMismatch }
            return -Int(count)
        } else {
            return Int(count)
        }
    }
}
