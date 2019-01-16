//
//  AvroEncoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

//
// AvroEncoder(Schema schema)
// encoder.encode(value) -> Data?
//
// AvroDecoder(Schema schema)
// decoder.decode(data) -> AvroValue


import Foundation

public protocol AvroEncoder {
    func encode(_ value: AvroValue) throws -> Data
    func encode(_ value: AvroValueConvertible, as schema: Schema) throws -> Data
}

public enum AvroCodingError: Error {
    case schemaMismatch
    case unionSizeMismatch
    case fixedSizeMismatch
    case enumMismatch
    case emptyValue
    case arraySizeMismatch
    case mapSizeMismatch
    case mapKeyTypeMismatch
}


open class GenericAvroEncoder: AvroEncoder {
    public enum Encoding {
        case binary
        case json
    }
    
    let encoding: Encoding
    
    public init(encoding: Encoding) {
        self.encoding = encoding
    }
    
    open func encode(_ value: AvroValueConvertible, as schema: Schema) throws -> Data {
        let encoder: AvroSerializer
        
        switch encoding {
        case .binary:
            encoder = BinaryAvroSerializer()
        case .json:
            encoder = JsonAvroSerializer()
        }
        
        let avroValue = try AvroValue(value: value, as: schema)
        try encode(avroValue, serializer: encoder)
        return encoder.data
    }
    
    open func encode(_ value: AvroValue) throws -> Data {
        return try encode(value, as: value.schema)
    }
    
    private func encode(_ value: AvroValue, serializer: AvroSerializer) throws {
        switch value {
        case .avroNull:
            serializer.encodeNull()
            
        case .avroBoolean(let value):
            serializer.encodeBoolean(value)
            
        case .avroInt(let value):
            serializer.encodeInt(value)
            
        case .avroLong(let value):
            serializer.encodeLong(value)
            
        case .avroFloat(let value):
            serializer.encodeFloat(value)
            
        case .avroDouble(let value):
            serializer.encodeDouble(value)
            
        case .avroString(let value):
            serializer.encodeString(value)
            
        case .avroBytes(let value):
            serializer.encodeBytes(value)
            
        case .avroArray(_, let values):
            serializer.encodeArrayStart(count: values.count)
            var first = true
            for value in values {
                if first {
                    first = false
                } else {
                    serializer.encodeSeparator()
                }
                try encode(value as! AvroValue, serializer: serializer)
            }
            serializer.encodeArrayEnd()
            
        case .avroMap(_, let pairs):
            serializer.encodeMapStart(count: pairs.count)
            var first = true
            for (key, value) in pairs {
                if first {
                    first = false
                } else {
                    serializer.encodeSeparator()
                }
                serializer.encodePropertyName(key)
                try encode(value as! AvroValue, serializer: serializer)
            }
            serializer.encodeMapEnd()
            
        case .avroRecord(.avroRecord(_, let fieldSchemas), let pairs):
            serializer.encodeRecordStart()
            var first = true
            for fSchema in fieldSchemas {
                if first {
                    first = false
                } else {
                    serializer.encodeSeparator()
                }
                guard case .avroField(let key, _, _) = fSchema else {
                    throw AvroCodingError.schemaMismatch
                }
                serializer.encodeFieldName(key)
                try encode(pairs[key] as! AvroValue, serializer: serializer)
            }
            serializer.encodeRecordEnd()
            
        case .avroRecord(_, _):
            throw AvroCodingError.schemaMismatch
            
        case .avroEnum(_, let index, let value):
            serializer.encodeEnum(index: index, symbol: value)
            
        case .avroUnion(_, _, AvroValue.avroNull):
            serializer.encodeNull()
            
        case .avroUnion(let subschemas, let index, let value):
            serializer.encodeUnionStart(index: index, typeName: subschemas[index].typeName)
            try encode(value as! AvroValue, serializer: serializer)
            serializer.encodeUnionEnd()
            
        case .avroFixed(_, let fixedBytes):
            serializer.encodeFixed(fixedBytes)
        }
    }
}

public protocol AvroSerializer {
    var data: Data { get }
    
    func encodeNull()
    
    func encodeBoolean(_ value: Bool)
    
    func encodeInt(_ value: Int32)
    
    func encodeLong(_ value: Int64)
    
    func encodeFloat(_ value: Float)
    
    func encodeDouble(_ value: Double)
    
    func encodeString(_ value: String)
    
    func encodeBytes(_ value: Data)
    
    func encodeFixed(_ value: Data)
    
    func encodeArrayStart(count: Int)
    
    func encodeFieldName(_ value: String)
    
    func encodeArrayEnd()
    
    func encodeUnionStart(index: Int, typeName: String)
    
    func encodeUnionEnd()
    
    func encodeRecordStart()
    
    func encodeMapStart(count: Int)
    
    func encodeRecordEnd()
    
    func encodeMapEnd()
    
    func encodeEnum(index: Int, symbol: String)
    
    func encodeSeparator()
    
    func encodePropertyName(_ name: String)
}
