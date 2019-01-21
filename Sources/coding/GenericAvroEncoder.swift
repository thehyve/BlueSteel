//
//  GenericAvroEncoder.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 17/01/2019.
//  Copyright © 2019 RADAR-base. All rights reserved.
//

import Foundation

/// Generic Avro Encoder for both binary and JSON encodings.
open class GenericAvroEncoder: AvroEncoder {
    let encoding: AvroEncoding

    public init(encoding: AvroEncoding) {
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
        encode(avroValue, serializer: encoder)
        return encoder.data
    }

    open func encode(_ value: AvroValue) throws -> Data {
        return try encode(value, as: value.schema)
    }

    private func encode(_ value: AvroValue, serializer: AvroSerializer) {
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
                encode(value as! AvroValue, serializer: serializer)
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
                encode(value as! AvroValue, serializer: serializer)
            }
            serializer.encodeMapEnd()

        case .avroRecord(let schema, let pairs):
            guard case .avroRecord(_, let fieldSchemas) = schema else {
                assertionFailure("Avro record should have a record schema")
                return
            }
            serializer.encodeRecordStart()
            var first = true
            for field in fieldSchemas {
                if first {
                    first = false
                } else {
                    serializer.encodeSeparator()
                }
                serializer.encodeFieldName(field.name)
                encode(pairs[field.name] as! AvroValue, serializer: serializer)
            }
            serializer.encodeRecordEnd()

        case .avroEnum(_, let index, let value):
            serializer.encodeEnum(index: index, symbol: value)

        case .avroUnion(_, let index, AvroValue.avroNull):
            serializer.encodeUnionNull(index: index)

        case .avroUnion(let subschemas, let index, let value):
            serializer.encodeUnionStart(index: index, typeName: subschemas[index].typeName)
            encode(value as! AvroValue, serializer: serializer)
            serializer.encodeUnionEnd()

        case .avroFixed(_, let fixedBytes):
            serializer.encodeFixed(fixedBytes)
        }
    }
}

/// Serializer for a given encoding.
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

    func encodeUnionNull(index: Int)

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
