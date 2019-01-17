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
