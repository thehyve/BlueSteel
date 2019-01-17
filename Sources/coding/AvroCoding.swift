//
//  AvroCoding.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 17/01/2019.
//  Copyright © 2019 RADAR-base. All rights reserved.
//

import Foundation

public protocol AvroEncoder {
    func encode(_ value: AvroValue) throws -> Data
    func encode(_ value: AvroValueConvertible, as schema: Schema) throws -> Data
}

public protocol AvroDecoder {
    func decode(_ data: Data, as schema: Schema) throws -> AvroValue
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

public enum AvroEncoding {
    case binary
    case json
}

enum SchemaCodingError: Error {
    case typeMismatch
    case notAnObject
    case unknownEncoding
    case unknownType(String)
    case missingField(String)
}
