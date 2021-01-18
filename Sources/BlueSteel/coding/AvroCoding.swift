//
//  AvroCoding.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 17/01/2019.
//  Copyright © 2019 RADAR-base. All rights reserved.
//

import Foundation

/// Encodes an Avro value
public protocol AvroEncoder {
    /// Encodes an Avro value using its embedded schema, assuming that it is valid.
    /// - Throws: `AvroConversionError` if the value cannot be converted to
    ///           its own schema.
    func encode(_ value: AvroValue) throws -> Data
    /// Encodes an Avro value using a given schema. The schema should be validated.
    /// - Throws: `AvroConversionError` if the value cannot be converted to
    ///           given schema.
    func encode(_ value: AvroValueConvertible, as schema: Schema) throws -> Data
}

/// Decodes an Avro value
public protocol AvroDecoder {
    /// Decode an Avro value using a given schema. The schema should be validated.
    /// - Throws:
    ///   - `AvroCodingError` if the value cannot be decoded
    ///   - `AvroConversionError` if the value cannot be converted to the given schema
    ///   - JSON error if JSON encoding is used and the data cannot be decoded
    ///     as a proper JSON object.
    ///
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

public enum SchemaCodingError: Error {
    case notAnObject
    case unknownEncoding
    case typeMismatch(AvroCodingContext)
    case unknownType(String, AvroCodingContext)
    case missingField(String, AvroCodingContext)
}

/// Keeps track of where in the Schema hierarchy
/// the coding process is. Is used in errors to identify
/// exactly where Avro values or Schemas are invalid.
public struct AvroCodingContext {
    var namespace: String? = nil
    public var nesting: [String] = []

    init(namespace: String? = nil, nesting: [String] = []) {
        self.namespace = namespace
        self.nesting = nesting
    }

    public init(nesting: [String] = []) {
        self.init(namespace: nil, nesting: nesting)
    }

    /// Nest the current context other types.
    /// - Parameters:
    ///   - type: types to be nested in.
    /// - Returns: new context with the given nesting
    func nestedIn(type: String...) -> AvroCodingContext {
        return AvroCodingContext(namespace: namespace, nesting: nesting + type)
    }

    /// Replace the last type on the stack by another type.
    /// This can be useful if the name of a type becomes
    /// known while parsing it.
    mutating func replaceLast(type: String) {
        let lastIndex = nesting.count - 1
        if lastIndex >= 0 {
            nesting[lastIndex] = type
        } else {
            nesting.append(type)
        }
    }

    mutating func updateNamespace(jsonObject: [String: Any]) {
        guard let jsonNamespace = jsonObject["namespace"] as? String else { return }

        namespace = jsonNamespace
    }

    /// Extract the namespace and single type from a full type name.
    /// - Returns:
    ///   - namespace: the namespace of the full type. This is empty if it has no namespace, or the namespace
    ///                is equal to the current namespace.
    ///   - name:      the name without a namespace.
    ///   - readableName: a name that identifies a type given the current context.
    func extractNamespace(fullName: String) -> (namespace: String?, name: String, readableName: String) {
        if let lastPeriod = fullName.lastIndex(of: ".") {
            let currentNamespace = String(fullName[..<lastPeriod])
            let currentName = String(fullName[fullName.index(after: lastPeriod)...])
            if currentNamespace == namespace {
                return (namespace: nil, name: currentName, readableName: currentName)
            } else {
                return (namespace: currentNamespace, name: currentName, readableName: fullName)
            }
        } else {
            return (namespace: nil, name: fullName, readableName: fullName)
        }
    }

    func fullName(for name: String) -> String {
        if name.range(of: ".") == nil, let namespace = namespace {
            return namespace + "." + name
        } else {
            return name
        }
    }
}
