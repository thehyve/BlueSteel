//
//  Schema+Equatable.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 17/01/2019.
//  Copyright © 2019 RADAR-base. All rights reserved.
//

import Foundation

extension Schema: Equatable {
    public static func ==(lhs: Schema, rhs: Schema) -> Bool {
        switch (lhs, rhs) {
        case (.avroBoolean, .avroBoolean),
             (.avroInt, .avroInt),
             (.avroLong, .avroLong),
             (.avroFloat, .avroFloat),
             (.avroDouble, .avroDouble),
             (.avroBytes, .avroBytes),
             (.avroString, .avroString),
             (.avroNull, .avroNull),
             (.avroUnknown, .avroUnknown):
            return true
        case (.avroArray(let lschema), .avroArray(let rschema)),
             (.avroMap(let lschema), .avroMap(let rschema)):
            return lschema == rschema
        case (.avroRecord(let lname, let lschema), .avroRecord(let rname, let rschema)):
            return lname == rname && lschema == rschema
        case (.avroField(let lname, let lschema, let ldefault), .avroField(let rname, let rschema, let rdefault)):
            return lname == rname && lschema == rschema && ldefault == rdefault
        case (.avroUnion(let lschemas), .avroUnion(let rschemas)):
            return lschemas == rschemas
        case (.avroEnum(let lname, let lsymbols), .avroEnum(let rname, let rsymbols)):
            return lname == rname && lsymbols == rsymbols
        case (.avroFixed(let lname, let lsize), .avroFixed(let rname, let rsize)):
            return lname == rname && lsize == rsize
        default:
            return false
        }
    }
}

extension Schema: Hashable {
    private func hash(into hasher: inout Hasher, excludingTypes: inout Set<String>) {
        switch self {
        case .avroNull:
            hasher.combine(1)
        case .avroBoolean:
            hasher.combine(2)
        case .avroInt:
            hasher.combine(3)
        case .avroLong:
            hasher.combine(4)
        case .avroFloat:
            hasher.combine(5)
        case .avroBytes:
            hasher.combine(6)
        case .avroString:
            hasher.combine(7)
        case .avroArray(let schema):
            hasher.combine(8)
            schema.hash(into: &hasher, excludingTypes: &excludingTypes)
        case .avroRecord(let name, let fields):
            hasher.combine(9)
            hasher.combine(name)
            if excludingTypes.insert(name).inserted {
                for field in fields {
                    field.hash(into: &hasher, excludingTypes: &excludingTypes)
                }
            }
        case .avroField(let name, let schema, let fieldDefault):
            hasher.combine(10)
            hasher.combine(name)
            schema.hash(into: &hasher, excludingTypes: &excludingTypes)
            if let fieldDefault = fieldDefault {
                switch fieldDefault {
                case .avroNull:
                    hasher.combine(1)
                case .avroLong(let value):
                    hasher.combine(2)
                    hasher.combine(value)
                case .avroDouble(let value):
                    hasher.combine(3)
                    hasher.combine(value)
                case .avroString(let value):
                    hasher.combine(4)
                    hasher.combine(value)
                default:
                    hasher.combine(5)
                }
            } else {
                hasher.combine(0)
            }
        case .avroEnum(let name, let symbols):
            hasher.combine(11)
            hasher.combine(name)
            if excludingTypes.insert(name).inserted {
                hasher.combine(symbols)
            }
        case .avroFixed(let name, let size):
            hasher.combine(12)
            hasher.combine(name)
            if excludingTypes.insert(name).inserted {
                hasher.combine(size)
            }
        case .avroUnion(let subSchemas):
            hasher.combine(13)
            for subSchema in subSchemas {
                subSchema.hash(into: &hasher, excludingTypes: &excludingTypes)
            }
        case .avroDouble:
            hasher.combine(14)
        case .avroMap(let schema):
            hasher.combine(15)
            schema.hash(into: &hasher, excludingTypes: &excludingTypes)
        case .avroUnknown:
            hasher.combine(16)
        }
    }
    
    public func hash(into hasher: inout Hasher) {
        var excludingTypes: Set<String> = []
        hash(into: &hasher, excludingTypes: &excludingTypes)
    }
}
