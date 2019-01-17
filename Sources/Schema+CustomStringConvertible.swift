//
//  Schema+CustomStringConvertible.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 17/01/2019.
//  Copyright © 2019 RADAR-base. All rights reserved.
//

import Foundation

extension Schema: CustomStringConvertible {
    public var description: String {
        switch self {
        case .avroMap(let valueSchema):
            return "map<\(valueSchema)>"
        case .avroArray(let itemSchema):
            return "array<\(itemSchema)>"
        case .avroFixed(let name, let size):
            return "\(name)<fixed>(\(size))>"
        case .avroEnum(let name, let symbols):
            return "\(name)<enum>(\(symbols))"
        case .avroRecord(let name, let fields):
            let fieldsString = fields.map { field in
                if let defaultValue = field.defaultValue {
                    return "\(field.name): \(field.schema) (default: \(defaultValue))" as String
                } else {
                    return "\(field.name): \(field.schema)" as String
                }
            }.joined(separator: ", ")
            return "\(name)<record>([\(fieldsString)])"

        case .avroUnion(let subSchemas):
            return "\(subSchemas)"
        default:
            return typeName
        }
    }
}
