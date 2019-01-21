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
        var excludingTypeNames = Set<String>()
        return description(excludingTypeNames: &excludingTypeNames)
    }

    private func description(excludingTypeNames: inout Set<String>) -> String {
        switch self {
        case .avroMap(let valueSchema):
            return "map<\(valueSchema.description(excludingTypeNames: &excludingTypeNames))>"
        case .avroArray(let itemSchema):
            return "array<\(itemSchema.description(excludingTypeNames: &excludingTypeNames))>"
        case .avroFixed(let name, let size):
            if excludingTypeNames.insert(name).inserted  {
                return "\(name)<fixed>(\(size))>"
            } else {
                return name
            }
        case .avroEnum(let name, let symbols):
            if excludingTypeNames.insert(name).inserted  {
                return "\(name)<enum>([\(symbols.joined(separator: ", "))])"
            } else {
                return name
            }
        case .avroRecord(let name, let fields):
            if excludingTypeNames.insert(name).inserted  {
                let fieldsString = fields.map { field in
                    if let defaultValue = field.defaultValue {
                        return "\(field.name): \(field.schema.description(excludingTypeNames: &excludingTypeNames)) (default: \(defaultValue))" as String
                    } else {
                        return "\(field.name): \(field.schema.description(excludingTypeNames: &excludingTypeNames))" as String
                    }
                    }.joined(separator: ", ")
                return "\(name)<record>([\(fieldsString)])"
            } else {
                return name
            }

        case .avroUnion(let subSchemas):
            let subString = subSchemas.map { $0.description(excludingTypeNames: &excludingTypeNames) }.joined(separator: ", ")
            return "[\(subString)]"

        default:
            return typeName
        }
    }
}
