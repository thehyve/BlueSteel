//
//  AvroValue+Equatable.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 17/01/2019.
//  Copyright © 2019 RADAR-base. All rights reserved.
//

import Foundation

extension AvroValue: Equatable {
    public static func ==(lhs: AvroValue, rhs: AvroValue) -> Bool {
        switch (lhs, rhs) {
        case (.avroNull, .avroNull):
            return true
        case (.avroBoolean(let lvalue), .avroBoolean(let rvalue)):
            return lvalue == rvalue
        case (.avroInt(let lvalue), .avroInt(let rvalue)):
            return lvalue == rvalue
        case (.avroLong(let lvalue), .avroLong(let rvalue)):
            return lvalue == rvalue
        case (.avroFloat(let lvalue), .avroFloat(let rvalue)):
            return lvalue == rvalue
        case (.avroDouble(let lvalue), .avroDouble(let rvalue)):
            return lvalue == rvalue
        case (.avroString(let lvalue), .avroString(let rvalue)):
            return lvalue == rvalue
        case (.avroBytes(let lvalue), .avroBytes(let rvalue)):
            return lvalue == rvalue
        case (.avroFixed(let lschema, let lvalue), .avroFixed(let rschema, let rvalue)):
            return lschema == rschema && lvalue == rvalue
        case (.avroMap(let lschema, let lvalues), .avroMap(let rschema, let rvalues)):
            guard lschema == rschema, lvalues.keys == rvalues.keys else { return false }
            for (key, lvalue) in lvalues {
                guard let rvalue = rvalues[key],
                    let lAvroValue = try? AvroValue(value: lvalue, as: lschema),
                    let rAvroValue = try? AvroValue(value: rvalue, as: rschema),
                    lAvroValue == rAvroValue else {
                        return false
                }
            }
            return true
            
        case (.avroRecord(.avroRecord(let lname, let lfieldSchemas), let lvalues), .avroRecord(.avroRecord(let rname, let rfieldSchemas), let rvalues)):
            guard lname == rname, lfieldSchemas == rfieldSchemas else { return false }
            for fieldSchema in lfieldSchemas {
                guard case .avroField(let fieldName, let innerSchema, _) = fieldSchema,
                    let lvalue = lvalues[fieldName],
                    let rvalue = rvalues[fieldName],
                    let lAvroValue = try? AvroValue(value: lvalue, as: innerSchema),
                    let rAvroValue = try? AvroValue(value: rvalue, as: innerSchema),
                    lAvroValue == rAvroValue else {
                        return false
                }
            }
            return true
            
        case (.avroArray(let lschema, let lvalues), .avroArray(let rschema, let rvalues)):
            guard lschema == rschema, lvalues.count == rvalues.count else { return false }
            for i in 0 ..< lvalues.count {
                guard let lAvroValue = try? AvroValue(value: lvalues[i], as: lschema),
                    let rAvroValue = try? AvroValue(value: rvalues[i], as: rschema),
                    lAvroValue == rAvroValue else {
                        return false
                }
            }
            return true
            
        case (.avroEnum(let lschema, let lindex, let lvalue), .avroEnum(let rschema, let rindex, let rvalue)):
            return lschema == rschema && lindex == rindex && lvalue == rvalue
            
        case (.avroUnion(let lschema, let lindex, let lvalue), .avroUnion(let rschema, let rindex, let rvalue)):
            guard lschema == rschema,
                lindex == rindex,
                let lAvroValue = try? AvroValue(value: lvalue, as: lschema[lindex]),
                let rAvroValue = try? AvroValue(value: rvalue, as: rschema[rindex]) else {
                    return false
            }
            return lAvroValue == rAvroValue
        default:
            return false
        }
    }
}
