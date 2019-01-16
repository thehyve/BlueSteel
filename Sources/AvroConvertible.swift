//
//  AvroConvertible.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public protocol AvroValueConvertible {
    func toAvro() -> AvroValue
}

extension String: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroString(self)
    }
}

extension Bool: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroBoolean(self)
    }
}

extension Int: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroLong(Int64(self))
    }
}

extension Int32: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroInt(self)
    }
}

extension Int64: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroLong(self)
    }
}

extension Float: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroFloat(self)
    }
}

extension Double: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroDouble(self)
    }
}

extension Data: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroBytes(self)
    }
}

extension Dictionary: AvroValueConvertible where Key == String, Value == AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroMap(valueSchema: .avroUnknown, self)
    }
}

extension Array: AvroValueConvertible where Element == AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroArray(itemSchema: .avroUnknown, self)
    }
}

extension NSNull: AvroValueConvertible {
    public func toAvro() -> AvroValue {
        return AvroValue.avroNull
    }
}
