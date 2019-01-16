//
//  Varint.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

// MARK: Varint

public struct Varint: CustomStringConvertible {
    let backing: Data

    public var count: Int {
        return backing.count
    }

    public var description: String {
        return backing.description
    }

    init?(fromData data: Data) {
        var buf = Data()
        
        for x in data {
            buf.append(x)
            
            if x & 0x80 == 0 {
                break
            }
        }
        guard !buf.isEmpty else { return nil }
        self.backing = buf
    }

    public init(fromValue value: UInt) {
        self.init(fromValue: UInt64(value))
    }

    public init(fromValue value: UInt64) {
        var buffer = Data()
        if value == 0 {
            buffer.append(0)
        } else {
            var shiftedValue = value
            while shiftedValue > 0 {
                if !buffer.isEmpty {
                    buffer[buffer.count - 1] |= 0x80
                }
                buffer.append(UInt8(shiftedValue & 0xFF))

                // Next index
                shiftedValue >>= 7
            }
        }

        self.backing = buffer
    }

    public func toUInt64() -> UInt64 {
        var result: UInt64 = 0

        for idx in 0 ..< backing.count {
            result |= UInt64(backing[idx] & 0x7F) << UInt64(7 * idx)
        }

        return result
    }
}

// MARK: Zig Zag encoding

extension Int32 {
    public init(fromZigZag: UInt64) {
        self = Int32(Int64(bitPattern: ((fromZigZag & 0x1) * UInt64.max) ^ (fromZigZag >> 1)))
    }
}

extension Int64 {
    public init(fromZigZag: UInt64) {
        self = Int64(bitPattern: ((fromZigZag & 0x1) * UInt64.max) ^ (fromZigZag >> 1))
    }
}

extension UInt64 {
    public init(toZigZag: Int64) {
        self = UInt64(bitPattern: ((toZigZag << 1) ^ (toZigZag >> 63)) as Int64)
    }
}
