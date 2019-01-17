//
//  Varint.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

// MARK: Varint

struct ZigZagInt {
    let data: Data

    var count: Int {
        return data.count
    }

    var value: Int64 {
        var bitPattern: UInt64 = 0

        for idx in 0 ..< data.count {
            bitPattern |= UInt64(data[idx] & 0x7F) << UInt64(7 * idx)
        }

        let isPositive = bitPattern & 0x1 == 0
        let unsignedBitPattern = bitPattern >> 1
        return isPositive ? Int64(bitPattern: unsignedBitPattern) : Int64(bitPattern: ~unsignedBitPattern)
    }

    init?(from data: Data) {
        var buf = Data(capacity: min(data.count, 8))

        for x in data {
            buf.append(x)

            if x & 0x80 == 0 {
                break
            }
        }
        guard !buf.isEmpty else { return nil }
        self.data = buf
    }

    init(value: Int64) {
        let unsignedBitPattern = value << 1
        let zigZag = value >= 0 ? UInt64(bitPattern: unsignedBitPattern) : UInt64(bitPattern: ~unsignedBitPattern)

        var buffer = Data(capacity: 8)
        if zigZag == 0 {
            buffer.append(0)
        } else {
            var shiftedValue = zigZag
            var idx = 0
            while shiftedValue > 0 {
                if idx > 0 {
                    buffer[idx - 1] |= 0x80
                }
                buffer.append(UInt8(shiftedValue & 0xFF))

                // Next index
                idx += 1
                shiftedValue >>= 7
            }
        }

        self.data = buffer
    }
}
