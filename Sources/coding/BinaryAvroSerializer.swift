//
//  BinaryAvroEncoder.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 10/01/2019.
//  Copyright © 2019 Gilt Groupe. All rights reserved.
//

import Foundation

open class BinaryAvroSerializer: AvroSerializer {
    public func encodeUnionNull(index: Int) {
        encodeLong(Int64(index))
    }

    public var data: Data

    public init() {
        data = Data()
    }

    open func encodeNull() {
    }

    open func encodeBoolean(_ value: Bool) {
        if value {
            data.append(UInt8(0x1))
        } else {
            data.append(UInt8(0x0))
        }
    }

    open func encodeInt(_ value: Int32) {
        encodeLong(Int64(value))
    }

    open func encodeLong(_ value: Int64) {
        let varInt = ZigZagInt(value: value)
        data.append(contentsOf: varInt.data)
    }

    open func encodeFloat(_ value: Float) {
        let bitValue = value.bitPattern.littleEndian
        data.append(withUnsafeBytes(of: bitValue) { Data($0) })
    }

    open func encodeDouble(_ value: Double) {
        let bitValue = value.bitPattern.littleEndian
        data.append(withUnsafeBytes(of: bitValue) { Data($0) })
    }

    open func encodeString(_ value: String) {
        encodeBytes(value.data(using: .utf8)!)
    }

    open func encodeBytes(_ value: Data) {
        encodeLong(Int64(value.count))
        data.append(value)
    }

    open func encodeFixed(_ value: Data) {
        data.append(value)
    }

    open func encodeArrayStart(count: Int) {
        if count > 0 {
            encodeLong(Int64(count))
        }
    }

    open func encodeArrayEnd() {
        encodeLong(0)
    }

    open func encodeUnionStart(index: Int, typeName: String) {
        encodeLong(Int64(index))
    }

    open func encodeUnionEnd() {
    }

    open func encodeRecordStart() {
    }

    open func encodeMapStart(count: Int) {
        if count > 0 {
            encodeLong(Int64(count))
        }
    }

    open func encodeRecordEnd() {
    }

    open func encodeMapEnd() {
        encodeLong(0)
    }

    open func encodeSeparator() {
    }

    open func encodePropertyName(_ name: String) {
        encodeString(name)
    }

    open func encodeFieldName(_ value: String) {
    }

    open func encodeEnum(index: Int, symbol: String) {
        encodeInt(Int32(index))
    }
}
