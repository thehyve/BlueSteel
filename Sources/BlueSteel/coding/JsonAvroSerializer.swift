//
//  JsonAvroSerializer.swift
//  BlueSteel
//
//  Created by Joris Borgdorff on 16/01/2019.
//  Copyright © 2019 Gilt Groupe. All rights reserved.
//

import Foundation

open class JsonAvroSerializer: AvroSerializer {
    public func encodeUnionNull(index: Int) {
        encodeNull()
    }

    var string: String
    public var data: Data {
        return string.data(using: .utf8)!
    }

    public init() {
        string = ""
    }

    open func encodeNull() {
        string += "null"
    }

    open func encodeBoolean(_ value: Bool) {
        if value {
            string += "true"
        } else {
            string += "false"
        }
    }

    open func encodeInt(_ value: Int32) {
        string += String(value)
    }

    open func encodeLong(_ value: Int64) {
        string += String(value)
    }

    open func encodeFloat(_ value: Float) {
        string += String(value)
    }

    open func encodeDouble(_ value: Double) {
        string += String(value)
    }

    open func encodeFieldName(_ value: String) {
        encodePropertyName(value)
    }

    private static func lowNibbleAsHex(_ v: UInt32) ->String {
        let nibble = v & 0xf
        if nibble <= 0x9 {
            return String(Unicode.Scalar(nibble + 48)!)  // 48 = '0'
        } else {
            return String(Unicode.Scalar(nibble - 10 + 65)!)  // 65 = 'A'
        }
    }

    open func encodeString(_ value: String) {
        string += "\""

        for c in value.unicodeScalars {
            if c == "\"" {
                string += "\\\""
            } else if c == "\\" {
                string += "\\\\"
            } else if c.value < 32 {
                string += "\\u00"
                string += JsonAvroSerializer.lowNibbleAsHex(c.value >> 4)
                string += JsonAvroSerializer.lowNibbleAsHex(c.value)
            } else {
                string += String(c)
            }
        }

        string += "\""
    }

    open func encodeBytes(_ value: Data) {
        string += "\""

        for c in value {
            if c == 34 {
                string += "\\\""
            } else if c == 92 {
                string += "\\\\"
            } else if c < 32 {
                string += "\\u00"
                string += JsonAvroSerializer.lowNibbleAsHex(UInt32(c >> 4))
                string += JsonAvroSerializer.lowNibbleAsHex(UInt32(c))
            } else {
                string += String(Unicode.Scalar(c))
            }
        }

        string += "\""
    }

    open func encodeFixed(_ value: Data) {
        encodeBytes(value)
    }

    open func encodeArrayStart(count: Int) {
        string += "["
    }

    open func encodeArrayEnd() {
        string += "]"
    }

    open func encodeUnionStart(index: Int, typeName: String) {
        encodeRecordStart()
        encodePropertyName(typeName)
    }

    open func encodeUnionEnd() {
        encodeRecordEnd()
    }

    open func encodeRecordStart() {
        string += "{"
    }

    open func encodeMapStart(count: Int) {
        encodeRecordStart()
    }

    open func encodeMapEnd() {
        encodeRecordEnd()
    }

    open func encodeRecordEnd() {
        string += "}"
    }

    open func encodeSeparator() {
        string += ","
    }

    open func encodePropertyName(_ name: String) {
        encodeString(name)
        string += ":"
    }

    open func encodeEnum(index: Int, symbol: String) {
        encodeString(symbol)
    }
}
