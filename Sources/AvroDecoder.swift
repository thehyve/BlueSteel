//
//  AvroDecoder.swift
//  BlueSteel
//
//  Created by Matt Isaacs.
//  Copyright (c) 2014 Gilt. All rights reserved.
//

import Foundation

public protocol AvroDecoder {
    func decode(_ data: Data, as schema: Schema) throws -> AvroValue
}
