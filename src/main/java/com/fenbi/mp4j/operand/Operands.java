/**
*
* Copyright (c) 2017 ytk-mp4j https://github.com/yuantiku
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:

* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.

* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
*/

package com.fenbi.mp4j.operand;


import com.esotericsoftware.kryo.Serializer;

/**
 * @author xialong
 */
public final class Operands {
    public final static DoubleOperand DOUBLE_OPERAND() {
        return DOUBLE_OPERAND(false);
    }
    
    public final static FloatOperand FLOAT_OPERAND() {
        return FLOAT_OPERAND(false);
    }

    public final static LongOperand LONG_OPERAND() {
        return LONG_OPERAND(false);
    }

    public final static IntOperand INT_OPERAND() {
        return INT_OPERAND(false);
    }

    public final static ShortOperand SHORT_OPERAND() {
        return SHORT_OPERAND(false);
    }

    public final static ByteOperand BYTE_OPERAND() {
        return BYTE_OPERAND(false);
    }

    public final static StringOperand STRING_OPERAND() {
        return STRING_OPERAND(false);
    }

    public final static <T> ObjectOperand OBJECT_OPERAND(Serializer<T> serializer, Class type) {
        return OBJECT_OPERAND(serializer, type, false);
    }

    public final static DoubleOperand DOUBLE_OPERAND(boolean compress) {
        DoubleOperand operand = new DoubleOperand();
        operand.setCompress(compress);
        return operand;
    }

    public final static FloatOperand FLOAT_OPERAND(boolean compress) {
        FloatOperand operand = new FloatOperand();
        operand.setCompress(compress);
        return operand;
    }

    public final static LongOperand LONG_OPERAND(boolean compress) {
        LongOperand operand = new LongOperand();
        operand.setCompress(compress);
        return operand;
    }

    public final static IntOperand INT_OPERAND(boolean compress) {
        IntOperand operand = new IntOperand();
        operand.setCompress(compress);
        return operand;
    }

    public final static ShortOperand SHORT_OPERAND(boolean compress) {
        ShortOperand operand = new ShortOperand();
        operand.setCompress(compress);
        return operand;
    }

    public final static ByteOperand BYTE_OPERAND(boolean compress) {
        ByteOperand operand = new ByteOperand();
        operand.setCompress(compress);
        return operand;
    }

    public final static StringOperand STRING_OPERAND(boolean compress) {
        StringOperand operand = new StringOperand();
        operand.setCompress(compress);
        return operand;
    }

    public final static <T> ObjectOperand OBJECT_OPERAND(Serializer<T> serializer, Class type, boolean compress) {
        ObjectOperand<T> operand = new ObjectOperand<T>(serializer, type);
        operand.setCompress(compress);
        return operand;
    }
}
