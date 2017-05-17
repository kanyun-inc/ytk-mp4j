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

package com.fenbi.mp4j.operator;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author xialong
 */
public class OperatorsTest {
    @Test
    public void doubleTest() {
        Assert.assertTrue(Operators.Double.SUM.apply(1.0, 2.0) == 3.0);
        Assert.assertTrue(Operators.Double.MAX.apply(1.0, 2.0) == 2.0);
        Assert.assertTrue(Operators.Double.MIN.apply(1.0, 2.0) == 1.0);
        Assert.assertTrue(Operators.Double.PROD.apply(1.0, 2.0) == 2.0);
        Assert.assertTrue(Operators.Double.FLOAT_MAX_LOC.apply(
                Operators.Double.compositeDouble(1.9f, 1),
                Operators.Double.compositeDouble(1.8f, 2)) ==
                Operators.Double.compositeDouble(1.9f, 1));
        Assert.assertTrue(Operators.Double.FLOAT_MIN_LOC.apply(
                Operators.Double.compositeDouble(1.9f, 1),
                Operators.Double.compositeDouble(1.8f, 2)) ==
                Operators.Double.compositeDouble(1.8f, 2));

    }

    @Test
    public void floatTest() {
        Assert.assertTrue(Operators.Float.SUM.apply(1.0f, 2.0f) == 3.0f);
        Assert.assertTrue(Operators.Float.MAX.apply(1.0f, 2.0f) == 2.0f);
        Assert.assertTrue(Operators.Float.MIN.apply(1.0f, 2.0f) == 1.0f);
        Assert.assertTrue(Operators.Float.PROD.apply(1.0f, 2.0f) == 2.0f);
    }

    @Test
    public void longTest() {
        Assert.assertTrue(Operators.Long.SUM.apply(1l, 2l) == 3l);
        Assert.assertTrue(Operators.Long.MAX.apply(1l, 2l) == 2l);
        Assert.assertTrue(Operators.Long.MIN.apply(1l, 2l) == 1l);
        Assert.assertTrue(Operators.Long.PROD.apply(1l, 2l) == 2l);
        Assert.assertTrue(Operators.Long.INT_MAX_LOC.apply(
                Operators.Long.compositeLong(19, 1),
                Operators.Long.compositeLong(18, 2)) ==
                Operators.Long.compositeLong(19, 1));
        Assert.assertTrue(Operators.Long.INT_MIN_LOC.apply(
                Operators.Long.compositeLong(19, 1),
                Operators.Long.compositeLong(18, 2)) ==
                Operators.Long.compositeLong(18, 2));
        Assert.assertTrue(Operators.Long.BITS_AND.apply(-1L, 1234L) == 1234L);
        Assert.assertTrue(Operators.Long.BITS_AND.apply(-1L, -14343434L) == -14343434L);
        Assert.assertTrue(Operators.Long.BITS_OR.apply(-1L, -14343434L) == 0xffffffffffffffffL);
        Assert.assertTrue(Operators.Long.BITS_OR.apply(1234L, 3456L) == 3538L);
        Assert.assertTrue(Operators.Long.BITS_XOR.apply(1234L, 3456L) == 2386L);
    }

    @Test
    public void intTest() {
        Assert.assertTrue(Operators.Int.SUM.apply(1, 2) == 3);
        Assert.assertTrue(Operators.Int.MAX.apply(1, 2) == 2);
        Assert.assertTrue(Operators.Int.MIN.apply(1, 2) == 1);
        Assert.assertTrue(Operators.Int.PROD.apply(1, 2) == 2);
        Assert.assertTrue(Operators.Int.BITS_AND.apply(-1, 1234) == 1234);
        Assert.assertTrue(Operators.Int.BITS_AND.apply(-1, -14343434) == -14343434);
        Assert.assertTrue(Operators.Int.BITS_OR.apply(-1, -14343434) == 0xffffffff);
        Assert.assertTrue(Operators.Int.BITS_OR.apply(1234, 3456) == 3538);
        Assert.assertTrue(Operators.Int.BITS_XOR.apply(1234, 3456) == 2386);
    }

    @Test
    public void shortTest() {
        Assert.assertTrue(Operators.Short.SUM.apply((short)1, (short)2) == (short)3);
        Assert.assertTrue(Operators.Short.MAX.apply((short)1, (short)2) == (short)2);
        Assert.assertTrue(Operators.Short.MIN.apply((short)1, (short)2) == (short)1);
        Assert.assertTrue(Operators.Short.PROD.apply((short)1, (short)2) == (short)2);
        Assert.assertTrue(Operators.Short.BITS_AND.apply((short)-1, (short)1234) == (short)1234);
        Assert.assertTrue(Operators.Short.BITS_AND.apply((short)-1, (short)-1434) == (short)-1434);
        Assert.assertTrue(Operators.Short.BITS_OR.apply((short)-1, (short)-1434) == (short)-1);
        Assert.assertTrue(Operators.Short.BITS_OR.apply((short)1234, (short)3456) == (short)3538);
        Assert.assertTrue(Operators.Short.BITS_XOR.apply((short)1234, (short)3456) == (short)2386);

    }

    @Test
    public void byteTest() {
        Assert.assertTrue(Operators.Byte.SUM.apply((byte)1, (byte)2) == (byte)3);
        Assert.assertTrue(Operators.Byte.MAX.apply((byte)1, (byte)2) == (byte)2);
        Assert.assertTrue(Operators.Byte.MIN.apply((byte)1, (byte)2) == (byte)1);
        Assert.assertTrue(Operators.Byte.PROD.apply((byte)1, (byte)2) == (byte)2);
        Assert.assertTrue(Operators.Byte.BITS_AND.apply((byte)-1, (byte)12) == (byte)12);
        Assert.assertTrue(Operators.Byte.BITS_AND.apply((byte)-1, (byte)-14) == (byte)-14);
        Assert.assertTrue(Operators.Byte.BITS_OR.apply((byte)-1, (byte)-14) == (byte)-1);
        Assert.assertTrue(Operators.Byte.BITS_OR.apply((byte)2, (byte)1) == (byte)3);
        Assert.assertTrue(Operators.Byte.BITS_XOR.apply((byte)-1, (byte)-1) == (byte)0);
    }



}
