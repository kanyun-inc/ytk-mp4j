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

/**
 * @author xialong
 */
public final class Operators {
    public static class Double {
        public static final IDoubleOperator SUM = new IDoubleOperator() {
            @Override
            public double apply(double d1, double d2) {
                return d1 + d2;
            }
        };
        public static final IDoubleOperator MAX = new IDoubleOperator() {
            @Override
            public double apply(double d1, double d2) {
                return Math.max(d1, d2);
            }
        };
        public static final IDoubleOperator MIN = new IDoubleOperator() {
            @Override
            public double apply(double d1, double d2) {
                return Math.min(d1, d2);
            }
        };

        public static final IDoubleOperator PROD = new IDoubleOperator() {
            @Override
            public double apply(double d1, double d2) {
                return d1 * d2;
            }
        };

        public static final IDoubleOperator FLOAT_MAX_LOC = new IDoubleOperator() {
            @Override
            public double apply(double d1, double d2) {
                float d1val = getFloatVal(d1);
                float d2val = getFloatVal(d2);
                return d1val >= d2val ? d1 : d2;
            }
        };

        public static final IDoubleOperator FLOAT_MIN_LOC = new IDoubleOperator() {
            @Override
            public double apply(double d1, double d2) {
                float d1val = getFloatVal(d1);
                float d2val = getFloatVal(d2);
                return d1val <= d2val ? d1 : d2;
            }
        };

        public static double compositeDouble(float val, int loc) {
            long ret = (0xffffffffL & java.lang.Float.floatToRawIntBits(val));
            ret <<= 32;
            ret |= (0xffffffffL & loc);
            return java.lang.Double.longBitsToDouble(ret);
        }

        public static float getFloatVal(double composite) {
            return java.lang.Float.intBitsToFloat((int) ((java.lang.Double.doubleToRawLongBits(composite) & 0xffffffff00000000L) >>> 32));
        }

        public static int getIntLoc(double composite) {
            return (int)(java.lang.Double.doubleToRawLongBits(composite) & 0xffffffff);
        }
    }

    public static class Float {
        public static final IFloatOperator SUM = new IFloatOperator() {
            @Override
            public float apply(float d1, float d2) {
                return d1 + d2;
            }
        };
        public static final IFloatOperator MAX = new IFloatOperator() {
            @Override
            public float apply(float d1, float d2) {
                return Math.max(d1, d2);
            }
        };
        public static final IFloatOperator MIN = new IFloatOperator() {
            @Override
            public float apply(float d1, float d2) {
                return Math.min(d1, d2);
            }
        };

        public static final IFloatOperator PROD = new IFloatOperator() {
            @Override
            public float apply(float d1, float d2) {
                return d1 * d2;
            }
        };
    }

    public static class Long {
        public static final ILongOperator SUM = new ILongOperator() {
            @Override
            public long apply(long d1, long d2) {
                return d1 + d2;
            }
        };
        public static final ILongOperator MAX = new ILongOperator() {
            @Override
            public long apply(long d1, long d2) {
                return Math.max(d1, d2);
            }
        };
        public static final ILongOperator MIN = new ILongOperator() {
            @Override
            public long apply(long d1, long d2) {
                return Math.min(d1, d2);
            }
        };

        public static final ILongOperator BITS_AND = new ILongOperator() {
            @Override
            public long apply(long d1, long d2) {
                return d1 & d2;
            }
        };

        public static final ILongOperator BITS_OR = new ILongOperator() {

            @Override
            public long apply(long d1, long d2) {
                return d1 | d2;
            }
        };

        public static final ILongOperator BITS_XOR = new ILongOperator() {
            @Override
            public long apply(long d1, long d2) {
                return d1 ^ d2;
            }
        };

        public static final ILongOperator PROD = new ILongOperator() {
            @Override
            public long apply(long d1, long d2) {
                return d1 * d2;
            }
        };

        public static final ILongOperator INT_MAX_LOC = new ILongOperator() {
            @Override
            public long apply(long d1, long d2) {
                int d1val = getIntVal(d1);
                int d2val = getIntVal(d2);

                return d1val >= d2val ? d1 : d2;
            }
        };

        public static final ILongOperator INT_MIN_LOC = new ILongOperator() {
            @Override
            public long apply(long d1, long d2) {
                int d1val = getIntVal(d1);
                int d2val = getIntVal(d2);
                return d1val <= d2val ? d1 : d2;
            }
        };

        public static long compositeLong(int val, int loc) {
            long ret = (0xffffffffL & val);
            ret <<= 32;
            ret |= (0xffffffffL & loc);
            return ret;
        }

        public static int getIntVal(long composite) {
            return (int)((composite & 0xffffffff00000000L) >>> 32);
        }

        public static int getIntLoc(long composite) {
            return (int)(composite & 0xffffffff);
        }

    }

    public static class Int {
        public static final IIntOperator SUM = new IIntOperator() {
            @Override
            public int apply(int d1, int d2) {
                return d1 + d2;
            }
        };
        public static final IIntOperator MAX = new IIntOperator() {
            @Override
            public int apply(int d1, int d2) {
                return Math.max(d1, d2);
            }
        };
        public static final IIntOperator MIN = new IIntOperator() {
            @Override
            public int apply(int d1, int d2) {
                return Math.min(d1, d2);
            }
        };

        public static final IIntOperator BITS_AND = new IIntOperator() {
            @Override
            public int apply(int d1, int d2) {
                return d1 & d2;
            }
        };

        public static final IIntOperator BITS_OR = new IIntOperator() {

            @Override
            public int apply(int d1, int d2) {
                return d1 | d2;
            }
        };

        public static final IIntOperator BITS_XOR = new IIntOperator() {
            @Override
            public int apply(int d1, int d2) {
                return d1 ^ d2;
            }
        };

        public static final IIntOperator PROD = new IIntOperator() {
            @Override
            public int apply(int d1, int d2) {
                return d1 * d2;
            }
        };
    }

    public static class Short {
        public static final IShortOperator SUM = new IShortOperator() {
            @Override
            public short apply(short d1, short d2) {
                return (short)(d1 + d2);
            }
        };
        public static final IShortOperator MAX = new IShortOperator() {
            @Override
            public short apply(short d1, short d2) {
                return d1 >= d2 ? d1 : d2;
            }
        };
        public static final IShortOperator MIN = new IShortOperator() {
            @Override
            public short apply(short d1, short d2) {
                return d1 <= d2 ? d1 : d2;
            }
        };

        public static final IShortOperator BITS_AND = new IShortOperator() {
            @Override
            public short apply(short d1, short d2) {
                return (short)(d1 & d2);
            }
        };

        public static final IShortOperator BITS_OR = new IShortOperator() {
            @Override
            public short apply(short d1, short d2) {
                return (short)(d1 | d2);
            }
        };

        public static final IShortOperator BITS_XOR = new IShortOperator() {
            @Override
            public short apply(short d1, short d2) {
                return (short)(d1 ^ d2);
            }
        };

        public static final IShortOperator PROD = new IShortOperator() {
            @Override
            public short apply(short d1, short d2) {
                return (short)(d1 * d2);
            }
        };

    }

    public static class Byte {
        public static final IByteOperator SUM = new IByteOperator() {
            @Override
            public byte apply(byte d1, byte d2) {
                return (byte)(d1 + d2);
            }
        };
        public static final IByteOperator MAX = new IByteOperator() {
            @Override
            public byte apply(byte d1, byte d2) {
                return d1 >= d2 ? d1 : d2;
            }
        };
        public static final IByteOperator MIN = new IByteOperator() {
            @Override
            public byte apply(byte d1, byte d2) {
                return d1 <= d2 ? d1 : d2;
            }
        };

        public static final IByteOperator BITS_AND = new IByteOperator() {
            @Override
            public byte apply(byte d1, byte d2) {
                return (byte)(d1 & d2);
            }
        };

        public static final IByteOperator BITS_OR = new IByteOperator() {
            @Override
            public byte apply(byte d1, byte d2) {
                return (byte)(d1 | d2);
            }
        };

        public static final IByteOperator BITS_XOR = new IByteOperator() {
            @Override
            public byte apply(byte d1, byte d2) {
                return (byte)(d1 ^ d2);
            }
        };

        public static final IByteOperator PROD = new IByteOperator() {
            @Override
            public byte apply(byte d1, byte d2) {
                return (byte)(d1 * d2);
            }
        };
    }

}
