﻿using System;
using System.Runtime.Intrinsics.X86;

namespace TimeSeries
{
    public unsafe struct TimeSeriesValuesSegment
    {
        public const int BitsForFirstTimestamp = 31;
        public const int LeadingZerosLengthBits = 5;
        public const int BlockSizeAdjustment = 1;
        public const int DefaultDelta = 60;

        public const int MaxLeadingZerosLength = (1 << LeadingZerosLengthBits) - 1;
        public const int BlockSizeLengthBits = 6;

        private byte* _buffer;
        private int _capacity;
        private SegmentHeader* Header => (SegmentHeader*)_buffer;

        public TimeSeriesValuesSegment(byte* buffer, int capacity)
        {
            _buffer = buffer;
            _capacity = capacity;
        }

        public bool IsEmpty => 
            ((BitsBufferHeader*)(_buffer + DataStart))->BitsPosition == 
            BitsBuffer.UnusedBitsInLastByteBitLength;

        private int DataStart => sizeof(SegmentHeader) + sizeof(StatefulTimeStampValue) * Header->NumberOfValues;

        public void Initialize(int numberOfValues)
        {
            new Span<byte>(_buffer, _capacity).Clear();
            if (numberOfValues > 32)
                ThrowValuesOutOfRange(numberOfValues);
            if (_capacity > ushort.MaxValue)
                ThrowInvalidCapacityLength();

            Header->NumberOfValues = (byte)numberOfValues;

            new BitsBuffer(_buffer + DataStart, _capacity - DataStart).Initialize();
        }

        public bool Append(int deltaFromStart, double val)
        {
            return Append(deltaFromStart, new Span<double>(&val, 1));
        }

        public bool Append(int deltaFromStart, Span<double> vals)
        {
            if (vals.Length != Header->NumberOfValues)
                ThrowInvalidNumberOfValues(vals);

            var bitsBuffer = GetBitsBuffer();
            // here it is cheaper to check a value that is somewhat bigger than the max
            // value we could ever write here than to compute the exact size. Worst case,
            // we'll open a segment a few bytes early. 
            if (bitsBuffer.HasBits((vals.Length + 2) * sizeof(long) * 8) == false)
                return false;

            var prevs = new Span<StatefulTimeStampValue>(_buffer + sizeof(SegmentHeader), Header->NumberOfValues);
            AddTimeStamp(deltaFromStart, ref bitsBuffer);

            for (int i = 0; i < vals.Length; i++)
            {
                AddValue(ref prevs[i], ref bitsBuffer, vals[i]);
            }
            Header->PreviousTimeStamp = deltaFromStart;
            return true;
        }

        public BitsBuffer GetBitsBuffer() => new BitsBuffer(_buffer + DataStart, _capacity - DataStart);

        private void AddTimeStamp(int deltaFromStart, ref BitsBuffer bitsBuffer)
        {
            if (IsEmpty)
            {
                bitsBuffer.AddValue((ulong)deltaFromStart, BitsForFirstTimestamp);
                Header->PreviousDelta = DefaultDelta;
                return;
            }

            int delta = deltaFromStart - Header->PreviousTimeStamp;
            int deltaOfDelta = delta - Header->PreviousDelta;
            if (deltaOfDelta == 0)
            {
                bitsBuffer.AddValue(0, 1);
                return;
            }
            if (deltaOfDelta > 0)
            {
                // There are no zeros. Shift by one to fit in x number of bits
                deltaOfDelta--;
            }

            int absValue = Math.Abs(deltaOfDelta);
            foreach (var timestampEncoding in TimestampEncodingDetails.Encodings)
            {
                if (absValue < timestampEncoding.MaxValueForEncoding)
                {
                    bitsBuffer.AddValue((ulong)timestampEncoding.ControlValue, timestampEncoding.ControlValueBitLength);

                    // Make this value between [0, 2^timestampEncodings[i].bitsForValue - 1]
                    long encodedValue = deltaOfDelta + timestampEncoding.MaxValueForEncoding;
                    bitsBuffer.AddValue((ulong)encodedValue, timestampEncoding.BitsForValue);

                    break;
                }
            }
            Header->PreviousTimeStamp = deltaFromStart;
            Header->PreviousDelta = delta;
        }

        private void AddValue(ref StatefulTimeStampValue prev, ref BitsBuffer bitsBuffer, double dblVal)
        {
            long val = BitConverter.DoubleToInt64Bits(dblVal);
            ulong xorWithPrevious = (ulong)(prev.LongValue ^ val);
            if (xorWithPrevious == 0)
            {
                // It's the same value.
                bitsBuffer.AddValue(0, 1);
                return;
            }

            bitsBuffer.AddValue(1, 1);

            var leadingZeroes = (int)Lzcnt.X64.LeadingZeroCount(xorWithPrevious);
            var trailingZeroes = (int)Bmi1.X64.TrailingZeroCount(xorWithPrevious);
      
            if (leadingZeroes > MaxLeadingZerosLength)
                leadingZeroes = MaxLeadingZerosLength;

            var useful = 64 - leadingZeroes - trailingZeroes;
            var prevUseful = 64 - prev.LeadingZeroes - prev.TrailingZeroes;

            var expectedSize = LeadingZerosLengthBits + BlockSizeLengthBits + useful;
            if (leadingZeroes >= prev.LeadingZeroes &&
                trailingZeroes >= prev.TrailingZeroes &&
                prevUseful < expectedSize)
            {
                // Control bit saying we should use the previous block information
                bitsBuffer.AddValue(1, 1);

                // Write the parts of the value that changed.
                ulong blockValue = xorWithPrevious >> prev.TrailingZeroes;
                bitsBuffer.AddValue(blockValue, prevUseful);
            }
            else
            {
                bitsBuffer.AddValue(0, 1);
                bitsBuffer.AddValue((ulong)leadingZeroes, LeadingZerosLengthBits);
                bitsBuffer.AddValue((ulong)(useful - BlockSizeAdjustment), BlockSizeLengthBits);
                ulong blockValue = xorWithPrevious >> trailingZeroes;
                bitsBuffer.AddValue(blockValue, useful);
                prev.LeadingZeroes = (byte)leadingZeroes;
                prev.TrailingZeroes = (byte)trailingZeroes;
            }
            prev.DoubleValue = dblVal;
        }

        public Enumerator GetEnumerator() => new Enumerator(this);

        public unsafe ref struct Enumerator
        {
            private readonly TimeSeriesValuesSegment _parent;
            private int _bitsPosisition;
            private int _previousTimeStamp, _previousTimeStampDelta;

            public Enumerator(TimeSeriesValuesSegment parent)
            {
                _parent = parent;
                _bitsPosisition = BitsBuffer.UnusedBitsInLastByteBitLength;
                _previousTimeStamp = _previousTimeStampDelta = -1;
            }

            public bool MoveNext(out int timestamp, Span<StatefulTimeStampValue> values)
            {
                if (values.Length != _parent.Header->NumberOfValues)
                    ThrowInvalidNumberOfValues();

                var bitsBuffer = _parent.GetBitsBuffer();

                if (_bitsPosisition == bitsBuffer.NumberOfBits)
                {
                    timestamp = default;
                    return false;
                }
                if (_bitsPosisition == BitsBuffer.UnusedBitsInLastByteBitLength)
                {
                    // we use the values as the statement location for the previous values as well
                    values.Clear();
                }

                timestamp = ReadTimeStamp(bitsBuffer);

                for (int i = 0; i < values.Length; i++)
                {
                    var nonZero = bitsBuffer.ReadValue(ref _bitsPosisition, 1);
                    if (nonZero == 0)
                    {
                        continue; // no change since last time
                    }
                    var usePreviousBlockInfo = bitsBuffer.ReadValue(ref _bitsPosisition, 1);
                    long xorValue;

                    if (usePreviousBlockInfo == 1)
                    {
                        xorValue = (long)bitsBuffer.ReadValue(ref _bitsPosisition, 64 - values[i].LeadingZeroes - values[i].TrailingZeroes);
                        xorValue <<= values[i].TrailingZeroes;
                    }
                    else
                    {
                        int leadingZeros = (int)bitsBuffer.ReadValue(ref _bitsPosisition, LeadingZerosLengthBits);
                        int blockSize = (int)bitsBuffer.ReadValue(ref _bitsPosisition, BlockSizeLengthBits) + BlockSizeAdjustment;
                        int trailingZeros = 64 - blockSize - leadingZeros;

                        xorValue = (long)bitsBuffer.ReadValue(ref _bitsPosisition, blockSize);
                        xorValue <<= trailingZeros;

                        values[i].TrailingZeroes = (byte)trailingZeros;
                        values[i].LeadingZeroes = (byte)leadingZeros;
                    }

                    values[i].LongValue = values[i].LongValue ^ xorValue;
                }

                return true;
            }

            private int ReadTimeStamp(BitsBuffer bitsBuffer)
            {
                if (_bitsPosisition == BitsBuffer.UnusedBitsInLastByteBitLength)
                {
                    _previousTimeStamp = (int)bitsBuffer.ReadValue(ref _bitsPosisition, BitsForFirstTimestamp);
                    _previousTimeStampDelta = DefaultDelta;
                    return _previousTimeStamp;
                }

                var type = bitsBuffer.FindTheFirstZeroBit(ref _bitsPosisition, TimestampEncodingDetails.MaxControlBitLength);
                if (type > 0)
                {
                    var index = type - 1;
                    ref var encoding = ref TimestampEncodingDetails.Encodings[index];
                    long decodedValue = (long)bitsBuffer.ReadValue(ref _bitsPosisition, encoding.BitsForValue);
                    // [0, 255] becomes [-128, 127]
                    decodedValue -= encoding.MaxValueForEncoding;
                    if (decodedValue >= 0)
                    {
                        // [-128, 127] becomes [-128, 128] without the zero in the middle
                        decodedValue++;
                    }
                    _previousTimeStampDelta += (int)decodedValue;
                }
                _previousTimeStamp += _previousTimeStampDelta;

                return _previousTimeStamp;
            }

            private void ThrowInvalidNumberOfValues()
            {
                throw new ArgumentOutOfRangeException("The values span provided must have a length of exactly: " + _parent.Header->NumberOfValues);
            }
        }


        private void ThrowInvalidNumberOfValues(Span<double> vals)
        {
            throw new ArgumentOutOfRangeException("Expected to have " + Header->NumberOfValues + " values, but was provided with: " + vals.Length);
        }

        private void ThrowInvalidCapacityLength()
        {
            throw new ArgumentOutOfRangeException("TimeSeriesValuesSegment can handle a size of up to 65,535, but was: " + _capacity);
        }

        private static void ThrowValuesOutOfRange(int numberOfValues)
        {
            throw new ArgumentOutOfRangeException("TimeSeriesValuesSegment can handle up to 32 values, but had: " + numberOfValues);
        }



    }
}
