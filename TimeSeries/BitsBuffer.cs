﻿using System;
using System.Diagnostics;

namespace TimeSeries
{
    public unsafe struct BitsBuffer
    {
        public const int UnusedBitsInLastByteBitLength = 3;

        byte* _buffer;
        int _size;
        private BlueEyes.BitBuffer temp;
        public BitsBufferHeader* Header;

        public int NumberOfBits => Header->BitsPosition;

        public bool HasBits(int numberOfBits)
        {
            return Header->BitsPosition + numberOfBits <= _size * 8;
        }

        public BitsBuffer(byte* buffer, int size)
        {
            Header = (BitsBufferHeader*)buffer;
            _buffer = buffer + sizeof(BitsBufferHeader);
            _size = size;
            temp = new BlueEyes.BitBuffer(1024);
        }

        public void Initialize()
        {
            AddValue(0, UnusedBitsInLastByteBitLength);
        }

        private ushort BitsAvailableInLastByte()
        {
            var numBits = NumberOfBits;
            int bitsAvailable = ((numBits & 0x7) != 0) ? (8 - (numBits & 0x7)) : 0;
            return (ushort)bitsAvailable;
        }

        public int FindTheFirstZeroBit(ref int bitsPosition, int limit)
        {
            int bits = 0;
            while (bits < limit)
            {
                var bit = ReadValue(ref bitsPosition, 1);
                if (bit == 0)
                {
                    return bits;
                }

                ++bits;
            }

            return bits;
        }

        public ulong ReadValue(ref int bitsPosition, int bitsToRead)
        {
            if (bitsToRead > 64)
                throw new ArgumentException($"Unable to read more than 64 bits at a time.  Requested {bitsToRead} bits", nameof(bitsToRead));

            if (bitsPosition + bitsToRead > _size * 8)
                throw new ArgumentException($"Not enough bits left in the buffer. Requested {bitsToRead} bits.  Current Position: {bitsPosition}", nameof(bitsToRead));

            ulong value = 0;
            for (int i = 0; i < bitsToRead; i++)
            {
                value <<= 1;
                ulong bit = (ulong)((_buffer[bitsPosition >> 3] >> (7 - (bitsPosition & 0x7))) & 1);
                value += bit;
                bitsPosition++;
            }




            return value;
        }

        public void AddValue(ulong value, int bitsInValue)
        {
            temp.AddValue(value, bitsInValue);
            Debug.Assert(HasBits(bitsInValue));

            if (bitsInValue == 0)
            {
                // Nothing to do.
                return;
            }

            var lastByteIndex = Header->BitsPosition / 8;
            var bitsAvailable = BitsAvailableInLastByte();

            Header->BitsPosition += (ushort)bitsInValue;

            if (bitsInValue <= bitsAvailable)
            {
                // The value fits in the last byte
                _buffer[lastByteIndex] += (byte)(value << (bitsAvailable - bitsInValue));
                return;
            }

            var bitsLeft = bitsInValue;
            if (bitsAvailable > 0)
            {
                // Fill up the last byte
                _buffer[lastByteIndex] += (byte)(value >> (bitsInValue - bitsAvailable));
                bitsLeft -= bitsAvailable;
                lastByteIndex++;
            }

            while (bitsLeft >= 8)
            {
                // We have enough bits to fill up an entire byte
                byte next = (byte)((value >> (bitsLeft - 8)) & 0xFF);
                _buffer[lastByteIndex++] = next;
                bitsLeft -= 8;
            }

            if (bitsLeft != 0)
            {
                // Start a new byte with the rest of the bits
                ulong mask = (ulong)((1 << bitsLeft) - 1L);
                byte next = (byte)((value & mask) << (8 - bitsLeft));
                _buffer[lastByteIndex] = next;
            }
        }
    }
}
