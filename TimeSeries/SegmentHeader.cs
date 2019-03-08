using System.Runtime.InteropServices;

namespace TimeSeries
{
    [StructLayout(LayoutKind.Explicit, Size = 16)]
    public unsafe struct SegmentHeader
    {
        [FieldOffset(0)]
        public int PreviousTimeStamp;
        [FieldOffset(4)]
        public int PreviousDelta;
        [FieldOffset(8)]
        public byte NumberOfValues;
        [FieldOffset(9)]
        public fixed byte Reserved[7];
    }
}
