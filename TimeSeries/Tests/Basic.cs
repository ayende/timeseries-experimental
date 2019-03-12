using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace TimeSeries.Tests
{
    public unsafe class Basic
    {
        [Fact]
        public void CanSetBits()
        {
            var buffer = stackalloc byte[16];
            var bitsBufffer = new BitsBuffer(buffer, 16);
            bitsBufffer.AddValue(12, 6);
            bitsBufffer.AddValue(3, 7);
            bitsBufffer.AddValue(279, 18);

            bitsBufffer.SetBits(6, 7, 7);

            int index = 0;
            Assert.Equal(12UL, bitsBufffer.ReadValue(ref index, 6));
            Assert.Equal(7UL, bitsBufffer.ReadValue(ref index, 7));
            Assert.Equal(279UL, bitsBufffer.ReadValue(ref index, 18));
        }

        [Fact]
        public void CanStoreSimpleValuesAndTags()
        {
            RunActualTest(
                (50, new double[] { 2, 3 }, "hello"),
                (70, new double[] { 23, 153 }, "hello 2"),
                (80, new double[] { 12, 3 }, "world"),
                (170, new double[] { 23, 133 }, "hello 3")
            );
        }

        [Fact]
        public void RepeatingNonSequentialTags()
        {
            RunActualTest(
                (ref TimeSeriesValuesSegment segment) =>
                {
                    var data = new Span<byte>(segment.GetBitsBuffer().Buffer,
                        segment.GetBitsBuffer().NumberOfBits / 8);

                    var index = data.IndexOf(Encoding.UTF8.GetBytes("world"));
                    Assert.NotEqual(-1, index);
                    Assert.Equal(-1, data.Slice(index+1).IndexOf(Encoding.UTF8.GetBytes("world")));
                },
                (50, new double[] { 2, 3 }, "hello"),
                (70, new double[] { 23, 153 }, "world"),
                (80, new double[] { 12, 3 }, "hello"),
                (170, new double[] { 23, 133 }, "world")
            );
        }

        [Fact]
        public void WithDifferentTags()
        {
            RunActualTest(
                (50, new double[] { 2, 3 }, "watches/fitbit"),
                (70, new double[] { 23, 153 }, "watches/fitbit"),
                (80, new double[] { 12, 3 }, "medical/device"),
                (170, new double[] { 23, 133 }, "watches/fitbit")
            );
        }

        private delegate void Validator(ref TimeSeriesValuesSegment segment);


        private static void RunActualTest(params (int, double[], string)[] data)
        {
            RunActualTest(null, data);
        }

        private static void RunActualTest(Validator validator, params (int, double[], string)[] data)
        {
            var buffer = stackalloc byte[1024];
            var segment = new TimeSeriesValuesSegment(buffer, 1024);
            segment.Initialize(2);

            foreach (var item in data)
            {
                segment.Append(item.Item1, item.Item2, Encoding.UTF8.GetBytes(item.Item3));
            }

            var enumerator = segment.GetEnumerator();
            var values = new Span<StatefulTimeStampValue>(new StatefulTimeStampValue[2]);
            Span<byte> tag = default;

            for (int i = 0; i < data.Length; i++)
            {
                Assert.True(enumerator.MoveNext(out int ts, values, ref tag));

                var item = data[i];
                Assert.Equal(item.Item1, ts);
                Assert.Equal(item.Item2[0], values[0].DoubleValue);
                Assert.Equal(item.Item2[1], values[1].DoubleValue);
                Assert.True(tag.SequenceEqual(new Span<byte>(Encoding.UTF8.GetBytes(item.Item3))));
            }

            validator?.Invoke(ref segment);
        }
    }
}
