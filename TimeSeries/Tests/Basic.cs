using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace TimeSeries.Tests
{
    public unsafe class Basic
    {
        [Fact]
        public void CanStoreSimpleValuesAndTags()
        {
            RunActualTest(
                (50, new double[] { 2, 3 }, "hello"),
                (70, new double[] { 23, 153 }, "hello 2"),
                (80, new double[] { 12, 3 }, "world"),
                (170, new double[] { 23, 133 }, "hello")
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

        private static void RunActualTest(params (int, double[], string)[] data)
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
        }
    }
}
