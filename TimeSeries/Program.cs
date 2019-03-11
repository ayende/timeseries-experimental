using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace TimeSeries
{
    unsafe class Program
    {
        static void Main(string[] args)
        {
            // https://raw.githubusercontent.com/fivethirtyeight/data/master/births/US_births_1994-2003_CDC_NCHS.csv

            //var items = File.ReadLines(@"C:\Users\ayende\Downloads\US_births_1994-2003_CDC_NCHS.csv")
            //    .Skip(1)
            //    .Select(line =>
            //    {
            //        var parts = line.Split(',');
            //        return (
            //            Date: new DateTime(int.Parse(parts[0]), int.Parse(parts[1]), int.Parse(parts[2])),
            //            Vals: new double[]
            //            {
            //                double.Parse(parts[4])
            //            }
            //        );
            //    })
            //    .OrderBy(x=>x.Date)
            //    .ToArray();

            //var buffer = (byte*)Marshal.AllocHGlobal(2048);
            //var segment = new TimeSeriesValuesSegment(buffer, 2048);
            //segment.Initialize(1);

            //int index = 0;

            //var baseDate = items[0].Date;
            ////foreach (var item in _heartRate)
            ////{
            ////    var ms = index * 500;

            ////    if (segment.Append(ms, Math.Round(item, 0)) == false)
            ////        break;
            ////    index++;
            ////}

            //foreach (var item in items)
            //{
            //    var ms = (int)(item.Date - baseDate).TotalMilliseconds;

            //    if (segment.Append(ms, item.Vals[0], Encoding.UTF8.GetBytes("machines/1-A")) == false)
            //        break;
            //    index++;
            //}
            //Console.WriteLine(segment.GetBitsBuffer().NumberOfBits / 8);
            //Console.WriteLine(index + " " + items.Length);

            //var enumerator = segment.GetEnumerator();
            //var vals = new StatefulTimeStampValue[1];
            //Span<byte> tag = default;
            //for (int i = 0; i < index; i++)
            //{
            //    var a = enumerator.MoveNext(out var ms, vals, ref tag);
            //    if (a == false)
            //    {
            //        Console.WriteLine("missing " + i);
            //        break;
            //    }
            //    if (tag.SequenceEqual(Encoding.UTF8.GetBytes("machines/1-A")) == false)
            //    {
            //        Console.WriteLine("Tag " + i);
            //        break;
            //    }
            //    var expected = (int)(items[i].Date - baseDate).TotalMilliseconds;
            //    if (ms != expected)
            //    {
            //        Console.WriteLine("date " + i );
            //        break;
            //    }
            //    if (vals[0].DoubleValue != items[i].Vals[0])
            //    {
            //        Console.WriteLine("val " + i);
            //        break;
            //    }
            //}

            //Console.WriteLine(index);

            //var buffer = (byte*)Marshal.AllocHGlobal(2048);
            //var segment = new TimeSeriesValuesSegment(buffer, 2048);
            //segment.Initialize(2);

            //segment.Append(50, new double[] { 2, 3 }, Encoding.UTF8.GetBytes("hello"));
            //segment.Append(70, new double[] { 23, 153 }, Encoding.UTF8.GetBytes("hello 2"));

            //segment.Append(80, new double[] { 12, 3 }, Encoding.UTF8.GetBytes("world"));
            //segment.Append(170, new double[] { 23, 133 }, Encoding.UTF8.GetBytes("hello"));

            //Console.WriteLine(segment.GetBitsBuffer().NumberOfBits / 8);

            //var enumerator = segment.GetEnumerator();
            //var values = new Span<StatefulTimeStampValue>(new StatefulTimeStampValue[2]);
            //Span<byte> tag = default;
            //while (enumerator.MoveNext(out int ts, values, ref tag))
            //{
            //    Console.WriteLine(ts + " " +
            //            string.Join(", ", values.ToArray().Select(x => x.DoubleValue)) + " " +
            //            Encoding.UTF8.GetString(tag)
            //    );
            //}
        }
    }
}
