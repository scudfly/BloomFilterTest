using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BloomFilterDotNet;
using Dapper;
using StackExchange.Redis;
using CommandFlags = StackExchange.Redis.CommandFlags;

namespace BloomFilterTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var bitArrayLengh = 100000;

            var bitArray = new BitArray(bitArrayLengh);
            for (var i = 0; i < bitArray.Length; i++)
            {
                bitArray[i] = true;
            }

            var stopWatch1 = new PerformanceTimer();

            stopWatch1.Start();

            BitArrayRedis.Save(bitArray, "aaaa");

            stopWatch1.Stop();

            Console.WriteLine($"保存耗时{stopWatch1.Duration}秒");


            stopWatch1.Start();

            var bitArray2 = BitArrayRedis.Load("aaaa", bitArrayLengh);

            stopWatch1.Stop();

            Console.WriteLine($"读取耗时{stopWatch1.Duration}秒");

            //Console.WriteLine(bitArray2.GetHashCode() == bitArray.GetHashCode() ? "结果一致" : "结果不一致");

            var flag2 = true;

            var bitArray3 = bitArray.Xor(bitArray2);

            for (var i = 0; i < bitArray.Length; i++)
            {
                flag2 = flag2 && bitArray3[i];

            }
            
            Console.WriteLine(flag2 ? "最终结果不一致" : "最终结果一致");

            Console.ReadKey();


            return;




            var bitLength = 100;

            var bfMail = new BloomFilter2<string>(bitLength, 20);

            bfMail.Add("a@a.com");
            bfMail.Add("b@b.com");
            bfMail.Add("c@c.com");
            bfMail.Add("d@d.com");
            bfMail.Add("e@e.com");
            bfMail.Add("f@f.com");
            bfMail.Add("g@g.com");
            bfMail.Add("h@h.com");
            bfMail.Add("i@i.com");
            bfMail.Add("j@j.com");
            bfMail.Add("k@k.com");
            bfMail.Add("l@l.com");
            bfMail.Add("m@m.com");
            bfMail.Add("n@n.com");
            bfMail.Add("o@o.com");
            bfMail.Add("p@p.com");
            bfMail.Add("q@q.com");
            bfMail.Add("r@r.com");
            bfMail.Add("s@s.com");
            bfMail.Add("t@t.com");

            //bfMail.Add("u@u.com");
            //bfMail.Add("v@v.com");
            //bfMail.Add("w@w.com");
            //bfMail.Add("x@x.com");
            //bfMail.Add("y@y.com");
            //bfMail.Add("z@z.com");



            bool flag;

            flag = bfMail.Contains("a@a.com");
            flag = bfMail.Contains("b@b.com");
            flag = bfMail.Contains("c@c.com");
            flag = bfMail.Contains("d@d.com");
            flag = bfMail.Contains("e@e.com");
            flag = bfMail.Contains("f@f.com");
            flag = bfMail.Contains("g@g.com");
            flag = bfMail.Contains("h@h.com");
            flag = bfMail.Contains("i@i.com");
            flag = bfMail.Contains("j@j.com");
            flag = bfMail.Contains("k@k.com");
            flag = bfMail.Contains("l@l.com");
            flag = bfMail.Contains("m@m.com");
            flag = bfMail.Contains("n@n.com");
            flag = bfMail.Contains("o@o.com");
            flag = bfMail.Contains("p@p.com");
            flag = bfMail.Contains("q@q.com");
            flag = bfMail.Contains("r@r.com");
            flag = bfMail.Contains("s@s.com");
            flag = bfMail.Contains("t@t.com");
            flag = bfMail.Contains("u@u.com");
            flag = bfMail.Contains("v@v.com");
            flag = bfMail.Contains("w@w.com");
            flag = bfMail.Contains("x@x.com");
            flag = bfMail.Contains("y@y.com");
            flag = bfMail.Contains("z@z.com");







            var bfMailArray =  bfMail.GetBitArray();



            var a = ConnectionMultiplexer.Connect("172.16.3.185").GetDatabase(1);

            

            var batch = a.CreateBatch();

            for (var i = 0; i < bfMailArray.Count; i++)
            {
                batch.StringSetBitAsync("ValueDateBloomFilter", i, bfMailArray[i]);
            }

            batch.Execute();


            var newMailArray = new BitArray(bitLength);

            var batch2 = a.CreateBatch();

            var taskList = new List<Task<bool>>();

            for (var i = 0; i < bfMailArray.Count; i++)
            {
                //newMailArray[i] = a.StringGetBit("ValueDateBloomFilter", i);

                taskList.Add(batch2.StringGetBitAsync("ValueDateBloomFilter", i));
            }

            batch2.Execute();

            for (var i = 0; i < bfMailArray.Count; i++)
            {
                newMailArray[i] = taskList[i].Result;
            }
            
            var bfMail2 = new BloomFilter2<string>(bitLength, 20, newMailArray);

            var wjl = bfMail2.FalsePositiveProbability();


            flag = bfMail2.Contains("a@a.com");
            flag = bfMail2.Contains("b@b.com");
            flag = bfMail2.Contains("c@c.com");
            flag = bfMail2.Contains("d@d.com");
            flag = bfMail2.Contains("e@e.com");
            flag = bfMail2.Contains("f@f.com");
            flag = bfMail2.Contains("g@g.com");
            flag = bfMail2.Contains("h@h.com");
            flag = bfMail2.Contains("i@i.com");
            flag = bfMail2.Contains("j@j.com");
            flag = bfMail2.Contains("k@k.com");
            flag = bfMail2.Contains("l@l.com");
            flag = bfMail2.Contains("m@m.com");
            flag = bfMail2.Contains("n@n.com");
            flag = bfMail2.Contains("o@o.com");
            flag = bfMail2.Contains("p@p.com");
            flag = bfMail2.Contains("q@q.com");
            flag = bfMail2.Contains("r@r.com");
            flag = bfMail2.Contains("s@s.com");
            flag = bfMail2.Contains("t@t.com");
            flag = bfMail2.Contains("u@u.com");
            flag = bfMail2.Contains("v@v.com");
            flag = bfMail2.Contains("w@w.com");
            flag = bfMail2.Contains("x@x.com");
            flag = bfMail2.Contains("y@y.com");
            flag = bfMail2.Contains("z@z.com");


            //a.GetDatabase(1).StringSetBit("ValueDateBloomFilter", 0, true);
            //a.GetDatabase(1).StringSetBit("ValueDateBloomFilter", 1, false);
            //a.GetDatabase(1).StringSetBit("ValueDateBloomFilter", 2, true);
            //a.GetDatabase(1).StringSetBit("ValueDateBloomFilter", 3, false);

            //var b = a.GetDatabase(1).StringGetBit("ValueDateBloomFilter", 0);
            //b = a.GetDatabase(1).StringGetBit("ValueDateBloomFilter", 1);
            //b = a.GetDatabase(1).StringGetBit("ValueDateBloomFilter", 2);
            //b = a.GetDatabase(1).StringGetBit("ValueDateBloomFilter", 3);

            return;


            var bf = new BloomFilter2<string>(20, 3);

            bf.Add("testing");
            //bf.Add("nottesting");
            //bf.Add("testingagain");

            //Console.WriteLine(bf.Contains("badstring")); // False  
            Console.WriteLine(bf.Contains("testing")); // True  

            List<string> testItems = new List<string>() { "badstring", "testing", "test" };

            Console.WriteLine(bf.ContainsAll(testItems)); // False  
            Console.WriteLine(bf.ContainsAny(testItems)); // True  

            //误检率: 0.040894188143892  
            Console.WriteLine("False Positive Probability: " + bf.FalsePositiveProbability());

            Console.ReadKey();

     

            var valueDateList = GetValueDate();

            var bloomFilter = CreateBloomFilter(valueDateList);

            var testKey1 = "1414878402940000100--714878314580000218|10479|714878314580001618|10424|-0-0";



            var stopWatch = new PerformanceTimer();

            stopWatch.Start();

            var redisResult = SearchRedis(testKey1);

            stopWatch.Stop();

            Console.WriteLine($"Redis搜索耗时{stopWatch.Duration}秒， 结果{redisResult}");


            stopWatch.Start();

            var bloomFilterResult = bloomFilter.Contains(testKey1);

            stopWatch.Stop();

            Console.WriteLine($"布隆过滤器搜索耗时{stopWatch.Duration}秒， 结果{bloomFilterResult}");




            //WriteRedis();

            //var sourceList = new List<int>();

            //var bloomFilter = new BloomFilter<int>(5000, HashFunctions.HashInt);

            //for (var i = 1; i <= 100000000; i++)
            //{
            //    sourceList.Add(i);
            //    bloomFilter.Add(i);
            //}


            //var a = 3733357;
            //var b = 334293493;

            //var stopWatch = new PerformanceTimer();

            //stopWatch.Start();

            //sourceList.Contains(a);

            //stopWatch.Stop();

            //var aElaped = stopWatch.Duration;

            //Console.WriteLine($"数组搜索耗时{aElaped}纳秒");

            //stopWatch.Start();

            //bloomFilter.Contains(a);

            //stopWatch.Stop();

            // var bElaped = stopWatch.Duration;


            //Console.WriteLine($"数组搜索耗时{bElaped}纳秒");

            //var total = 10000;

            //var error = 0;

            //for (var i = 1; i <= total; i++)
            //{
            //    var listResult = sourceList.Contains(i);
            //    var bloomResult = bloomFilter.Contains(i);

            //    if (listResult != bloomResult)
            //    {
            //        error++;
            //        Console.WriteLine("误判");
            //    }

            //}

            //var errorPencent = (decimal)error / (decimal)total;

            //Console.WriteLine($"结束, 误判数{error}, 误判率{errorPencent}");


            Console.ReadKey();
        }


        static List<ValueDate> GetValueDate()
        {
            var result = new List<ValueDate>();

            var connStr = "Server=172.16.3.185;Database=Caad.Setting;uid=sa;pwd=Z77KHj-XZ;";

            using (var conn = new System.Data.SqlClient.SqlConnection(connStr))
            {
                var sql = @"SELECT [RowId],[DateList] FROM [ValueDate]";

                var multi = conn.QueryMultiple(sql);

                while (!multi.IsConsumed)
                {
                    result = multi.Read<ValueDate>().ToList();
                }
            }

            return result;
        }

        static BloomFilter<string> CreateBloomFilter(List<ValueDate> valueDateList)
        {
            var a = HashFunctions.HashString("1414878402940000100--714878314580000218|10479|714878314580001618|10424|-0-0");

            var bloomFilter = new BloomFilter<string>(100000, HashFunctions.HashString);

            valueDateList.ForEach(vd =>
            {
                bloomFilter.Add(vd.RowId);
            });

            return bloomFilter;
        }

        static void WriteRedis(List<ValueDate> valueDateList)
        {
            var c = ConfigurationOptions.Parse("172.16.3.185");

            c.AllowAdmin = true;
            c.Password = "";

            var redis = ConnectionMultiplexer.Connect(c).GetDatabase(0);


            var a = ConnectionMultiplexer.Connect("172.16.3.185");

            a.GetDatabase(1).StringSetBit("ValueDateBloomFilter", 0, true);
            a.GetDatabase(1).StringSetBit("ValueDateBloomFilter", 1, false);
            a.GetDatabase(1).StringSetBit("ValueDateBloomFilter", 2, true);
            a.GetDatabase(1).StringSetBit("ValueDateBloomFilter", 3, false);


            var b = a.GetDatabase(1).StringGetBit("ValueDateBloomFilter", 0);

            valueDateList.ForEach(vd =>
            {
                redis.SetAdd(vd.RowId, vd.DateList);
            });
        }

        static bool SearchRedis(string key)
        {
            var redis = ConnectionMultiplexer.Connect("172.16.3.185").GetDatabase(0);

            return redis.KeyExists(key);
        }
    }

    class ValueDate
    {
        public string RowId { get; set; }

        public string DateList { get; set; }
    }
}
