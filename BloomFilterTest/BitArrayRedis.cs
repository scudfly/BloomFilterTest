using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace BloomFilterTest
{
    public static class BitArrayRedis
    {
        private static readonly IDatabase Db = ConnectionMultiplexer.Connect("172.16.3.185").GetDatabase(1);

        public static void Save(BitArray bitArray, string key)
        {
            var batch = Db.CreateBatch();

            for (var i = 0; i < bitArray.Count; i++)
            {
                batch.StringSetBitAsync(key, i, bitArray[i]);
            }

            batch.Execute();
        }

        public static BitArray Load(string key, int bitArrayLength)
        {
            var batch = Db.CreateBatch();

            var taskList = new List<Task<bool>>();
            
            for (var i = 0; i < bitArrayLength; i++)
            {
                taskList.Add(batch.StringGetBitAsync(key, i));
            }

            batch.Execute();

            return new BitArray(taskList.Select(t => t.Result).ToArray());
        }
    }
}