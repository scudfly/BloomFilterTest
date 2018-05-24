using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace BloomFilterTest
{
    /// <summary>  
    /// 一个布隆过滤器是一个空间有效的概率数据结构  
    /// 用于测试一个元素是否是一个集合的成员。误检率是可能的，但漏检率是不存在的。元素可以被添加到集合，但不能从集合删除。  
    /// </summary>  
    /// <typeparam name="T">泛型数据类型</typeparam>  
    public class BloomFilter2<T>
    {
        private Random _random;
        private readonly BitArray _bitArray;

        #region 属性  

        private int NumberOfHashes { set; get; }

        private int SetSize { set; get; }

        private int BitSize { set; get; }

        #endregion

        #region Constructors  
        /// <summary>  
        /// 初始化bloom滤波器并设置hash散列的最佳数目  
        /// </summary>  
        /// <param name="bitSize">布隆过滤器的大小(m)</param>  
        /// <param name="setSize">集合的大小 (n)</param>  
        public BloomFilter2(int bitSize, int setSize)
        {
            BitSize = bitSize;
            _bitArray = new BitArray(bitSize);
            SetSize = setSize;
            NumberOfHashes = OptimalNumberOfHashes(BitSize, SetSize);
        }

        public BloomFilter2(int bitSize, int setSize, BitArray bitArray)
        {
            BitSize = bitSize;
            _bitArray = bitArray;
            SetSize = setSize;
            NumberOfHashes = OptimalNumberOfHashes(BitSize, SetSize);
        }

        //<param name="numberOfHashes">hash散列函数的数量(k)</param>  
        public BloomFilter2(int bitSize, int setSize, int numberOfHashes)
        {
            BitSize = bitSize;
            _bitArray = new BitArray(bitSize);
            SetSize = setSize;
            NumberOfHashes = numberOfHashes;
        }
        #endregion



        #region 公共方法  
        public void Add(T item)
        {
            var a = Hash(item);

            _random = new Random(a);

            for (var i = 0; i < NumberOfHashes; i++)
            {
                var b = _random.Next(BitSize);
                _bitArray[b] = true;
            }
        }
        public bool Contains(T item)
        {
            var a = Hash(item);

            _random = new Random(a);

            for (var i = 0; i < NumberOfHashes; i++)
            {
                var b = _random.Next(BitSize);
                if (!_bitArray[b])
                    return false;
            }
            return true;
        }

        //检查列表中的任何项是否可能是在集合。  
        //如果布隆过滤器包含列表中的任何一项，返回真  
        public bool ContainsAny(IEnumerable<T> items)
        {
            return items.Any(Contains);
        }

        //检查列表中的所有项目是否都在集合。  
        public bool ContainsAll(IEnumerable<T> items)
        {
            return items.All(Contains);
        }

        /// <summary>  
        /// 计算遇到误检率的概率。  
        /// </summary>  
        /// <returns>Probability of a false positive</returns>  
        public double FalsePositiveProbability()
        {
            return Math.Pow(1 - Math.Exp(-NumberOfHashes * SetSize / (double)BitSize), NumberOfHashes);
        }

        public BitArray GetBitArray()
        {
            return _bitArray;
        }

        #endregion

        #region 私有方法  
        private int Hash(T item)
        {
            return item.GetHashCode();
        }

        //计算基于布隆过滤器散列的最佳数量  
        private int OptimalNumberOfHashes(int bitSize, int setSize)
        {
            return (int)Math.Ceiling(bitSize / setSize * Math.Log(2.0));
        }
        #endregion
    }
}
