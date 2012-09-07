using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using ServiceStack.Redis;
using System.Runtime.Serialization.Formatters.Binary;

namespace CohlintHelpers
{
    /// <summary>
    /// Redis ClientHelper
    /// </summary>
    public class RedisHelper
    {
        List<string> redisServerList = null;

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="redisHosts">参数格式为：host:port</param>
        public RedisHelper(List<string> redisServerList)
        {
            if (redisServerList == null || redisServerList.Count == 0)
            {
                throw new Exception("redis server list cannot be null or empty");
            }
            this.redisServerList = redisServerList;
        }

        /// <summary>
        /// 添加对象到指定redis server中
        /// </summary>
        /// <typeparam name="T">对象类型</typeparam>
        /// <param name="cacheKey">缓存键</param>
        /// <param name="value"></param>
        /// <param name="expiry"></param>
        /// <param name="timeout"></param>
        /// <param name="isCompress">缓存到redis之前，是否进行gzip压缩，默认会进行压缩</param>
        public void Add<T>(string cacheKey, T value, int timeout = 1000, bool isCompress = true)
        {
            if (value == null)
            {
                throw new Exception("can't cache null to redis server");
            }

            var bytes = Compression.SerializeAndCompress(value, isCompress);
            if (bytes == null)
            {
                throw new Exception("compress value error");
            }

            foreach (var server in redisServerList)
            {
                var serverInfo = server.Split(':');
                var ip = serverInfo[0];
                var port = int.Parse(serverInfo[1]);
                using (var client = new RedisClient(ip, port))
                {
                   client.Set(cacheKey, bytes);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cacheKey"></param>
        /// <param name="value"></param>
        /// <param name="slidingExpiration"></param>
        /// <param name="timeout"></param>
        /// <param name="isCompress"></param>
        public void Add<T>(string cacheKey, T value, TimeSpan slidingExpiration, int timeout = 1000, bool isCompress = true)
        {
            if (value == null)
            {
                return;
            }

            byte[] bytes = Compression.SerializeAndCompress(value, isCompress);
            if (bytes == null)
            {
                return;
            }

            foreach (var server in redisServerList)
            {
                var serverInfo = server.Split(':');
                var ip = serverInfo[0];
                var port = int.Parse(serverInfo[1]);
                using (var client = new RedisClient(ip, port))
                {
                    client.SendTimeout = timeout;
                    client.Set(cacheKey, bytes, slidingExpiration);
                }
            }
        }

        /// <summary>
        /// 获取数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cacheKey"></param>
        /// <param name="timeout"></param>
        /// <param name="isDecompress"></param>
        /// <returns></returns>
        public T Get<T>(string cacheKey, int timeout = 1000, bool isDecompress = true)
        {
            var r = default(T);
            if (string.IsNullOrEmpty(cacheKey))
            {
                throw new Exception("cache key cannot be null or empty");
            }
            Byte[] bytes = null;

            foreach (var server in redisServerList)
            {
                var serverInfo = server.Split(':');
                var ip = serverInfo[0];
                var port = int.Parse(serverInfo[1]);
                using (var client = new RedisClient(ip, port))
                {
                    client.SendTimeout = timeout;

                    //如果该server上存在key，则直接返回该值。
                    if (client.Exists(cacheKey) == 1)
                    {
                        bytes = client.Get<byte[]>(cacheKey);
                        break;
                    }
                }
            }

            if (bytes != null)
            {
                r = BytesToObject<T>(cacheKey, bytes, isDecompress);
                if (r == null)
                {
                    Remove(cacheKey);
                }
            }

            return r;
        }

        /// <summary>
        /// 移除key
        /// </summary>
        /// <param name="cacheKey"></param>
        /// <param name="timeout"></param>
        public void Remove(string cacheKey, int timeout = 1000)
        {
            foreach (var server in redisServerList)
            {
                var serverInfo = server.Split(':');
                var ip = serverInfo[0];
                var port = int.Parse(serverInfo[1]);
                using (var client = new RedisClient(ip, port))
                {
                    client.SendTimeout = timeout;

                    if (client.Exists(cacheKey) == 1)
                    {
                        client.Remove(cacheKey);
                    }
                }
            }
        }

        private T BytesToObject<T>(string cacheKey, Byte[] bytes, bool isDecompress)
        {
            T o = default(T);
            if (bytes != null)
            {
                o = (T)Compression.DecompressAndDeserialze(bytes, isDecompress);
                return o;
            }
            return o;
        }
    }

    static class Compression
    {
        #region = SerializeAndCompress =
        /// <summary>
        /// 序列化并且压缩
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static byte[] SerializeAndCompress(object obj, bool isCompress)
        {
            if (isCompress)
            {
                using (MemoryStream ms = new MemoryStream())
                {
                    using (GZipStream zs = new GZipStream(ms, CompressionMode.Compress, true))
                    {
                        BinaryFormatter bf = new BinaryFormatter();
                        if (zs != null)
                        {
                            bf.Serialize(zs, obj);
                        }
                        else
                        {
                            return null;
                        }
                    }
                    return ms.ToArray();
                }
            }
            using (MemoryStream stream = new MemoryStream())
            {
                new BinaryFormatter().Serialize(stream, obj);
                return stream.ToArray();
            }
        }
        #endregion

        #region = DecompressAndDeserialze =
        /// <summary>
        /// 解压并反序列化
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        public static object DecompressAndDeserialze(byte[] data, bool isDecompress)
        {
            if (isDecompress)
            {
                using (MemoryStream ms = new MemoryStream(data))
                {
                    using (GZipStream zs = new GZipStream(ms, CompressionMode.Decompress, true))
                    {
                        BinaryFormatter bf = new BinaryFormatter();
                        return bf.Deserialize(zs);
                    }
                }
            }


            using (MemoryStream stream = new MemoryStream(data))
            {
                return new BinaryFormatter().Deserialize(stream);
            }
        }
        #endregion
    }
}
