using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace Dapper.PagedDapper
{
    // <summary>
    /// The Dapper.PagedDapper extensions for Dapper
    /// Derived from Dapper.Contrib extensions for Dapper https://github.com/StackExchange/Dapper/tree/master/Dapper.Contrib
    /// </summary>
    public static partial class PagedDapperExtensions
    {
        /// <summary>
        /// Returns a list of entites from table "Ts".  
        /// Id of T must be marked with [Key] attribute.
        /// Entities created from interfaces are tracked/intercepted for changes and used by the Update() extension
        /// for optimal performance. 
        /// </summary>
        /// <typeparam name="T">Interface or type to create and populate</typeparam>
        /// <param name="connection">Open SqlConnection</param>
        /// <param name="transaction">The transaction to run under, null (the default) if none</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
        /// <returns>Entity of T</returns>
        public static Task<IEnumerable<T>> GetPagedAsync<T>(this IDbConnection connection, int pageNumber = 1, int pageSize = 100, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            if (pageNumber <= 0 || pageSize <= 0)
                throw new ArgumentException($"pageNumber and pageSize parameters must be greater than zero.");

            var type = typeof(T);
            var cacheType = typeof(List<T>);

            if (!GetQueries.TryGetValue(cacheType.TypeHandle, out string sql))
            {
                GetSingleKey<T>(nameof(GetPaged));
                var tableName = GetTableName(type);
                var adapter = GetFormatter(connection);
                sql = adapter.GetPagedQuery(tableName, pageNumber, pageSize);
                GetQueries[cacheType.TypeHandle] = sql;
            }
            if (!type.IsInterface)
            {
                return connection.QueryAsync<T>(sql, null, transaction, commandTimeout);
            }
            return GetPagedAsyncImpl<T>(connection, transaction, commandTimeout, sql, type);
        }

        private static async Task<IEnumerable<T>> GetPagedAsyncImpl<T>(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, string sql, Type type) where T : class
        {
            var result = await connection.QueryAsync(sql).ConfigureAwait(false);
            var list = new List<T>();
            foreach (IDictionary<string, object> res in result)
            {
                var obj = ProxyGenerator.GetInterfaceProxy<T>();
                foreach (var property in TypePropertiesCache(type))
                {
                    var val = res[property.Name];
                    if (val == null) continue;
                    if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        var genericType = Nullable.GetUnderlyingType(property.PropertyType);
                        if (genericType != null) property.SetValue(obj, Convert.ChangeType(val, genericType), null);
                    }
                    else
                    {
                        property.SetValue(obj, Convert.ChangeType(val, property.PropertyType), null);
                    }
                }
                ((IProxy)obj).IsDirty = false;   //reset change tracking and return
                list.Add(obj);
            }
            return list;
        }

    }
}