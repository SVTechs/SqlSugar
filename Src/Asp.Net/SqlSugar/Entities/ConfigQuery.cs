﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
namespace SqlSugar 
{
    public class SqlConfigTable
    {
        public SqlSugarProvider Context { get; set; }
        public void SetKeyValue<T>(Expression<Func<T, object>> keyExpression, Expression<Func<T, object>> valueExpression, string uniqueCode = null, Expression<Func<T, object>> whereExpression=null) 
        {
            var entity = this.Context.EntityMaintenance.GetEntityInfo<T>();
            ExpressionContext context = new ExpressionContext();
            var query = Context.Queryable<T>().QueryBuilder;
            var keyValue= query.GetExpressionValue(keyExpression, ResolveExpressType.FieldSingle).GetString();
            var ValueValue = query.GetExpressionValue(valueExpression, ResolveExpressType.FieldSingle).GetString();
            string where = null;
            if (whereExpression != null) 
            {
                where=query.GetExpressionValue(whereExpression, ResolveExpressType.WhereSingle).GetResultString();
            }
            context.MappingTables = this.Context.MappingTables;
            if (!SqlFuncExtendsion.TableInfos.Any(y => y.Type == typeof(T) && y.Code == uniqueCode))
            {
                SqlFuncExtendsion.TableInfos.Add(new ConfigTableInfo()
                {
                    Type = typeof(T),
                    TableName = entity.DbTableName,
                    Key = keyValue,
                    Value = ValueValue,
                    Where = where,
                    Parameter = query.Parameters,
                    Code = uniqueCode
                });
            }
            else
            {
                Check.Exception(true, "SetKeyValue error , entity & uniqueCode already exist");
            }
        }
        public void SetKeyValue<T>(Expression<Func<T, object>> key, Expression<Func<T, object>> value)
        {
            SetKeyValue<T>(key,value, null,null);
        }
    }

    public class ConfigTableInfo 
    { 
        public string Code { get; set; }
        public Type Type { get; set; }
        public string TableName { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
        public string Where { get; set; }
        public List<SugarParameter> Parameter { get; set; }
    }
}