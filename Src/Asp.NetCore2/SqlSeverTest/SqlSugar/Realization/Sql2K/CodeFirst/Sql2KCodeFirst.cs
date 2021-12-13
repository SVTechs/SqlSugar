using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlSugar
{
    public class Sql2KCodeFirst:CodeFirstProvider
    {
        protected override string GetTableName(EntityInfo entityInfo)
        {
            var table= this.Context.EntityMaintenance.GetTableName(entityInfo.EntityName);
            var tableArray = table.Split('.');
            var noFormat = table.Split(']').Length==1;
            if (tableArray.Length > 1 && noFormat)
            {
                return tableArray.Last();
            }
            else
            {
                return table;
            }
        }
    }
}
