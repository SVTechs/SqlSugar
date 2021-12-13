using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlSugar
{
    public class Sql2KQueryBuilder: QueryBuilder
    {
        public override string SqlTemplate
        {
            get
            {
                return " SELECT [TOP] {0} FROM {1} where 1=1 {" + UtilConstants.ReplaceKey + "} {2} {3} {4} ";
            }
        }

        public override string ToSqlString()
        {
            string oldOrderBy = this.OrderByValue;
            string externalOrderBy = oldOrderBy;
            var isIgnoreOrderBy = this.IsCount && this.PartitionByValue.IsNullOrEmpty();
            AppendFilter();
            sql = new StringBuilder();
            if (this.OrderByValue == null && (Skip != null || Take != null)) this.OrderByValue = " ORDER BY GetDate() ";
            if (this.PartitionByValue.HasValue())
            {
                this.OrderByValue = this.PartitionByValue + this.OrderByValue;
            }
            var pkColumn = GetPk();
            if (string.IsNullOrEmpty(pkColumn))
            {
                throw new Exception("No PK Column");
            }
            var tableName = GetTableNameString;
            var isFirst = (Skip == 0 || Skip == null) && Take == 1 && DisableTop == false;
            var isRowNumber = (Skip != null || Take != null) && !isFirst;
            var rowNumberString = $" and ({pkColumn} not in (select top {Skip} id from {tableName} {GetWhereValueString} {GetOrderByString})) ";
            string groupByValue = GetGroupByString + HavingInfos;
            string orderByValue = (!isRowNumber && this.OrderByValue.HasValue()) ? GetOrderByString : null;
            if (isIgnoreOrderBy) { orderByValue = null; }
            sql.AppendFormat(SqlTemplate, isFirst ? (" TOP 1 " + GetSelectValue) : GetSelectValue, tableName, GetWhereValueString.Replace("where", "and", StringComparison.InvariantCultureIgnoreCase), groupByValue, orderByValue);
            sql.Replace("[TOP]", isIgnoreOrderBy || isFirst ? "" : (Take == null ? "" : $" TOP {Take} ") );
            string rowNumberText = isRowNumber ? (isIgnoreOrderBy ? null : rowNumberString) : null;
            sql.Replace(UtilConstants.ReplaceKey, rowNumberText);
            if (isIgnoreOrderBy) { this.OrderByValue = oldOrderBy; return sql.ToString(); }
            var result = sql.ToString();
            /*
            var result = isFirst ? sql.ToString() : ToPageSql(sql.ToString(), this.Take, this.Skip);
            if (ExternalPageIndex > 0)
            {
                if (externalOrderBy.IsNullOrEmpty())
                {
                    externalOrderBy = " ORDER BY GetDate() ";
                }
                result = string.Format("SELECT *,ROW_NUMBER() OVER({0}) AS RowIndex2 FROM ({1}) ExternalTable ", GetExternalOrderBy(externalOrderBy), result);
                result = ToPageSql2(result, ExternalPageIndex, ExternalPageSize, true);
            }
            */
            this.OrderByValue = oldOrderBy;
            if (!string.IsNullOrEmpty(this.Offset))
            {
                if (this.OrderByValue.IsNullOrEmpty())
                {
                    result += " ORDER BY GETDATE() ";
                }
                result += this.Offset;
            }
            result = GetSqlQuerySql(result);
            return result;
        }
    }
}
