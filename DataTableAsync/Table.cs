using System;
using System.Data;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Drawing;
using System.Threading.Tasks;
using System.Globalization;
using static DataTableAsync.Table;
using System.Diagnostics;

namespace DataTableAsync
{
    // add sorting columns | rows
    // events

    public class Table
    {
        private readonly static CultureInfo enUS = new CultureInfo("en-US", false);
        private const string dateFormat = "yyyy-MM-dd HH:mm:ss.fff";
        internal readonly static string[] dateFormats = new string[] { "MM/dd/yyyy", "MM/dd/yy" };
        internal readonly static NumberStyles numberStyles = NumberStyles.AllowThousands | NumberStyles.AllowDecimalPoint | NumberStyles.AllowCurrencySymbol | NumberStyles.AllowParentheses | NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite | NumberStyles.AllowLeadingSign;

        #region" ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ E V E N T S "
        protected virtual void OnTableCleared(EventType tableEvent) => TableCleared?.Invoke(this, new TableEventArgs(tableEvent));
        public event EventHandler<TableEventArgs> TableCleared;
        protected virtual void OnColumnsChanged(EventType columnEvent, Column column) => ColumnsChanged?.Invoke(this, new TableEventArgs(columnEvent, column));
        public event EventHandler<TableEventArgs> ColumnsChanged;

        protected virtual void OnRowsChanged(EventType rowEvent, Row row) => RowsChanged?.Invoke(this, new TableEventArgs(rowEvent, row));
        public event EventHandler<TableEventArgs> RowsChanged;
        protected virtual void OnColumnCastFailed(Column column, Type toType) => ColumnCastFailed?.Invoke(this, new TableEventArgs(column, toType));
        public event EventHandler<TableEventArgs> ColumnCastFailed;
        protected virtual void OnCellChanged(Row row, Column column, object oldValue, object newValue) => CellChanged?.Invoke(this, new TableEventArgs(row, column, oldValue, newValue));
        public event EventHandler<TableEventArgs> CellChanged;
        #endregion
        //[JsonConverter(typeof(CustomConverter))]
        public string Name { get; set; }
        [JsonIgnore]
        public object Tag { get; set; }
        //[JsonConverter(typeof(CustomConverter))]
        public virtual ColumnCollection<string, Column> Columns { get; }
        //[JsonConverter(typeof(CustomConverter))]
        public virtual RowCollection<int, Row> Rows { get; }
        public Table() { Columns = new ColumnCollection<string, Column>(this); Rows = new RowCollection<int, Row>(this); }
        public Table(Tuple<Dictionary<byte, string>, Dictionary<int, Dictionary<string, string>>> Db2Out)
        {
            if (Db2Out != null && Db2Out.Item1.Any())
            {
                Columns = new ColumnCollection<string, Column>(this);
                Rows = new RowCollection<int, Row>(this);

                Dictionary<byte, string> cols = Db2Out.Item1;
                Dictionary<int, Dictionary<string, string>> rows = Db2Out.Item2;
                var values = cols.ToDictionary(k => k.Value, v => new List<Type>());
                foreach (var row in rows.Values)
                    foreach (var col in cols.Values)
                    {
                        var stringValue = row[col];
                        var valueType = SurroundClass.GetDataType(stringValue).Item1;
                        values[col].Add(valueType);
                    }
                foreach (var colType in values)
                    Columns.Add(colType.Key, SurroundClass.GetDataType(colType.Value));
                foreach (var row in rows.OrderBy(r => r.Key))
                    Rows.Add(row.Value.Values);
            }
        }
        public Table(string filepath)
        {
            Columns = new ColumnCollection<string, Column>(this); Rows = new RowCollection<int, Row>(this);
            if (File.Exists(filepath ?? string.Empty))
                Json_toTable(filepath);
            else
            {   
            } // html?
        }
        public Table(FileInfo jsonFile)
        {
            Columns = new ColumnCollection<string, Column>(this); Rows = new RowCollection<int, Row>(this);
            if (jsonFile.Exists) { Json_toTable(jsonFile.FullName); }
        }
        public Table(DataTable sourceTable, string primaryKey = null)
        {
            // the primarykey may have to be fed if the datatable is coming from a Db2 query
            Columns = new ColumnCollection<string, Column>(this); Rows = new RowCollection<int, Row>(this);
            DataTable_toTable(sourceTable, primaryKey);
        }
        public Table(IEnumerable dataSource)
        {
            Columns = new ColumnCollection<string, Column>(this); Rows = new RowCollection<int, Row>(this);
            Type tableType = dataSource.GetType();
            List<DataRow> datarows = new List<DataRow>(dataSource.OfType<DataRow>());
            if (datarows.Any()) DataTable_toTable(datarows.CopyToDataTable());
            else if (tableType == typeof(Table))
            {
                Table sourceTable = (Table)dataSource;
                foreach (Column column in sourceTable.Columns.Values) Columns.Add(column.Name, column.DataType);
                foreach (Row row in sourceTable.Rows.Values) Rows.Add(row);
                PrimaryKeys = sourceTable.PrimaryKeys;
            }
            else
            {
                List<Row> sourceRows = new List<Row>(dataSource.OfType<Row>());
                if (sourceRows.Any())
                {
                    List<Column> sourceColumns = new List<Column>(sourceRows.First().Table.Columns.Values);
                    foreach (Column column in sourceColumns) Columns.Add(column.Name, column.DataType);
                    foreach (Row row in sourceRows.OrderBy(r => r.Index)) Rows.Add(row);
                    if (Columns.Any()) PrimaryKeys = Columns.First().Value.Table?.PrimaryKeys;
                }
            }
        }
        public Table(IEnumerable<Row> dataSource)
        {
            Columns = new ColumnCollection<string, Column>(this); Rows = new RowCollection<int, Row>(this);
            List<Row> datarows = new List<Row>(dataSource.OfType<Row>());
            if (datarows.Any()) {
                List<Column> sourceColumns = new List<Column>(datarows.First().Table.Columns.Values);
                foreach (Column column in sourceColumns) Columns.Add(column.Name, column.DataType);
                foreach (Row row in datarows.OrderBy(r => r.Index)) Rows.Add(row);
                if (Columns.Any()) PrimaryKeys = Columns.First().Value.Table?.PrimaryKeys;
            }
        }
        private void DataTable_toTable(DataTable sourceTable, string primaryKey = null)
        {
            if (sourceTable != null)
            {
                primaryKey = primaryKey ?? string.Empty;
                List<Column> newKeys = new List<Column>();
                Dictionary<DataColumn, Column> cols = new Dictionary<DataColumn, Column>();
                foreach (DataColumn column in sourceTable.Columns)
                {
                    Column newColumn = new Column(column.ColumnName, column.DataType);
                    Columns.Add(column.ColumnName, newColumn);
                    cols.Add(column, newColumn);
                    if (sourceTable.PrimaryKey.Contains(column) | column.ColumnName.ToLowerInvariant() == primaryKey)
                        newKeys.Add(newColumn);
                }
                PrimaryKeys = newKeys.ToArray();
                int rowIndex = 0;
                foreach (DataRow row in sourceTable.AsEnumerable()) Rows.Add(rowIndex++, new Row(row.ItemArray, this));
            }
        }
        private void Json_toTable(string fullpath)
        {
            if (File.Exists(fullpath ?? string.Empty))
            {
                try
                {
                    Table fileTable = JsonConvert.DeserializeObject<Table>(File.ReadAllText(fullpath));
                    Name = fileTable.Name;
                    foreach (var column in fileTable.Columns) { Columns.Add(column.Key, column.Value); }
                    foreach (var row in fileTable.Rows) { Rows.Add(row.Key, row.Value); }
                }
                catch (JsonException je) { Console.WriteLine(je.Message); }
            }
            else { }
        }
        public void Clear() {
            Columns.Clear();
            Rows.Clear();
            OnTableCleared(EventType.TableCleared);
        }
        public Table Copy()
        {
            Table replica = Clone();
            foreach (Row row in Rows.Values)
                replica.Rows.Add(row.Values);
            return replica;
        }
        public Table Clone()
        {
            string tableName = Name;
            Table replica = new Table() { Name = tableName };
            List<Column> primarykeys = new List<Column>();
            foreach (Column column in Columns.Values)
            {
                Column replicaColumn = new Column() { Name = column.Name, DataType = column.DataType, DefaultValue = column.DefaultValue };
                replica.Columns.Add(replicaColumn);
                if (column.IsKey)
                    primarykeys.Add(replicaColumn);
            }
            replica.PrimaryKeys = primarykeys.ToArray();
            return replica;
        }
        public void Merge(Table mergeTable)
        {
            if (mergeTable != null)
            {
                if (!Columns.Any()) foreach (Column column in mergeTable.Columns.Values.OrderBy(c => c.Index))
                        Columns.Add(new Column(column.Name, column.DataType) { DefaultValue = column.DefaultValue });
                if (mergeTable.PrimaryKeys.Any()) PrimaryKeys = mergeTable.PrimaryKeys;
                // mergeTable.Columns count>=this.Columns.Count
                int Columns_sameNameCount = 0;
                foreach (Column column in mergeTable.Columns.Values)
                    if (Columns.ContainsKey(column.Name)) Columns_sameNameCount++;

                bool addByName = Columns_sameNameCount >= Columns.Count;
                bool addByIndex = !addByName;
                if (addByName)
                {
                    foreach (Row mergeRow in mergeTable.Rows.Values)
                    {
                        var rowValues = Columns.ToDictionary(k => k.Key, v => v.Value.DefaultValue, StringComparer.OrdinalIgnoreCase);
                        foreach (var mergeCell in mergeRow.Cells) { if (rowValues.ContainsKey(mergeCell.Key)) { rowValues[mergeCell.Key] = mergeCell.Value; } }
                        Rows.Add(new Row(rowValues.Values, this));
                    }
                }
                else if (addByIndex)
                    foreach (Row mergeRow in mergeTable.Rows.Values) Rows.Add(new Row(mergeRow.Cells.Values, this));
            }
        }
        public void Merge(DataTable mergeTable)
        {
            if (mergeTable != null)
            {
                if (!Columns.Any()) foreach (DataColumn column in mergeTable.Columns)
                        Columns.Add(new Column(column.ColumnName, column.DataType));

                // mergeTable.Columns count>=this.Columns.Count
                // , mergeTable.PrimaryKey.Contains(column)

                int Columns_sameNameCount = 0;
                foreach (DataColumn column in mergeTable.Columns)
                    if (Columns.ContainsKey(column.ColumnName)) Columns_sameNameCount++;

                bool addByName = Columns_sameNameCount >= Columns.Count;
                bool addByIndex = !addByName;
                if (addByName)
                {
                    foreach (DataRow mergeRow in mergeTable.AsEnumerable())
                    {
                        var rowValues = Columns.ToDictionary(k => k.Key, v => v.Value.DefaultValue, StringComparer.OrdinalIgnoreCase);
                        foreach (DataColumn column in mergeTable.Columns) { if (rowValues.ContainsKey(column.ColumnName)) { rowValues[column.ColumnName] = mergeRow[column.ColumnName]; } }
                        Rows.Add(new Row(rowValues.Values));
                    }
                }
                else if (addByIndex)
                    foreach (DataRow mergeRow in mergeTable.AsEnumerable()) Rows.Add(new Row(mergeRow.ItemArray));
            }
        }
        public DataTable CopyToDataTable()
        {
            DataTable copyTable = new DataTable() { TableName = Name };
            var primaryKeys = new List<Tuple<Column, DataColumn>>();
            foreach (Column column in Columns.Values)
            {
                var newDataCol = new DataColumn(column.Name, column.DataType) { DefaultValue = column.DefaultValue };
                copyTable.Columns.Add(newDataCol);
                if (column.IsKey)
                    primaryKeys.Add(Tuple.Create(column, newDataCol));
            }
            foreach (Row row in Rows.Values)
            {
                var itemArray = new List<object>();
                foreach (var cell in row.Cells)
                {
                    try {
                        var castCol = Columns[cell.Key];
                        var colType = castCol.DataType;
                        var castValue = SurroundClass.ChangeType(cell.Value, colType);
                        itemArray.Add(castValue);
                    }
                    catch {
                        itemArray.Add(null);
                    }
                }
                try { copyTable.Rows.Add(itemArray.ToArray()); }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                    //Debugger.Break();
                }
            }
            copyTable.PrimaryKey = primaryKeys.OrderBy(pk => pk.Item1.KeyIndex).Select(pk => pk.Item2).ToArray();
            return copyTable;
        }
        [JsonIgnore]
        public List<Row> AsEnumerable => new List<Row>(Rows.Values);
        [JsonIgnore]
        public string HTML
        {
            get
            {
                var table = this;
                if (table == null) { return null; }
                else
                {
                    Color hdrBackColor = Color.FromArgb(103, 71, 205);
                    Color hdrForeColor = Color.White;
                    Color rowBackColor = Color.Gainsboro;
                    Color rowForeColor = Color.Black;
                    Font tableFont = new Font("IBM Plex Mono", 9);

                    Font headerFont = new Font(tableFont.FontFamily.Name, tableFont.Size + 1);
                    Dictionary<Row, List<string>> Rows = new Dictionary<Row, List<string>>();
                    Dictionary<string, int> columnWidths = new Dictionary<string, int>();
                    Dictionary<string, string> colAligns = new Dictionary<string, string>();

                    #region" align preferences "
                    List<Type> lefts = new List<Type> { typeof(string) };
                    List<Type> centers = new List<Type> { typeof(bool), typeof(byte), typeof(short), typeof(int), typeof(long), typeof(DateTime), typeof(Icon), typeof(Image) };
                    List<Type> rights = new List<Type> { typeof(double), typeof(decimal) };
                    #endregion

                    var rowStrings = Row_strings();

                    using (Bitmap bmp = new Bitmap(50, 50))
                    {
                        using (Graphics g = Graphics.FromImage(bmp))
                        {
                            var colIndx = 1;
                            var hdrNames = rowStrings[-2];
                            foreach (var col in Columns.Values) {
                                var colName = hdrNames[col.Name];
                                columnWidths[col.Name] = 18 + Convert.ToInt32(g.MeasureString(new string('X', colName.Length), headerFont).Width);
                                var lftRghtCtr = lefts.Contains(col.DataType) ? "left" : rights.Contains(col.DataType) ? "right" : "center";
                                var colAlign = $"tr td:nth-child({colIndx})" + " {text-align: " + lftRghtCtr + ";}";
                                colAligns.Add(col.Name, colAlign);
                                colIndx++;
                            }
                        }
                    }

                    List<string> Top = new List<string>();
                    string hexHdrBackColor = string.Format(CultureInfo.InvariantCulture, "#{0:X2}{1:X2}{2:X2}", hdrBackColor.R, hdrBackColor.G, hdrBackColor.B);
                    string hexHdrForeColor = string.Format(CultureInfo.InvariantCulture, "#{0:X2}{1:X2}{2:X2}", hdrForeColor.R, hdrForeColor.G, hdrForeColor.B);
                    string hexRowBackColor = string.Format(CultureInfo.InvariantCulture, "#{0:X2}{1:X2}{2:X2}", rowBackColor.R, rowBackColor.G, rowBackColor.B);
                    string hexRowForeColor = string.Format(CultureInfo.InvariantCulture, "#{0:X2}{1:X2}{2:X2}", rowForeColor.R, rowForeColor.G, rowForeColor.B);
                    byte headSz = 15; //$"{Math.Round(headerFont.Size / (double)10, 1):00.0}";
                    byte rowSz = 13; //$"{Math.Round(tableFont.Size / (double)10, 1):00.0}";
                    Top.Clear();
                    Top.Add("<!DOCTYPE html>");
                    Top.Add("<html>");
                    Top.Add("<head>");
                    Top.Add("<style>");
                    Top.Add("table {border-collapse:collapse; border: 1px solid #778db3;}"); // width: 100%;
                    Top.Add("th {" + $"font-family:{headerFont.FontFamily.Name}; background-color:{hexHdrBackColor}; color:{hexHdrForeColor}; text-align:center; font-weight:bold; font-size:{headSz}px; border: 1px solid #778db3; white-space: nowrap;" + "}");
                    Top.Add("td {" + $"font-family:{tableFont.FontFamily.Name}; text-align:left; font-size:{rowSz}px; border: 1px #696969; white-space: nowrap;" + "}");
                    Top.Add(string.Join(Environment.NewLine, colAligns.Values));
                    Top.Add("</style>");
                    Top.Add("</head>");
                    Top.Add("<body>");
                    Top.Add("<table>");
                    Top.Add("<tr>" + string.Join("", from C in columnWidths select "<th width=" + C.Value + ";>" + C.Key + "</th>") + "</tr>");
                    List<string> Middle = new List<string>();
                    // "#F5F5F5" : "#FFFFFF"
                    foreach (var row in rowStrings.Values.Skip(2))
                    {
                        var tds = string.Join("", row.Values.Skip(1).Select(td => $"<td>{td.Trim()}</td>"));
                        Middle.Add("<tr style=background-color:" + (Middle.Count % 2 == 0 ? hexRowBackColor : "#FFFFFF").ToString() + $"; color:{hexRowForeColor}>" + tds + "</tr>");
                    }                        
                    List<string> Bottom = new List<string>()
        {
            "</table>",
            "</body>",
            "</html>"
        };
                    List<string> All = new List<string>();
                    All.AddRange(Top);
                    All.AddRange(Middle);
                    All.AddRange(Bottom);
                    return string.Join(Environment.NewLine, All);
                }
            }
        }
        [JsonIgnore]
        public string Lines
        {
            get
            {
                //row\Col1 ┼ Col2 ┼ Col3 ┼ Col4 ┼ Col5 ┼ Col6 ┼ Col7 ┼ Col8 ┼ Col9
                //0        │x     │y     │z     │      │      │      │      │       │
                //1        │x     │y     │z     │      │      │      │      │       │
                //2        │x     │y     │z     │      │      │      │      │       │
                //3        │x     │y     │z     │      │      │      │      │       │
                //4        │x     │y     │z     │      │      │      │      │       │
                //5        │x     │y     │z     │      │      │      │      │       │
                //6        │x     │y     │z     │      │      │      │      │       │
                //7        │x     │y     │z     │      │      │      │      │       │
                var rowStrings = Row_strings();
                return string.Join(Environment.NewLine, from row in rowStrings select string.Join(row.Key == -1 ? "┼" : "│", from c in row.Value select c.Value));
            }
        }
        public Dictionary<int, Dictionary<string, string>> Row_strings()
        {
            // the pad right function transforms a string to the length of the int paranmter and only pads right IF the string.length is LOWER than the parameter
            var rowIndxColWidth = new int[] { Rows.Count >= 1000 ? Rows.Count - 1 : 3 }.Max();
            var colStrs = new Dictionary<string, Dictionary<int, string>>();
            var rowStrs = new Dictionary<int, Dictionary<string, string>>();
            var colWdths = new Dictionary<string, int>();
            rowStrs[-2] = new Dictionary<string, string> { { "row", "row".PadRight(rowIndxColWidth) } }; // headers = -2
            rowStrs[-1] = new Dictionary<string, string> { { "row", new string('─', 3) } }; // separating line = -1

            // iterate first to collect the widths of each column
            foreach (var col in Columns.Values)
            {
                colStrs[col.Name] = new Dictionary<int, string>();
                colWdths[col.Name] = col.Name.Length;
                var objFormat = ObjectFormat(col.Name);
                foreach (var row in Rows)
                {
                    object cellValue = row.Value[col.Name];
                    string cellString = string.Empty;
                    if (!(cellValue == null | cellValue == DBNull.Value))
                    {
                        if (col.DataType == typeof(DateTime))
                        {
                            cellString = ((DateTime)cellValue).ToString(objFormat);
                        }
                        else if (col.DataType == typeof(DateTime[]) | col.DataType == typeof(List<DateTime>) | col.DataType == typeof(IEnumerable<DateTime>))
                        {
                            var cellStrings = new List<string>();
                            foreach (var date in (IEnumerable<DateTime>)cellValue)
                                cellStrings.Add(date.ToString(objFormat));
                            cellString = string.Join(" ▪ ", cellStrings);
                        }
                        else if (col.DataType == typeof(float) | col.DataType == typeof(decimal) | col.DataType == typeof(double))
                        {
                            var cellValStr = cellValue.ToString();
                            //var isNeg = cellValStr.ToLower().Contains("cr");
                            //cellValStr = Regex.Replace(cellValStr, "[a-z][A-Z]{1,}", string.Empty);
                            decimal.TryParse(cellValStr, numberStyles, enUS, out decimal cellVal);
                            //if (isNeg)
                            //    cellVal = -1 * cellVal;
                            cellString = cellVal.ToString(objFormat);
                        }
                        else
                        {
                            cellString = cellValue.ToString();
                        }
                    }
                    colStrs[col.Name][row.Key] = cellString;
                    if (cellString.Length > colWdths[col.Name])
                        colWdths[col.Name] = cellString.Length;
                }
                rowStrs[-2][col.Name] = col.Name.PadRight(new int[] { colWdths[col.Name], col.Name.Length }.Max());
                rowStrs[-1][col.Name] = new string('─', rowStrs[-2][col.Name].Length);
            }

            // now iterate to create a list of the rows
            foreach (var row in Rows) {
                rowStrs[row.Key] = new Dictionary<string, string> { { "row", row.Key.ToString().PadRight(rowIndxColWidth) } };
                var rowArray = new List<string> { row.Key.ToString().PadRight(rowIndxColWidth) };
                foreach (var col in Columns)
                {
                    string cellString = colStrs[col.Key][row.Key];
                    cellString = cellString.PadRight(colWdths[col.Key]);
                    rowArray.Add(cellString);
                    rowStrs[row.Key][col.Key] = cellString;
                }
            }

            // center column names
            foreach (var col in rowStrs[-2].ToArray())
            {
                /// |xx        |   10 wide, but name is 2 wide, padded 8
                /// divide extra / 2
                var padLen = col.Value.Length - col.Key.Length;
                if (padLen > 1)
                {
                    var padLftRght = padLen / 2;
                    rowStrs[-2][col.Key] = $"{new string(' ', padLftRght)}{col.Key}{new string(' ', padLen - padLftRght)}";
                }
            }
            return rowStrs;
        }
        public Dictionary<string, Dictionary<int, string>> Col_strings() {
            var rowStrs = Row_strings();
            var colStrs = new Dictionary<string, Dictionary<int, string>>();
            foreach (var rw in rowStrs.Skip(2))
            {
                foreach (var col in rw.Value)
                {
                    if (!colStrs.ContainsKey(col.Key))
                        colStrs[col.Key] = new Dictionary<int, string>();
                    colStrs[col.Key][rw.Key] = col.Value;
                }
            }
            return colStrs;
        }
        private string DateFormat(List<DateTime> dates)
        {
            bool allMidnight = true;
            foreach (var date in dates)
            {
                if (date.TimeOfDay.Ticks != 0)
                {
                    allMidnight = false;
                    break;
                }
            }
            return allMidnight ? dateFormat.Split(' ')[0] : dateFormat;
        }
        private string ObjectFormat(string colName) {
            
            var format = string.Empty;
            if (Columns.ContainsKey(colName))
            {
                var col = Columns[colName];
                var colValues = col.Values.Values.Where(d => !(d == null | d == DBNull.Value));
                if (col.DataType == typeof(DateTime))
                {
                    var dates = colValues.Select(d => (DateTime)d).ToList();
                    format = DateFormat(dates);
                }
                else if (col.DataType == typeof(DateTime[]) | col.DataType == typeof(List<DateTime>) | col.DataType == typeof(IEnumerable<DateTime>))
                {
                    bool allMidnight = true;
                    foreach (var dateArray in colValues)
                    {
                        var dates = new List<DateTime>((IEnumerable<DateTime>)dateArray); // DateTime[] | List<DateTime> can be cast as IEnumerable<DateTime>
                        format = DateFormat(dates);
                        if (format == dateFormat) {
                            allMidnight = false;
                            break;
                        } // if equals long date format
                    }
                    format = allMidnight ? dateFormat.Split(' ')[0] : dateFormat;
                }
                else if (col.DataType == typeof(float) | col.DataType == typeof(decimal) | col.DataType == typeof(double))
                {
                    var decPlaces = 0;
                    foreach (var val in colValues) {
                        var dec = val.ToString().Split('.');
                        if (dec.Length > 1 && dec[1].Length > decPlaces)
                            decPlaces = dec[1].Length;
                    }
                    format = decPlaces == 0 ? "" : $"N{decPlaces}";
                }
                return format;
            }
            else
                return null;
        }
        [JsonIgnore]
        public string Json
        {
            get
            {
                try { return JsonConvert.SerializeObject(this, Formatting.None) ?? string.Empty; }
                catch (JsonSerializationException jse)
                {
                    return jse.Message;
                }
            }
        }
        [JsonIgnore]
        public Column[] PrimaryKeys
        {
            get => primaryKeys.OrderBy(pk => pk.Key).Select(pk => pk.Value).ToArray();
            set
            {
                primaryKeys.Clear();
                foreach (Column pk in value)
                    primaryKeys.Add((byte)primaryKeys.Count, pk);
                // setting keys AFTER rows are loaded
                foreach (var row in Rows)
                    AddKeys(row);
            }
        }
        [JsonIgnore]
        public Dictionary<dynamic, dynamic> Keys { get; } = new Dictionary<dynamic, dynamic>();
        internal Dictionary<byte, Column> primaryKeys = new Dictionary<byte, Column>();
        public Row FindRow(object findValue) => FindRow(new object[] { findValue });
        public Row FindRow(object[] findValues)
        {
            if (findValues != null && findValues.Length == PrimaryKeys.Length)
            {
                Dictionary<dynamic, dynamic> tempDict = Keys;
                byte keyIndex = 0;
                int rowIndex = -1;
                try
                {
                    foreach (var findValue in findValues)
                    {
                        var castValue = SurroundClass.ChangeType(findValue, primaryKeys[keyIndex++].DataType);
                        if (castValue.GetType() == typeof(string))
                        {
                            var caseDict = tempDict.ToDictionary(k => (string)k.Key, v => v.Value, StringComparer.OrdinalIgnoreCase);
                            if (keyIndex == PrimaryKeys.Length)
                                rowIndex = caseDict[(string)castValue];
                            else
                                tempDict = caseDict[(string)castValue];
                        }
                        else
                        {
                            if (keyIndex == PrimaryKeys.Length)
                                rowIndex = tempDict[castValue];
                            else
                                tempDict = tempDict[castValue];
                        }
                    }
                    return rowIndex == -1 ? null : Rows[rowIndex];
                }
                catch { return null; }
            }
            else
                return null;
        }
        public Row NewRow() {
            object nullObj = null;
            var nulls = Enumerable.Range(0, Columns.Count).Select(n => nullObj);
            var newRow = new Row(nulls, this);
            return newRow;
        }
        internal void AddKeys(KeyValuePair<int, Row> row)
        {
            var tempDict = Keys;
            foreach (Column col in PrimaryKeys)
            {
                var castValue = SurroundClass.ChangeType(row.Value[col.Name], col.DataType);
                if (castValue == null || castValue == DBNull.Value)
                    throw new Exception($"Primary keys can not have a null value - column -> {col.Name}");
                if (!tempDict.ContainsKey(castValue))
                {
                    tempDict[castValue] = new Dictionary<dynamic, dynamic>();
                    //Debugger.Break();
                }
                if (col.KeyIndex == (PrimaryKeys.Count() - 1))
                {
                    tempDict[castValue] = row.Key;
                    //Debugger.Break();
                }
                else {
                    tempDict = tempDict[castValue];
                    //Debugger.Break();
                }
            }
            //Debugger.Break();
        }

        #region" ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ N E S T E D   C L A S S E S [ COLUMNS|ROWS ] "
        [Serializable]
        public sealed class ColumnCollection<TKey, TValue> : Dictionary<TKey, TValue>
        {
            [JsonIgnore]
            public Table Table { get; }
            public ColumnCollection(Table parent) : base(GetComparer()) => Table = parent;
            private static IEqualityComparer<TKey> GetComparer() => (IEqualityComparer<TKey>)StringComparer.InvariantCultureIgnoreCase;
            public new TValue Add(TKey key, TValue value)
            {
                base.Add(key, value);
                Column addColumn = Table.Columns[key.ToString()];
                addColumn.Index = Table.Columns.Count - 1;
                addColumn.parent = Table;
                foreach (Row row in Table.Rows.Values) { row.Cells.Add(addColumn.Name, addColumn.DefaultValue); }
                Table.OnColumnsChanged(EventType.ColumnAdd, addColumn);
                return value;
            }
            // below statics must use the above base method <string, Column> otherwise recursion from calling one of my statics: public Column Add
            public Column Add(Column addColumn)
            {
                Table.Columns.Add(addColumn.Name, addColumn);
                return Table.Columns[addColumn.Name];
            }
            public Column Add(string columnName, Type columnType)
            {
                var addColumn = new Column(columnName, columnType);
                Table.Columns.Add(columnName, addColumn);
                return Table.Columns[addColumn.Name];
            }
            public Column Add(string columnName, Type columnType, object defaultValue)
            {
                var addColumn = new Column(columnName, columnType) { DefaultValue = defaultValue };
                Table.Columns.Add(columnName, addColumn);
                return Table.Columns[addColumn.Name];
            }
            public List<Column> AddRange(IEnumerable<Column> columns)
            {
                foreach (Column col in columns)
                    Add(col);
                return new List<Column>(columns);
            }
            public new TKey Remove(TKey key)
            {
                if (key != null)
                {
                    if (ContainsKey(key))
                    {
                        string columnName = key.ToString();
                        Column removeColumn = Table.Columns[columnName];
                        var colIndex = removeColumn.Index;
                        var colsToRight = new List<Column>(Table.Columns.Values.Where(c => c.Index > colIndex).OrderByDescending(c => c.Index));
                        if (removeColumn.IsKey)
                        {
                            // reindex the primary keys without the removeColumn
                            Dictionary<byte, Column> newKeys = new Dictionary<byte, Column>();
                            foreach (var col in Table.primaryKeys.OrderBy(c => c.Key))
                                if (col.Value != removeColumn)
                                    newKeys.Add((byte)newKeys.Count, col.Value);
                            Table.primaryKeys = newKeys;
                        }
                        base.Remove(key);
                        var colDict = Table.Columns.Values.Select((col, index) => new { col, index }).ToDictionary(k => k.col.Name, v => v.index);
                        foreach (Row row in Table.AsEnumerable)
                            Table.Rows[row.Index].Cells.Remove(key.ToString());
                        foreach (var colToRight in colsToRight) {
                            colToRight.Index = colDict[colToRight.Name];
                        }
                        //Debugger.Break();
                        Table.OnColumnsChanged(EventType.ColumnRemove, removeColumn);
                    }
                }
                return key;
            }
            public new void Clear()
            {
                Table.primaryKeys.Clear();
                foreach (Row row in Table.Rows.Values)
                    row.Cells.Clear();
                base.Clear();
                Table.OnColumnsChanged(EventType.ColumnsCleared, null);
            }
            public new TValue this[TKey key]
            {
                get
                {
                    if (ContainsKey(key)) return base[key];
                    else
                        throw new Exception($"{key} key not found");
                }
                set { base[key] = value; }
            }
            public TValue this[int index]
            {
                get
                {
                    var cols = Keys.ToDictionary(k => Table.Columns[k.ToString()].Index, v => this[v]);
                    return cols.ContainsKey(index) ? cols[index] : default;
                }
            }
        }
        public class Column
        {
            [JsonIgnore]
            public Table Table { get { return parent; } }
            internal Table parent = null;
            public Type DataType
            {
                get { return datatype; }
                set
                {
                    if (datatype != value)
                    {
                        if (Table == null) { datatype = value; }
                        else
                        {
                            // update keys/primarykey (not done)
                            if (value == typeof(string))
                            {
                                foreach (Row row in Table.Rows.Values) { row.Cells[Name] = row.Cells[Name].ToString(); }
                            }
                            else
                            {
                                bool cantParseAllValues = false;
                                foreach (Row row in Table.Rows.Values) { if (!SurroundClass.Cast(value, row.Cells[Name])) { cantParseAllValues = true; break; } }
                                if (!cantParseAllValues) { datatype = value; }
                            }
                        }
                    }
                }
            }
            private Type datatype;
            public object DefaultValue
            {
                get { return defaultValue; }
                set
                {
                    if (defaultValue != value)
                    {
                        if (SurroundClass.Cast(DataType, value))
                        {
                            defaultValue = value;
                            AwaitTask();
                        }
                    }
                }
            }
            private object defaultValue = null;
            private async void AwaitTask()
            {
                while (Table == null) { await Task.Delay(50); }
                foreach (Row row in Table.Rows.Values.Where(r => r.Cells[Name] == null))
                {
                    row.Cells[Name] = defaultValue;
                    Table.OnCellChanged(row, this, row.Cells[Name], defaultValue);
                }
            }
            public string Name { get; set; }
            public int Index
            {
                get => index;
                set
                {
                    if(index != value)
                    {
                        if (Table != null)
                        {
                            var keyNames = new List<string>(Table.PrimaryKeys.Select(c => c.Name));
                            var pkeys = new List<Column>();
                            List<Column> orderedCols = new List<Column>(Table.Columns.Values.OrderBy(c => c.Index));
                            List<int> ints = new List<int>(orderedCols.Select(c => c.Index));
                            ints.Remove(index);
                            ints.Insert(value, index);
                            foreach (var col in orderedCols)
                                Table.Columns[col.Name].index = ints.IndexOf(col.Index);
                            var rows = new List<object[]>();
                            foreach (var row in Table.Rows.OrderBy(r => r.Key))
                            {
                                var cells = new List<object>();
                                foreach (var cell in row.Value.Cells.OrderBy(c => Table.Columns[c.Key].Index))
                                    cells.Add(cell.Value);
                                rows.Add(cells.ToArray());
                            }
                            Table.Columns.Clear();
                            Table.Rows.Clear();
                            foreach (var col in orderedCols.OrderBy(c => c.Index)) {
                                var newCol = new Column(col.Name, col.DataType, col.DefaultValue);
                                Table.Columns.Add(newCol);
                                if (keyNames.Contains(col.Name))
                                    pkeys.Add(newCol);
                            }
                            foreach (var row in rows)
                                Table.Rows.Add(row);
                            Table.PrimaryKeys = pkeys.ToArray();
                        }
                        index = value;
                    }
                }
            }
            internal int index;
            public bool IsKey => Table != null && Table.PrimaryKeys.Contains(this);
            public int KeyIndex
            {
                get
                {
                    if (Table == null) return -1;
                    var reverseKeys = Table.primaryKeys.ToDictionary(k => k.Value, v => v.Key);
                    return reverseKeys.ContainsKey(this) ? reverseKeys[this] : -1;
                }
            }
            [JsonIgnore]
            public Dictionary<int, object> Values => Table == null ? new Dictionary<int, object>() : Table.Rows.ToDictionary(k => k.Key, v => v.Value[Name]);
            public Column() { }
            public Column(string name, Type datatype)
            {
                Name = name;
                DataType = datatype;
            }
            public Column(string name, Type datatype, object defaultValue)
            {
                Name = name;
                DataType = datatype;
                DefaultValue = defaultValue;
            }
            public Row FindRow(dynamic findValue)
            {
                if (IsKey)
                {
                    try { return Table.FindRow(findValue); }
                    catch { return null; }
                }
                else return null;
            }
            public override string ToString()
            {
                string toString = $"[{Index}] {Name} <{DataType.Name}>";
                if (defaultValue != null)
                    toString += $" {defaultValue}";
                if (IsKey)
                    toString += $" pk{KeyIndex}";
                return toString;
            }
        }
        [Serializable]
        public sealed class RowCollection<TKey, TValue> : Dictionary<TKey, TValue>
        {
            [JsonIgnore]
            public Table Table { get; private set; }
            public RowCollection(Table parent) : base(GetComparer()) => Table = parent;
            private static IEqualityComparer<TKey> GetComparer() => typeof(TKey) == typeof(string) ? (IEqualityComparer<TKey>)StringComparer.InvariantCultureIgnoreCase : EqualityComparer<TKey>.Default;
            public new TValue Add(TKey key, TValue value)
            {
                // index should always be set by increment and not the user
                int rowIndex = Table.Rows.Count;
                base.Add(key, value);
                Row addRow = Table.Rows[rowIndex];
                addRow.parent = Table;
                addRow.index = rowIndex;
                // can add an existing Row, or a New one
                bool initialize = addRow.Cells.ContainsKey("→0←");//1st is always 0
                if (initialize)
                {
                    Dictionary<string, object> tempCells = new Dictionary<string, object>(addRow.Cells);
                    addRow.Cells.Clear();
                    int columnIndex = 0;
                    foreach (Column column in Table.Columns.Values)
                    {
                        string indexName = $"→{columnIndex++}←";
                        // a Row can be created with a different number of columns, if greater then they will be clipped. If fewer, use the DefaultValue
                        // DataTable's work ok when the ItemArray.Count <= Column.Count but an error is thrown when the array's length exceeds the Column.Count 
                        object cellValue = tempCells.ContainsKey(indexName) ? tempCells[indexName] : column.DefaultValue;
                        bool allowCast = true;
                        if (allowCast)
                        {
                            object castedValue = null;
                            // code to test if value can be cast as the Column.Datatype
                            Type variableType = column.DataType;
                            try { castedValue = SurroundClass.ChangeType(cellValue, variableType); }
                            catch (Exception) { }
                            // or just let the value be since some might not work
                            addRow.Cells.Add(column.Name, castedValue);
                        }
                        else { addRow.Cells.Add(column.Name, cellValue); }
                    }
                }
                if (Table.PrimaryKeys.Any()) Table.AddKeys(new KeyValuePair<int, Row>(rowIndex, addRow));
                Table.OnRowsChanged(EventType.RowAdd, addRow);
                return value;
            }
            public Row Add()
            {
                Row emptyRow = new Row();
                foreach (Column column in Table.Columns.Values)
                    emptyRow.Cells.Add(column.Name, null);
                    //emptyRow[column.Name] = null;
                int rowIndex = Table.Rows.Count;
                emptyRow.index = rowIndex;
                Table.Rows.Add(rowIndex, emptyRow);
                return Table.Rows[rowIndex];
            }
            public Row Add(Row addRow)
            {
                int rowIndex = Table.Rows.Count;
                addRow.index = rowIndex;
                Table.Rows.Add(rowIndex, addRow);                
                return Table.Rows[rowIndex];
            }
            public Row Add(IEnumerable addValues) => Add(new Row(addValues, Table));
            public Row Add(object[] addValues) => Add(new Row(addValues, Table));
            public new TKey Remove(TKey key)
            {
                if (key != null)
                {
                    if (ContainsKey(key))
                    {
                        int rowIndex = int.Parse(key.ToString());
                        Row removeRow = Table.Rows[rowIndex];
                        Table.OnRowsChanged(EventType.RowRemove, removeRow);
                        base.Remove(key);
                    }
                }
                return key;
            }
            public new void Clear()
            {
                base.Clear();
                Table.OnRowsChanged(EventType.RowsCleared, null);
            }
            public Row Find(dynamic value) => Table?.FindRow(value);
        }
        public sealed class Row : IEquatable<Row>
        {
            [JsonIgnore]
            public Table Table { get { return parent; } }
            internal Table parent = null;
            public int Index
            {
                get
                {
                    return index;
                    //if (Table == null) return index;
                    //else return Table.Rows.Where(r => r.Value == this).FirstOrDefault().Key;
                }
            }
            internal int index;
            public object this[string key]
            {
                get { return Cells[key]; }
                set { Cells[key] = value; }
            }
            public object this[int index]
            {
                get {
                    if (index < Table.Columns.Count)
                    {
                        var cols = Table.Columns.ToDictionary(k => k.Value.Index, v => v.Value.Name);
                        return cols.ContainsKey(index) ? Cells[cols[index]] : null;
                    }
                    else
                        return null;
                }
                set {
                    if (index < Table.Columns.Count)
                    {
                        var cols = Table.Columns.ToDictionary(k => k.Value.Index, v => v.Value.Name);
                        if (cols.ContainsKey(index))
                            Cells[cols[index]] = value;
                    }
                }
            }
            public CellCollection<string, object> Cells;
            public Row() => Cells = new CellCollection<string, object>(this);
            public Row(IEnumerable values)
            {
                Cells = new CellCollection<string, object>(this);
                // if a Row is instantiated without a Parent(table) then the Cells.Keys will not be a column name, but →0←, →1←, →2←,... →n←
                int columnIndex = 0;
                foreach (object cellValue in values) Cells.Add($"→{columnIndex++}←", cellValue);
            }
            public Row(IEnumerable values, Table parent = null)
            {
                Cells = new CellCollection<string, object>(this);
                // if a Row is instantiated without a Parent(table) then the Cells.Keys will not be a column name, but →0←, →1←, →2←,... →n←
                if (values != null)
                {
                    this.parent = parent;
                    Dictionary<int, object> cellValues = new Dictionary<int, object>();
                    int valueIndex = 0;
                    foreach (object cellValue in values)
                        cellValues.Add(valueIndex++, cellValue);
                    int columnIndex = 0;
                    if (parent != null)
                    {
                        Dictionary<int, Column> cols = parent.Columns.Values.ToDictionary(k => k.Index, v => v);
                        for (int colIndex = 0; colIndex < (new int[] { cellValues.Count, parent.Columns.Count }).Min(); colIndex++)
                            Cells.Add(cols[colIndex].Name, cellValues[columnIndex++]);
                    }
                    else
                        foreach (object cellValue in values) Cells.Add($"→{columnIndex++}←", cellValue);
                }
            }
            [JsonIgnore]
            public object[] Values => Cells.Values.ToArray();
            public override string ToString()
            {
                string cellString = Cells.Any() ? string.Join("│", Cells.Values.Select(c => (c ?? string.Empty).ToString())) : "[empty]";
                return $"[{Index}] {cellString}";
            }

            public bool Equals(Row other)
            {
                if (other is null) return false;
                var bools = new List<bool>(Cells.Select(c => other.Cells.ContainsKey(c.Key) && other.Cells[c.Key] == c.Value));
                bools.AddRange(other.Cells.Select(c => Cells.ContainsKey(c.Key) && Cells[c.Key] == c.Value));
                return !bools.Any(b => !b);
            }
            public override int GetHashCode() {

                int hash = 0;
                foreach (var cellValue in Cells.Values) {
                    if(cellValue != null)
                        hash ^= cellValue.GetType() == typeof(string) ? ((string)cellValue ?? string.Empty).GetHashCode() : cellValue.GetHashCode();
                }
                return hash;
            }
            [Serializable]
            public class CellCollection<TKey, TValue> : Dictionary<TKey, TValue>
            {
                protected virtual void OnCellChanged(Row row, Column column, object oldValue, object newValue)
                {
                    CellChanged?.Invoke(this, new TableEventArgs(row, column, oldValue, newValue));
                }
                public event EventHandler<TableEventArgs> CellChanged;
                public Row Row { get; }
                public CellCollection(Row parent) : base(GetComparer()) { Row = parent; }
                private static IEqualityComparer<TKey> GetComparer() => (IEqualityComparer<TKey>)StringComparer.InvariantCultureIgnoreCase;
                //{
                //    return typeof(TKey) == typeof(string) ? (IEqualityComparer<TKey>)StringComparer.InvariantCultureIgnoreCase : EqualityComparer<TKey>.Default;
                //}

                public new TValue Add(TKey key, TValue value)
                {
                    /// replace all DbNull values with null to consolidate nullable values
                    /// cell values with both DbNull and null require 2 checks
                    base.Add(key, value);
                    object cellValue = Row.Cells[key.ToString()];
                    cellValue = cellValue == DBNull.Value ? null : cellValue;
                    Row.Cells[key.ToString()] = cellValue;
                    return (TValue)cellValue;
                } // <-- every cell add comes thru here
                public new TKey Remove(TKey key)
                {
                    base.Remove(key);
                    return key;
                }
                public new TValue this[TKey key]
                {
                    get
                    {
                        if (ContainsKey(key)) return base[key];
                        else
                            throw new Exception($"{key} key not found");
                    }
                    set
                    {
                        if (key != null)
                        {
                            Table table = Row.Table;
                            if (table != null)
                            {
                                string columnName = key.ToString();
                                if (table.Columns.ContainsKey(columnName))
                                {
                                    Column column = table.Columns[columnName];
                                    var fakeDict = new Dictionary<int, dynamic> { { 0, null } };
                                    TValue oldValue = ContainsKey(key) ? base[key] : fakeDict[0];
                                    table.OnCellChanged(Row, column, oldValue, value);
                                    base[key] = value;
                                }
                            }
                        }
                    }
                }
            }
        }
        #endregion

        public override string ToString() => $"Columns [{Columns.Count}] : Rows [{Rows.Count}] Name {Name ?? "None"}";
    }

    #region " ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ S U P P O R T I N G   C L A S S E S ,  S T R U C T U R E S ,  E N U M S "
    public enum EventType { none, TableCleared, RowsCleared, RowAdd, RowAddFail, RowRemove, CellChange, ColumnsCleared, ColumnAdd, ColumnCastFail, ColumnRemove }
    public class TableEventArgs : EventArgs
    {
        public Column Column;
        public Row Row { get; }
        public EventType TableAction { get; }
        public Type ProposedType { get; }
        public object CellValue { get; }
        public object ProposedCellValue { get; }
        public TableEventArgs(EventType columnEvent, Column column) { TableAction = columnEvent; Column = column; }
        public TableEventArgs(Column column, Type toType) { Column = column; ProposedType = toType; TableAction = EventType.ColumnCastFail; }
        public TableEventArgs(EventType rowEvent) { TableAction = rowEvent; }
        public TableEventArgs(EventType rowEvent, Row row) { TableAction = rowEvent; Row = row; }
        public TableEventArgs(Row row, Column column, object oldValue, object newValue)
        {
            TableAction = EventType.CellChange;
            Row = row;
            Column = column;
            CellValue = oldValue;
            ProposedCellValue = newValue;
        }
        public override string ToString()
        {
            if (TableAction == EventType.ColumnsCleared | TableAction == EventType.RowsCleared) { return TableAction.ToString(); }
            else
            {
                List<string> strings = new List<string>();
                if (Column != null) { strings.Add($"Column {Column.Name}"); }
                if (Row != null) { strings.Add($"Row {Row.Index}"); }
                if (CellValue != null) { strings.Add($"Cell(old) {CellValue}"); }
                if (ProposedCellValue != null) { strings.Add($"Cell(new) {ProposedCellValue}"); }
                return $"{TableAction} -> {string.Join(", ", strings)}";
            }
        }
    }
    public class CustomConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer) => throw new NotImplementedException("Not implemented yet");
        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.Null) { return string.Empty; }
            else if (reader.TokenType == JsonToken.String)
            {
                object jsonObject = serializer.Deserialize(reader, objectType);
                return jsonObject;
            }
            else
            {
                //List<string> lines = new List<string>(xx.Split(new string[] {Environment.NewLine}, StringSplitOptions.None));
                JObject obj = JObject.Load(reader);
                if (obj["Code"] != null)
                    return obj["Code"].ToString();
                else
                {
                    object jsonObject;
                    try
                    {
                        jsonObject = serializer.Deserialize(reader, objectType);
                        return jsonObject;
                    }
                    catch (JsonException je)
                    {
                        Console.WriteLine(je.Message);
                        return null;
                    }
                }

            }
        }
        public override bool CanWrite => false;
        public override bool CanConvert(Type objectType) => false;
    }
    internal static class SurroundClass
    {
        internal static object ChangeType(object value, Type type)
        {
            if (value == null && type.IsGenericType) return Activator.CreateInstance(type);
            if (value == null) return null;
            if (type == value.GetType()) return value;
            if (type.IsEnum)
            {
                if (value is string)
                    return Enum.Parse(type, value as string);
                else
                    return Enum.ToObject(type, value);
            }
            if (!type.IsInterface && type.IsGenericType)
            {
                Type innerType = type.GetGenericArguments()[0];
                object innerValue = ChangeType(value, innerType);
                return Activator.CreateInstance(type, new object[] { innerValue });
            }
            if (value is string && type == typeof(Guid)) return new Guid(value as string);
            if (value is string && type == typeof(Version)) return new Version(value as string);
            if (!(value is IConvertible)) return value;
            try { return Convert.ChangeType(value, type); }
            catch { return null; }
        }
        internal static Tuple<Type, object> GetDataType(string value)
        {
            if (value == null) {
                return Tuple.Create(typeof(string), (object)value);
            }

            else if (decimal.TryParse(value, out decimal _Decimal))
            {
                // REM /// NUMERIC+COULD BE DECIMAL Or INTEGER
                if (value.Split('.').Length == 1)
                {
                    // REM /// INTEGER
                    // REM /// MUST BE A WHOLE NUMBER. START WITH SMALLEST AND WORK UP
                    if (byte.TryParse(value, out byte _Byte))
                        return Tuple.Create(typeof(byte), (object)_Byte);
                    else
                    {
                        if (short.TryParse(value, out short _Short))
                            return Tuple.Create(typeof(short), (object)_Short);
                        else
                        {
                            if (int.TryParse(value, out int _Integer))
                                return Tuple.Create(typeof(int), (object)_Integer);
                            else
                            {
                                if (long.TryParse(value, out long _Long))
                                    return Tuple.Create(typeof(long), (object)_Long);
                                else
                                    // REM /// NOT DATE, BOOLEAN, DECIMAL, NOR INTEGER...DEFAULT TO STRING
                                    return Tuple.Create(typeof(string), (object)null);
                            }
                        }
                    }
                }
                else
                    // REM /// DECIMAL
                    return Tuple.Create(typeof(decimal), (object)_Decimal);
            }
            else
            {
                if (bool.TryParse(value, out bool _Boolean) | value.ToUpperInvariant() == "TRUE" | value.ToUpperInvariant() == "FALSE")
                    return Tuple.Create(typeof(bool), (object)_Boolean);

                else
                {
                    string[] dateFormats = new[] {
                "M/d/yyyy",
                "M/d/yyyy h:mm",
                "M/d/yyyy h:mm:ss",
                "M/d/yyyy h:mm:ss tt",
                "yyyy-M-d h:mm:ss tt"
            };
                    if (DateTime.TryParse(value, CultureInfo.CurrentCulture, DateTimeStyles.AdjustToUniversal, out DateTime date1))
                    {
                        if (date1.Date == date1)
                            // supposed to be no time
                            return Tuple.Create(typeof(DateTime), (object)date1);
                        else
                            // supposed to be with time
                            return Tuple.Create(typeof(DateTime), (object)date1);
                    }
                    else if (DateTime.TryParseExact(value, dateFormats, CultureInfo.CurrentCulture, DateTimeStyles.AllowWhiteSpaces, out DateTime date2)) {
                        if (date2.Date == date2)
                            // supposed to be no time
                            return Tuple.Create(typeof(DateTime), (object)date2);
                        else
                            // supposed to be with time
                            return Tuple.Create(typeof(DateTime), (object)date2);
                    }
                    else
                        // Some objects can not be converted in the ToString Function ... they only show as the object name
                        if (value.Contains("Drawing.Bitmap") | value.Contains("Drawing.Image"))
                        return Tuple.Create(typeof(Image), (object)null);

                    else if (value.Contains("Drawing.Icon"))
                        return Tuple.Create(typeof(Icon), (object)null);

                    else
                        return Tuple.Create(typeof(string), (object)value);
                }
            }
        }
        internal static Type GetDataType(IEnumerable<Type> types)
        {
            if (types == null)
                return null;
            else
            {
                List<Type> distincttypes = types.Where(t => t != null).Distinct().ToList();
                if (distincttypes.Any())
                {
                    int typeCount = distincttypes.Count;
                    if (typeCount == 1)
                        // ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  ONLY 1 TYPE, RETURN IT
                        return distincttypes.First();
                    else
                    {
                        // ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  MULTIPLE TYPES - CHOOSE BEST FIT ex) Date + DateAndTime = DateAndTime, Byte + Short = Short
                        if (distincttypes.Intersect(new Type[]
                        {
                            typeof(DateTime),
                        }).Count() == typeCount)
                            return typeof(DateTime);
                        else
                    // ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  NUMERIC
                    if (distincttypes.Intersect(new Type[]
                    {
                        typeof(byte),
                        typeof(short),
                        typeof(int),
                        typeof(long),
                        typeof(double),
                        typeof(decimal)
                    }).Count() == typeCount)
                        {
                            if (distincttypes.Intersect(new Type[]
                            {
                                typeof(byte),
                                typeof(short),
                                typeof(int),
                                typeof(long)
                            }).Count() == typeCount)
                            {
                                // ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  MIX OF INTEGER ... DESCEND IN SIZE TO GET LARGEST NECESSARY
                                if (distincttypes.Contains(typeof(long)))
                                    return typeof(long);

                                else if (distincttypes.Contains(typeof(int)))
                                    return typeof(int);

                                else if (distincttypes.Contains(typeof(short)))
                                    return typeof(short);

                                else
                                    return typeof(byte);
                            }
                            else
                                // ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  COULD BE MIX OF INTEGER, DECIMAL, DOUBLE
                                return typeof(double);
                        }
                        else if (distincttypes.Intersect(new Type[] { typeof(Image), typeof(Bitmap) }).Count() == typeCount)
                            // ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  IMAGE / BITMAP
                            return typeof(Bitmap);
                        else if (distincttypes.Intersect(new Type[]
                        {
                    typeof(Image),
                    typeof(Bitmap),
                    typeof(Icon)
                        }).Any())
                            // ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  IMAGES DON'T MIX WITH OTHER VALUES AS THEY CAN'T REPRESENTED IN A TEXT FORM
                            return typeof(object);
                        else
                            // ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■  STRING AS DEFAULT
                            return typeof(string);
                    }
                }
                else
                    return null;
            }
        }
        internal static Type GetDataType(object[] objects)
        {
            if (objects == null)
                return typeof(string);
            else
            {
                var nonNullTypes = new List<Type>();
                foreach(object obj in objects)
                    if(DBNull.Value != obj && obj !=null)
                        nonNullTypes.Add(GetDataType(obj.ToString()).Item1);
                return GetDataType(nonNullTypes);
            }
        }
        public static bool Cast(Type tryType, object value)
        {
            if (value == null) { return true; }
            else if (tryType == typeof(char)) { return value.GetType() == typeof(char); }
            else if (tryType == typeof(string)) { return value.GetType() == typeof(string); }
            else if (tryType == typeof(Bitmap)) { return value.GetType() == typeof(System.Drawing.Image) | value.GetType() == typeof(Bitmap); }
            else if (tryType == typeof(bool))
            {
                // something here with 0 | 1
                return value.GetType() == typeof(bool) | new string[] { "true", "false" }.Contains(value.ToString().ToLowerInvariant());
            }
            else if (tryType == typeof(DateTime))
            {
                // something here with cultureinfo
                return value.GetType() == typeof(DateTime);
            }
            else
            {
                string valueString = value.ToString().ToLowerInvariant();
                if (valueString.All(char.IsNumber))
                {
                    if (tryType == typeof(double)) { return double.TryParse(valueString, out double result); }
                    else if (tryType == typeof(decimal)) { return decimal.TryParse(valueString, out decimal result); }
                    else if (tryType == typeof(long)) { return long.TryParse(valueString, out long result); }
                    else if (tryType == typeof(ulong)) { return ulong.TryParse(valueString, out ulong result); }
                    else if (tryType == typeof(int)) { return int.TryParse(valueString, out int result); }
                    else if (tryType == typeof(float)) { return float.TryParse(valueString, out float result); }
                    else if (tryType == typeof(short)) { return short.TryParse(valueString, out short result); }
                    else if (tryType == typeof(ushort)) { return ushort.TryParse(valueString, out ushort result); }
                    else if (tryType == typeof(sbyte)) { return sbyte.TryParse(valueString, out sbyte result); }
                    else if (tryType == typeof(byte)) { return byte.TryParse(valueString, out byte result); }
                    //else if (tryType == typeof(nuint)) { return nuint.TryParse(valueString, out nuint result); }
                    //else if (tryType == typeof(nint)) { return nint.TryParse(valueString, out nint result); }
                    else { return false; }
                }
                else { return false; }
            }
        }
        private static List<string> EnumNames(Type enumType) => Enum.GetNames(enumType).ToList();
        public static T ParseEnum<T>(string value)
        {
            foreach (var enumItem in EnumNames(typeof(T)))
            {
                if (enumItem.ToUpperInvariant() == value?.ToUpperInvariant())
                    return (T)Enum.Parse(typeof(T), enumItem, true);
            }
            return default;
        }
    }
    #endregion
}
