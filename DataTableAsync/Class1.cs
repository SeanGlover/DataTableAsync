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
using System.Text.RegularExpressions;
using static DataTableAsync.Table;

namespace DataTableAsync
{
    // add sorting columns | rows
    // events

    public class Table
    {
        internal const string bar = "|";
        #region" ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ E V E N T S "
        protected virtual void OnColumnsChanged(EventType columnEvent, Column column) { ColumnsChanged?.Invoke(this, new TableEventArgs(columnEvent, column)); }
        public event EventHandler<TableEventArgs> ColumnsChanged;
        protected virtual void OnColumnCastFailed(Column column, Type toType) { ColumnCastFailed?.Invoke(this, new TableEventArgs(column, toType)); }
        public event EventHandler<TableEventArgs> ColumnCastFailed;
        protected virtual void OnRowsChanged(EventType rowEvent, Row row) { RowsChanged?.Invoke(this, new TableEventArgs(rowEvent, row)); }
        public event EventHandler<TableEventArgs> RowsChanged;
        protected virtual void OnCellChanged(Row row, Column column, object oldValue, object newValue)
        {
            CellChanged?.Invoke(this, new TableEventArgs(row, column, oldValue, newValue));
        }
        public event EventHandler<TableEventArgs> CellChanged;
        #endregion
        //[JsonConverter(typeof(CustomConverter))]
        public string Name { get; set; }
        //[JsonConverter(typeof(CustomConverter))]
        public virtual ColumnCollection<string, Column> Columns { get; private set; }
        //[JsonConverter(typeof(CustomConverter))]
        public virtual RowCollection<int, Row> Rows { get; private set; }
        public Table() { InitCollections(); }
        public Table(Tuple<Dictionary<byte, string>, Dictionary<int, Dictionary<string, string>>> Db2Out)
        {
            if (Db2Out != null && Db2Out.Item1.Any())
            {
                InitCollections();
                Dictionary<byte, string> cols = Db2Out.Item1;
                Dictionary<int, Dictionary<string, string>> rows = Db2Out.Item2;
                var values = cols.ToDictionary(k => k.Value, v => new Dictionary<int, string>());
                foreach (var row in rows)
                    foreach (var col in cols.Values)
                        values[col].Add(row.Key, row.Value[col]);
                Dictionary<string, Type> colTypes = values.ToDictionary(k => k.Key, v => SurroundClass.GetDataType(v.Value.Values));
                foreach (var colType in colTypes)
                    Columns.Add(colType.Key, colType.Value);
                foreach (var row in rows.OrderBy(r => r.Key))
                    Rows.Add(row.Value.Values);
            }
        }
        public Table(string filepath)
        {
            InitCollections();
            if (File.Exists(filepath ?? string.Empty)) Json_toTable(filepath);
            else
            {
                // html?
            }
        }
        public Table(FileInfo jsonFile)
        {
            InitCollections();
            if (jsonFile.Exists) { Json_toTable(jsonFile.FullName); }
        }
        public Table(DataTable sourceTable, string primaryKey = null)
        {
            // the primarykey may have to be fed if the datatable is coming from a Db2 query
            InitCollections();
            DataTable_toTable(sourceTable, primaryKey);
        }
        public Table(IEnumerable dataSource)
        {
            InitCollections();
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
        private void InitCollections() { Columns = new ColumnCollection<string, Column>(this); Rows = new RowCollection<int, Row>(this); }
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
        public void Clear() { Columns.Clear(); Rows.Clear(); }
        public Table Copy()
        {
            Table replica = Clone();
            //new Row() { ... }
            foreach (Row row in Rows.Values) { replica.Rows.Add(row); }
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
                if (column.IsKey) primarykeys.Add(replicaColumn);
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
                int columns_sameNameCount = 0;
                foreach (Column column in mergeTable.Columns.Values)
                    if (Columns.ContainsKey(column.Name)) columns_sameNameCount++;

                bool addByName = columns_sameNameCount >= Columns.Count;
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

                int columns_sameNameCount = 0;
                foreach (DataColumn column in mergeTable.Columns)
                    if (Columns.ContainsKey(column.ColumnName)) columns_sameNameCount++;

                bool addByName = columns_sameNameCount >= Columns.Count;
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
            foreach (Column column in Columns.Values) { copyTable.Columns.Add(new DataColumn(column.Name, column.DataType) { DefaultValue = column.DefaultValue }); }
            foreach (Row row in Rows.Values) { copyTable.Rows.Add(row.Cells.Values.ToArray()); }
            return copyTable;
        }
        [JsonIgnore]
        public List<Row> AsEnumerable { get { return new List<Row>(Rows.Values); } }
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

                    Dictionary<string, Dictionary<int, string>> columnStrings = new Dictionary<string, Dictionary<int, string>>();
                    Dictionary<string, int> columnWidths = new Dictionary<string, int>();
                    Dictionary<string, string> columnAlignments = new Dictionary<string, string>();
                    List<Type> lefts = new List<Type> { typeof(string) };
                    List<Type> centers = new List<Type> { typeof(bool), typeof(byte), typeof(short), typeof(int), typeof(long), typeof(DateTime), typeof(Icon), typeof(Image) };
                    List<Type> rights = new List<Type> { typeof(double), typeof(decimal) };
                    int columnIndex = 1;
                    const double testDec = 1.5;

                    using (Bitmap bmp = new Bitmap(50, 50))
                    {
                        using (Graphics g = Graphics.FromImage(bmp))
                        {
                            foreach (Column col in table.Columns.Values)
                            {
                                Type colType = col.DataType;
                                var decConvert = SurroundClass.ChangeType(testDec, colType);
                                bool colType_isDecimal = decConvert is testDec;
                                bool colValues_areDecimal = colType_isDecimal;
                                if (colType_isDecimal & col.Values.Any())
                                {
                                    Type colGetType = SurroundClass.GetDataType(col.Values.Values);
                                    decConvert = SurroundClass.ChangeType(testDec, colGetType);
                                    double.TryParse(decConvert.ToString(), out double dblConvert);
                                    colValues_areDecimal = dblConvert == testDec;
                                    if (!colValues_areDecimal) colType = colGetType;
                                }

                                Dictionary<int, string> strings = new Dictionary<int, string>();
                                int rowIndex = 0;
                                List<string> rowArray = new List<string>();
                                foreach (Row row in table.Rows.Values)
                                {
                                    object rowCell = row.Cells[col.Name];
                                    var castCell = SurroundClass.ChangeType(rowCell, colType);
                                    string cellString;
                                    if (rowCell == DBNull.Value | rowCell == null) cellString = string.Empty;
                                    else if (colType == typeof(DateTime)) // Dates
                                    {
                                        DateTime cellDate = (DateTime)castCell;
                                        if (cellDate.TimeOfDay.Ticks == 0)
                                            cellString = cellDate.ToShortDateString();
                                        else
                                            cellString = cellDate.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
                                    }
                                    else if (colValues_areDecimal) // Decimal .
                                    {
                                        NumberFormatInfo nfi = new CultureInfo("en-US", false).NumberFormat;
                                        // Displays a value with the default separator (".")
                                        bool testDouble = double.TryParse(rowCell.ToString(), out double doubleValue);
                                        cellString = doubleValue.ToString("N", nfi);
                                    }
                                    else { cellString = Regex.Replace(rowCell.ToString(), @"[\n]", "<br/>"); } // Assume string
                                    strings.Add(rowIndex, cellString);
                                    if (!Rows.ContainsKey(row)) { Rows.Add(row, new List<string>()); }
                                    Rows[row].Add(cellString);
                                    rowIndex += 1;
                                }
                                columnStrings.Add(col.Name, strings);

                                double columnHeadWidth = g.MeasureString(col.Name, headerFont).Width;
                                double columnMaxContentWidth = strings.Values.Any() ? strings.Values.Select(c => g.MeasureString(c, tableFont).Width).Max() : 0;
                                double columnWidth = 18 + columnHeadWidth >= columnMaxContentWidth ? columnHeadWidth : columnMaxContentWidth;

                                columnWidths.Add(col.Name, Convert.ToInt32(Math.Ceiling(columnWidth + 6))); // padded 6
                                                                                                            // tr td:nth-child(2) {text-align: right;}
                                string columnAlignment = $"tr td:nth-child({columnIndex})" + " {text-align: " + (lefts.Contains(colType) ? "left" : rights.Contains(colType) ? "right" : "center").ToString() + ";}";
                                columnAlignments.Add(col.Name, columnAlignment);
                                columnIndex += 1;
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
                    Top.Add(string.Join(Environment.NewLine, columnAlignments.Values));
                    Top.Add("</style>");
                    Top.Add("</head>");
                    Top.Add("<body>");
                    Top.Add("<table>");
                    Top.Add("<tr>" + string.Join("", from C in columnWidths select "<th width=" + C.Value + ";>" + C.Key + "</th>") + "</tr>");
                    List<string> Middle = new List<string>();
                    foreach (var Row in Rows)
                    {
                        // "#F5F5F5" : "#FFFFFF"
                        Middle.Add("<tr style=background-color:" + (Middle.Count % 2 == 0 ? hexRowBackColor : "#FFFFFF").ToString() + $"; color:{hexRowForeColor}>" + string.Join("", from IA in Row.Value select "<td>" + IA + "</td>") + "</tr>");
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
                //row\Col1 ‖ Col2 ‖ Col3 ‖ Col4|Col5|Col6|Col7|Col8|Col9
                //0  |x   |y   |z   |    |    |    |    |    |
                //1  |x   |y   |z   |    |    |    |    |    |
                //2  |x   |y   |z   |    |    |    |    |    |
                //3  |x   |y   |z   |    |    |    |    |    |
                //4  |x   |y   |z   |    |    |    |    |    |
                //5  |x   |y   |z   |    |    |    |    |    |
                //6  |x   |y   |z   |    |    |    |    |    |
                //7  |x   |y   |z   |    |    |    |    |    |
                List<string> rowLines = new List<string>();
                int rowIndexWidth = new int[] { Rows.Any() ? Rows.Keys.Max().ToString().Length : 0, "row".Length }.Max();
                List<string> header = new List<string>(new string[] { $"row{new string(' ', rowIndexWidth - "row".Length)}" });
                Dictionary<string, List<int>> columnWidths = new Dictionary<string, List<int>>();
                Dictionary<string, Dictionary<int, string>> columnStrings = new Dictionary<string, Dictionary<int, string>>();
                foreach (var col in Columns)
                {
                    columnWidths.Add(col.Key, new List<int>());
                    columnWidths[col.Key].Add(col.Key.Length);
                    columnStrings.Add(col.Key, new Dictionary<int, string>());
                    foreach (var row in Rows)
                    {
                        object cellValue = row.Value.Cells[col.Key];
                        string cellString = cellValue == DBNull.Value || cellValue == null ? string.Empty : cellValue.ToString(); // dates always long form :(
                        columnStrings[col.Key].Add(row.Key, cellString);
                        columnWidths[col.Key].Add(cellString.Length);
                    }
                    int colMaxWidth = columnWidths[col.Key].Max();
                    header.Add($"{col.Key}{new string(' ', colMaxWidth - col.Key.Length)}");
                }
                // add header row 1st
                rowLines.Add(string.Join(bar, header));
                foreach (var row in Rows)
                {
                    List<string> rowArray = new List<string>(new string[] { $"{row.Key}{new string(' ', rowIndexWidth - row.Key.ToString().Length)}" });
                    foreach (var col in Columns)
                    {
                        int columnWidth = columnWidths[col.Key].Max();
                        string cellString = columnStrings[col.Key][row.Key];
                        rowArray.Add($"{cellString}{new string(' ', columnWidth - cellString.Length)}");
                    }
                    rowLines.Add(string.Join(bar, rowArray));
                }
                return string.Join(Environment.NewLine, rowLines);
            }
        }
        [JsonIgnore]
        public string Json => JsonConvert.SerializeObject(this, Formatting.None);
        [JsonIgnore]
        public Column[] PrimaryKeys
        {
            get => primaryKeys.OrderBy(pk => pk.Key).Select(pk => pk.Value).ToArray();
            set
            {
                primaryKeys.Clear();
                foreach (Column pk in value) primaryKeys.Add((byte)primaryKeys.Count, pk);
                // setting keys AFTER rows are loaded
                foreach (var row in Rows) AddKeys(row);
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
        internal void AddKeys(KeyValuePair<int, Row> row)
        {
            var tempDict = Keys;
            foreach (Column col in PrimaryKeys)
            {
                var castValue = SurroundClass.ChangeType(row.Value[col.Name], col.DataType);
                if (!tempDict.ContainsKey(castValue))
                    tempDict[castValue] = new Dictionary<dynamic, dynamic>();
                if (col.KeyIndex == (PrimaryKeys.Count() - 1))
                    tempDict[castValue] = row.Key;
                else
                    tempDict = tempDict[castValue];
            }
        }
        #region" ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ N E S T E D   C L A S S E S [ COLUMNS|ROWS] "
        [Serializable]
        public sealed class ColumnCollection<TKey, TValue> : Dictionary<TKey, TValue>
        {
            [JsonIgnore]
            public Table Table { get; private set; }
            public ColumnCollection(Table parent) : base(GetComparer()) { Table = parent; }
            private static IEqualityComparer<TKey> GetComparer() => typeof(TKey) == typeof(string) ? (IEqualityComparer<TKey>)StringComparer.InvariantCultureIgnoreCase : EqualityComparer<TKey>.Default;
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
            public List<Column> AddRange(Column[] columns)
            {
                foreach (Column col in columns) Add(col);
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
                        if (removeColumn.IsKey)
                        {
                            // reindex the primary keys without the removeColumn
                            Dictionary<byte, Column> newKeys = new Dictionary<byte, Column>();
                            foreach (var col in Table.primaryKeys.OrderBy(c => c.Key))
                                if (col.Value != removeColumn) newKeys.Add((byte)newKeys.Count, col.Value);
                            Table.primaryKeys = newKeys;
                        }
                        base.Remove(key);
                        foreach (Row row in Table.AsEnumerable) { Table.Rows[row.Index].Cells.Remove(key.ToString()); }
                        Table.OnColumnsChanged(EventType.ColumnRemove, removeColumn);
                    }
                }
                return key;
            }
            public new void Clear()
            {
                Table.primaryKeys.Clear();
                foreach (Row row in Table.Rows.Values) { row.Cells.Clear(); }
                Table.OnColumnsChanged(EventType.ColumnsCleared, null);
                base.Clear();
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
                                foreach (Row row in Table.Rows.Values) { if (!Wrap.Cast(value, row.Cells[Name])) { cantParseAllValues = true; break; } }
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
                        if (Wrap.Cast(DataType, value))
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
                get { return index; }
                set
                {
                    if (index != value)
                    {
                        if (Table != null)
                        {
                            List<Column> orderedCols = new List<Column>(Table.Columns.Values.OrderBy(c => c.Index));
                            List<int> ints = new List<int>(orderedCols.Select(c => c.Index));
                            ints.Remove(index);
                            ints.Insert(value, index);
                            foreach (var col in orderedCols) Table.Columns[col.Name].index = ints.IndexOf(col.Index);
                        }
                        index = value;
                        if (parent != null) parent.Columns.OrderBy(c => c.Value.Index); // does nothing
                    }
                }
            }
            private int index;
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
            public RowCollection(Table parent) : base(GetComparer()) { Table = parent; }
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
            public Row Add(Row addRow)
            {
                int rowIndex = Table.Rows.Count;
                addRow.index = rowIndex;
                Table.Rows.Add(rowIndex, addRow);
                return Table.Rows[rowIndex];
            }
            public Row Add(IEnumerable addValues) { return Add(new Row(addValues, Table)); }
            public Row Add(object[] addValues) { return Add(new Row(addValues, Table)); }
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
                Table.OnRowsChanged(EventType.RowsCleared, null);
                base.Clear();
            }
            public Row Find(dynamic value) => Table?.FindRow(value);
        }
        public sealed class Row
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
                    Dictionary<int, object> Values = new Dictionary<int, object>();
                    int valueIndex = 0;
                    foreach (object cellValue in values) Values.Add(valueIndex++, cellValue);
                    int columnIndex = 0;
                    if (parent != null)
                    {
                        Dictionary<int, Column> cols = parent.Columns.Values.ToDictionary(k => k.Index, v => v);
                        for (int colIndex = 0; colIndex < (new int[] { Values.Count, parent.Columns.Count }).Min(); colIndex++)
                            Cells.Add(cols[colIndex].Name, Values[columnIndex++]);
                    }
                    else
                        foreach (object cellValue in values) Cells.Add($"→{columnIndex++}←", cellValue);
                }
            }
            public override string ToString()
            {
                string cellString = Cells.Any() ? string.Join(bar, Cells.Values.Select(c => (c ?? string.Empty).ToString())) : "[empty]";
                return $"[{Index}] {cellString}";
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
                private static IEqualityComparer<TKey> GetComparer()
                {
                    return typeof(TKey) == typeof(string) ? (IEqualityComparer<TKey>)StringComparer.InvariantCultureIgnoreCase : EqualityComparer<TKey>.Default;
                }

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
                            if (ContainsKey(key))
                            {
                                Table table = Row.Table;
                                if (table != null)
                                {
                                    string columnName = key.ToString();
                                    if (table.Columns.ContainsKey(columnName))
                                    {
                                        Column column = table.Columns[columnName];
                                        table.OnCellChanged(Row, column, base[key], value);
                                        base[key] = value;
                                    }
                                }
                            }
                            else
                            {
                                throw new Exception($"{key} key not found");
                            }
                        }
                    }
                }
            }
        }
        #endregion

        public override string ToString() { return $"Columns [{Columns.Count}] : Rows [{Rows.Count}] Name {Name ?? "None"}"; }
    }

    #region " ■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■ S U P P O R T I N G   C L A S S E S ,  S T R U C T U R E S ,  E N U M S "
    public enum EventType { none, RowsCleared, RowAdd, RowAddFail, RowRemove, CellChange, ColumnsCleared, ColumnAdd, ColumnCastFail, ColumnRemove }
    public class TableEventArgs : EventArgs
    {
        public Table.Column Column;
        public Table.Row Row { get; }
        public EventType TableAction { get; }
        public Type ProposedType { get; }
        public object CellValue { get; }
        public object ProposedCellValue { get; }
        public TableEventArgs(EventType columnEvent, Table.Column column) { TableAction = columnEvent; Column = column; }
        public TableEventArgs(Table.Column column, Type toType) { Column = column; ProposedType = toType; TableAction = EventType.ColumnCastFail; }
        public TableEventArgs(EventType rowEvent) { TableAction = rowEvent; }
        public TableEventArgs(EventType rowEvent, Table.Row row) { TableAction = rowEvent; Row = row; }
        public TableEventArgs(Table.Row row, Table.Column column, object oldValue, object newValue)
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
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException("Not implemented yet");
        }
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
        public override bool CanWrite
        {
            get { return false; }
        }
        public override bool CanConvert(Type objectType)
        {
            return false;
        }
    }
    public static class Wrap
    {
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
        internal static Type GetDataType(string value)
        {
            if (value == null)
                return typeof(string);
            else if (decimal.TryParse(value, out decimal _Decimal))
            {
                // REM /// NUMERIC+COULD BE DECIMAL Or INTEGER
                if (value.Split('.').Length == 1)
                {
                    // REM /// INTEGER
                    // REM /// MUST BE A WHOLE NUMBER. START WITH SMALLEST AND WORK UP
                    if (byte.TryParse(value, out byte _Byte))
                        return typeof(byte);
                    else
                    {
                        if (short.TryParse(value, out short _Short))
                            return typeof(short);
                        else
                        {
                            if (int.TryParse(value, out int _Integer))
                                return typeof(int);
                            else
                            {
                                if (long.TryParse(value, out long _Long))
                                    return typeof(long);
                                else
                                    // REM /// NOT DATE, BOOLEAN, DECIMAL, NOR INTEGER...DEFAULT TO STRING
                                    return typeof(string);
                            }
                        }
                    }
                }
                else
                    // REM /// DECIMAL
                    return typeof(decimal);
            }
            else
            {
                if (bool.TryParse(value, out bool _Boolean) | value.ToUpperInvariant() == "TRUE" | value.ToUpperInvariant() == "FALSE")
                    return typeof(bool);

                else
                {
                    string[] dateFormats = new[] {
                "M/d/yyyy",
                "M/d/yyyy h:mm",
                "M/d/yyyy h:mm:ss",
                "M/d/yyyy h:mm:ss tt",
                "yyyy-M-d h:mm:ss tt"
            };
                    if (DateTime.TryParse(value, CultureInfo.CurrentCulture, DateTimeStyles.AdjustToUniversal, out DateTime _Date) | DateTime.TryParseExact(value, dateFormats, CultureInfo.CurrentCulture, DateTimeStyles.AllowWhiteSpaces, out _Date))
                    {
                        if (_Date.Date == _Date)
                            // supposed to be no time
                            return typeof(DateTime);
                        else
                            // supposed to be with time
                            return typeof(DateTime);
                    }
                    else
                        // Some objects can not be converted in the ToString Function ... they only show as the object name
                        if (value.Contains("Drawing.Bitmap") | value.Contains("Drawing.Image"))
                        return typeof(Image);

                    else if (value.Contains("Drawing.Icon"))
                        return typeof(Icon);

                    else
                        return typeof(string);
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
        internal static Type GetDataType(IEnumerable<string> strings) => strings == null ? null : GetDataType(strings.Select(s => GetDataType(s)));
        internal static Type GetDataType(IEnumerable<object> objects) => objects == null ? null : GetDataType(objects.Where(o => o != null).Select(o => GetDataType(o.ToString())));
    }
    #endregion
}
