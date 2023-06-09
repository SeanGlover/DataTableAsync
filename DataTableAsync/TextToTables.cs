using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;

namespace DataTableAsync
{
    public enum RunType {none, ddl, sql }

    public class TextToTables
    {
        public List<ScriptElement> Labels { get; }
        public List<SchemaTable> Tables => Labels.Where(l => l.Source == ScriptElement.LabelName.SystemTable).Select(l => new SchemaTable(l.Block.Value)).Distinct().ToList();
        public string Script { get; }
        public RunType ScriptType { get; }

        private const string objectPattern = "([A-Z0-9!%{}^~_@#$]{1,}([.][A-Z0-9!%{}^~_@#$]{1,}){0,2})";
        private readonly List<ScriptElement> Withs = new List<ScriptElement>();

        public TextToTables(string sql_ddl)
        {
            Labels = new List<ScriptElement>();
            Script = sql_ddl;
            ScriptType = GetInstructionType(Script);
            AddLabels();
        }

        public static RunType GetInstructionType(string sql_ddl)
        {
            ///// _CommentsReplaced REMOVES POTENTIAL MATCHES FROM TEXT INSIDE A COMMENT...WHICH SHOULD NOT BE CONSIDERED
            string textNoComments = StripComments(sql_ddl);
            if ((textNoComments ?? string.Empty).Any())
            {
                Match Match_Comment = Regex.Match(textNoComments, @"COMMENT\s{1,}ON\s{1,}(TABLE|COLUMN|FUNCTION|TRIGGER|DOCUMENT|PROCEDURE|ROLE|TRUSTED|MASK)\s{1,}", RegexOptions.IgnoreCase);
                Match Match_Drop = Regex.Match(textNoComments, @"DROP[\s]{1,}(TABLE|VIEW|Function|TRIGGER)[\s]{1,}" + objectPattern, RegexOptions.IgnoreCase);
                Match Match_Insert = Regex.Match(textNoComments, @"INSERT[\s]{1,}INTO[\s]{1,}" + objectPattern + @"([\s]{0,}\([A-Z0-9!%{}^~_@#$]{1,}(,[\s]{0,}[A-Z0-9!%{}^~_@#$]{1,}){0,}\)){0,}", RegexOptions.IgnoreCase);
                Match Match_Delete = Regex.Match(textNoComments, @"DELETE[\s]{1,}FROM[\s]{1,}" + objectPattern, RegexOptions.IgnoreCase);
                Match Match_Update = Regex.Match(textNoComments, @"UPDATE[\s]{1,}" + objectPattern + @"([\s]{1,}([A-Z0-9!%{}^~_@#$]{1,})){0,1}[\s]{1,}Set[\s]{1,}", RegexOptions.IgnoreCase);
                Match Match_CreateAlterDrop = Regex.Match(textNoComments, @"(CREATE|ALTER|DROP)(\s{1,}OR REPLACE){0,1}\s{1,}((AUXILIARY\s+){0,1}TABLE|(BLOB\s+|CLOB\s+|LOB\s+)TABLESPACE|VIEW|Function|TRIGGER)[\s]{1,}" + objectPattern, RegexOptions.IgnoreCase);
                Match Match_GrantRevoke = Regex.Match(textNoComments, @"(GRANT|REVOKE)[\s]{1,}((Select|UPDATE|INSERT|DELETE|ALTER|INDEX|REFERENCES|EXECUTE)[\s]{0,}[,]{0,1}[\s]{0,}){1,8}[\s]{1,}On[\s]{1,}" + objectPattern, RegexOptions.IgnoreCase);
                if (Match_Comment.Success | Match_Drop.Success | Match_Insert.Success | Match_Delete.Success | Match_Update.Success | Match_CreateAlterDrop.Success | Match_GrantRevoke.Success)
                    return RunType.ddl;
                else if (Regex.Match(textNoComments, "SELECT[^■]{1,}?(?=FROM)", RegexOptions.IgnoreCase).Success)
                    return RunType.sql;
                else
                    return RunType.none;
            }
            else
                return RunType.none;
        }
        private static string StripComments(string sql_ddl)
        {
            // EXEMPTS TEXT FROM CONSIDERATION, BUT NOT IF IT IS IN APOSTROPHES (CONSTANTS)
            // 1] SELECT '----------------------' = CONSTANT
            // 2] --SELECT 'SPG'                  = GREENOUT

            var textIn = sql_ddl ?? string.Empty;
            var dblHyphens = new List<Match>(Regex.Matches(textIn, "--[^\r\n]{1,}|$", RegexOptions.IgnoreCase).OfType<Match>());
            var qtdHyphens = new List<Match>(Regex.Matches(textIn, "'[^'\r\n]{0,}'", RegexOptions.IgnoreCase).OfType<Match>());
            var okHyphens = new List<Match>();
            foreach (var dblHyphen in dblHyphens)
            {
                var dh = new List<int>(Enumerable.Range(dblHyphen.Index, dblHyphen.Length));
                var canAdd = true;
                foreach (var qtdHyphen in qtdHyphens)
                {
                    var qh = new List<int>(Enumerable.Range(qtdHyphen.Index + 1, qtdHyphen.Length - 2));
                    if (qh.Intersect(dh).Any())
                        canAdd = false;
                }
                if (canAdd)
                    okHyphens.Add(dblHyphen);
            }

            string textOut = textIn;
            foreach (var okHyphen in okHyphens)
            {
                textOut = textOut.Remove(okHyphen.Index, okHyphen.Length);
                textOut = textOut.Insert(okHyphen.Index, new string('¼', okHyphen.Length));
            }
            return textOut.Replace("¼", string.Empty);
        }
        private void AddLabels()
        {
            ///// REQUIRES KNOWING IF IsDDL + CALLS ParenthesisNodes
            string Blackout_Selects = Script;
            string BlackOut_Parentheses = Script;
            string Blackout_Handled = Script;

            if ((Script ?? string.Empty).Any())
            {
                ///// BEGIN BY IDENTIFYING SIMPLE OBJECTS
                var Unions = new List<StringData>(Regex.Matches(Script, @"[\s\r\n]{1,}\b(UNION ALL|UNION|EXCEPT|INTERSECT)\b[\s\r\n]{1,}", RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
                Labels.AddRange(Unions.Select(u => new ScriptElement(u, ScriptElement.LabelName.Union)));

                var Selects = new List<StringData>(Regex.Matches(Script, "SELECT[^■]{1,}?(?=FROM)", RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
                foreach (var SelectStatement in Selects)
                {
                    Blackout_Selects = ChangeText(Blackout_Selects, SelectStatement);
                    Labels.Add(new ScriptElement(SelectStatement, ScriptElement.LabelName.SelectBlock));

                    ///// COMPLICATED TO DETERMINE END OF FIELD...EXAMPLE:    (CASE LEFT(R.SAI, 2) WHEN 'WW' THEN 'Y' ELSE 'N' END) --H.IN
                    var selectFields = FieldsFromBlocks(SelectStatement, "SELECT(\\sdistinct){0,1}\\s{1,}");
                    Labels.AddRange(selectFields);
                }

                var GroupBys = new List<StringData>(Regex.Matches(Script, @"\bGROUP[\s]{1,}BY\b[\s]{1,}" + objectPattern + @"(,[\s]{1,}[A-Z0-9!%{}^~_@#$]{1,}([.][A-Z0-9!%{}^~_@#$]{1,}){0,2}){0,}", RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
                foreach (var GroupBy in GroupBys)
                {
                    var GroupByHighlight = Regex.Match(GroupBy.Value, @"\bGROUP[\s]{1,}BY\b", RegexOptions.IgnoreCase).Value;
                    Labels.Add(new ScriptElement(GroupBy, ScriptElement.LabelName.GroupBlock));
                    Labels.AddRange(FieldsFromBlocks(GroupBy, @"GROUP[\s]{1,}BY[\s]{1,}"));
                }
                
                var OrderBys = new List<StringData>(Regex.Matches(Script, @"\bORDER[\s]{1,}BY\b[\s]{1,}" + objectPattern + @"(,[\s]{1,}[A-Z0-9!%{}^~_@#$]{1,}([.][A-Z0-9!%{}^~_@#$]{1,}){0,2}){0,}", RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
                foreach (var OrderBy in OrderBys)
                {
                    var OrderByHighlight = Regex.Match(OrderBy.Value, @"\bORDER[\s]{1,}BY\b", RegexOptions.IgnoreCase).Value;
                    Labels.Add(new ScriptElement(OrderBy, ScriptElement.LabelName.OrderBlock));
                    Labels.AddRange(FieldsFromBlocks(OrderBy, @"ORDER[\s]{1,}BY[\s]{1,}"));
                }
                
                var Limits = new List<StringData>(Regex.Matches(Script, @"(FETCH[\s]{1,}FIRST[\s]{1,}[0-9]{1,}[\s]{1,}ROWS[\s]{1,}ONLY|LIMIT[\s]{1,}[0-9]{1,})", RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
                Labels.AddRange(Limits.Select(l => new ScriptElement(l, ScriptElement.LabelName.Limit)));

                ///// STRIP AWAY PARTS OF TEXT IDENTIFIED SO THEY ARE NOT CONSIDERED AGAIN
                ///// LOOKS FOR ACCEPTABLE OBJECT NAMING CONVENTIONS- CERTAIN CHARACTERS ARE NOT ALLOWED IN TABLE, VIEW, FUNCTION, AND TRIGGER NAMES + CAN BE AS: {1] DB.OWNER.NAME, 2] OWNER.NAME, 3] NAME}

                var PotentialObjects = new List<StringData>(Regex.Matches(Script, objectPattern, RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
                if (ScriptType == RunType.ddl)
                {
                    var Patterns = new Dictionary<string, string>
                    {
                        { "TriggerInsertDelete", @"(BEFORE|AFTER|INSTEAD[\s]{1,}OF)[\s]{1,}(INSERT|DELETE)[\s]{1,}ON[\s]" },
                        { "TriggerUpdate", @"(BEFORE|AFTER|INSTEAD[\s]{1,}OF)[\s]{1,}(UPDATE[\s]{1,}OF[\s]{1,})([A-Z0-9!%{}^~_@#$]{1,})([\s]{0,}[,][\s]{0,}[A-Z0-9!%{}^~_@#$]{1,}){0,}[\s]{1,}ON[\s]{1,}" },
                        ///// OTHER DDL COMMANDS: GRANT|REVOKE (SELECT|UPDATE|INSERT|DELETE|ALTER|INDEX|REFERENCES|EXECUTE) ON
                        { "GrantRevoke", @"(GRANT|REVOKE)[\s]{1,}(SELECT|UPDATE|INSERT|DELETE|ALTER|INDEX|REFERENCES|EXECUTE)[\s]{1,}ON[\s]{1,}(FUNCTION[\s]{1,}){0,}" },
                        ///// OTHER DDL COMMANDS: ALTER|DROP TABLE (CREATE WOULD BE NEW AND THEREFORE SHOULD NOT COUNT)
                        { "AlterDrop", @"(ALTER|DROP)[\s]{1,}(TABLE|VIEW)[\s]{1,}" },
                        ///// OTHER DDL COMMANDS: INSERT INTO, UPDATE, DELETE FROM}
                        { "InsertUpdateDelete", @"(INSERT[\s]{1,}INTO|DELETE[\s]{1,}FROM|UPDATE)[\s]{1,}" }
                    };
                    foreach (var Pattern in Patterns.Keys)
                    {
                        List<StringData> Statements = new List<StringData>(Regex.Matches(Script, Patterns[Pattern] + objectPattern, RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
                        List<string> KeyWords = new List<string>(Regex.Matches(Patterns[Pattern], "[A-Z]{2,}", RegexOptions.IgnoreCase).OfType<Match>().Select(pk => pk.Value));
                        var potentialTables = from op in PotentialObjects where (from s in Statements where s.Contains(op) select s).Any() & !KeyWords.Intersect(new string[] { op.Value }).Any() select op;
                        foreach (var table in potentialTables)
                        {
                            ScriptElement SystemTableElement = new ScriptElement(table, ScriptElement.LabelName.SystemTable);
                            Labels.Add(SystemTableElement);
                        }
                        foreach (var Statement in Statements)
                            Blackout_Handled = ChangeText(Script, Statement);
                    }
                }

                var Root = new StringData(0, 0, Blackout_Handled);
                ParenthesisNodes(Root, Script);
                BlackOut_Parentheses = Blackout_Handled;
                ///// EASIER TO CAPTURE WITH BLOCKS WHEN IGNORING CONTENT INSIDE WITH(ignore me)
                foreach (StringData ParenthesesBlock in Root.All)
                    BlackOut_Parentheses = ChangeText(BlackOut_Parentheses, ParenthesesBlock);

                ///// 1] WITH DEBITS ■■■■ AS ■■■■ |2] WITH DEBITS AS ■■■■ |3] , FINAL ■■■■ AS ■■■■ |4] , FINAL AS ■■■■
                string WithPattern = @"(?<=WITH |,)[\s]{0,}[A-Z0-9!%{}^~_@#$]{1,}[\s]{0,}[■]{0,}[\s]AS[\s]{0,}[■]{1,}";
                List<StringData> WithBlocks = new List<StringData>(Regex.Matches(BlackOut_Parentheses, WithPattern, RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
                foreach (var WithBlock in WithBlocks)
                {
                    ///// REGEX LOOKBEHIND MUST HAVE A FIXED LENGTH WHICH MEANS HAVING TO ADJUST THE StringData.START ACCOUNTING FOR PRECEDING SPACES
                    int WithStart = 0;
                    foreach (var Item in WithBlock.Value)
                    {
                        if (Item == ' ')
                            WithStart += 1;
                        else
                            break;
                    }
                    int NewStart = WithBlock.Start + WithStart;
                    int NewLength = WithBlock.Length - WithStart;
                    string WithValue = WithBlock.Value.Substring(WithStart, NewLength).Split(' ').First();
                    WithValue = WithValue.Split('■').First();
                    var WithElement = new ScriptElement(new StringData(NewStart, WithBlock.Length - WithStart, Script.Substring(NewStart, NewLength)), ScriptElement.LabelName.WithBlock);
                    Withs.Add(WithElement);
                }
                Labels.AddRange(Withs);
                ///// IT IS BEST TO HANDLE OUTSIDE () AND INSIDE () SEPARATELY
                List<ScriptElement> From_OutsideWiths = new List<ScriptElement>(FromBlocks(new StringData(0, 0, BlackOut_Parentheses)));
                Labels.AddRange(From_OutsideWiths);
                ///// (SELECT...FROM TABLENAME...WHERE)
                ///// NEED INNERMOST FROM STATEMENTS FIRST SINCE THEY WILL INTERFERE WITH OUTER FROM STATEMENTS...SELECT A, (SELECT B FROM) FROM (SELECT *)
                KeyValuePair<string, List<ScriptElement>> FromInnerValuePair = FromWhittle(Blackout_Handled);
                Labels.AddRange(FromInnerValuePair.Value);
                KeyValuePair<string, List<ScriptElement>> FromOuterValuePair = FromWhittle(FromInnerValuePair.Key);
                Labels.AddRange(FromOuterValuePair.Value);
            }
        }
        private string ChangeText(string textIn, StringData withBlock, char Value = '■')
        {
            string textOut = textIn;
            {
                textOut = textOut.Remove(withBlock.Start, withBlock.Length);
                textOut = textOut.Insert(withBlock.Start, new string(Value, withBlock.Length));
            }
            return textOut;
        }
        private KeyValuePair<string, List<ScriptElement>> FromWhittle(string TextIn)
        {
            var Root = new StringData(0, 0, TextIn);
            ParenthesisNodes(Root, TextIn);

            var FromsAll = new List<StringData>(from PT in Root.All
                                                where PT.Value.ToUpperInvariant().Contains("FROM")
                                                select PT);
            var FromsWithFroms = new List<StringData>(from FA in FromsAll
                                                      where (from P in FA.Parentheses
                                                             where P.Value.ToUpperInvariant().Contains("FROM")
                                                             select P).Any()
                                                      select FA);
            var FromsNoFroms = new List<StringData>(FromsAll.Except(FromsWithFroms));
            var FromElements = new List<ScriptElement>();
            string FromText = TextIn;
            foreach (var Section in FromsNoFroms)
            {
                FromElements.AddRange(FromBlocks(Section));
                FromText = FromText.Remove(Section.Start, Section.Length);
                FromText = FromText.Insert(Section.Start, new string('■', Section.Length));
            }
            return new KeyValuePair<string, List<ScriptElement>>(FromText, FromElements);
        }
        private List<ScriptElement> FromBlocks(StringData _StringData)
        {
            string From_SectionValue = null;
            List<ScriptElement> FromElements = new List<ScriptElement>();
            var WithList = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var _With in Withs)
            {
                if (!WithList.ContainsKey(_With.Block.Value))
                    WithList.Add(_With.Block.Value, string.Empty);
            }

            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            ///// ***** DUPLICATIONS DUE TO NESTED FROM STATEMENTS...ObjectsFromText.FromsInsideBubbles CALLS FOR EACH FROM IN A BUBBLE
            ///// FUNCTION TAKES A SECTION OF BODY.TEXT AND SEGMENTS TEXT BLOCKS OF FROM...=>|WHERE
            ///// FromBlockPattern IS NON-GREEDY SO NEED TO ITERATE MULTIPLE FROM's UNTIL ALL ARE GONE (EX. UNIONS)
            ///// FROM[^©] = FROM+ANYTHING UP TO A KEY WORD OR EOL... DO NOT USE <BlackOut> AS BUBBLES WILL HAVE BLACKED OUT ANY () IN THE FROM BLOCK
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

            string FromBlockPattern = @"FROM[\s]{1,}[^" + '©' + @"]{1,}?(?=\bWHERE\b|\bUNION\b|\bEXCEPT\b|\bINTERSECT\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|\bFETCH\b|\z)";
            const string FromJoinCommaPattern = @"(?<=FROM |JOIN )[\s]{0,}[A-Z0-9!%{}^~_@#$♥]{1,}([.][A-Z0-9!%{}^~_@#$♥]{1,}){0,2}([\s]{1,}[A-Z0-9!%{}^~_@#$]{1,}){0,1}|(?<=,)[\s]{0,}[A-Z0-9!%{}^~_@#$♥]{1,}([.][A-Z0-9!%{}^~_@#$♥]{1,}){0,2}([\s]{1,}[A-Z0-9!%{}^~_@#$]{1,}){0,1}";

            ///// GET THE FROM CHUNK FROM START TO END INCLUDING ALL JOINS, ETC UP TO BUT NOT INCLUDING WHERE, UNION, ETC
            List<StringData> From_Sections = new List<StringData>(Regex.Matches(_StringData.Value, FromBlockPattern, RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));

            foreach (var From_Section in From_Sections)
            {
                From_SectionValue = From_Section.Value;
                StringData Root = new StringData(0, 0, From_Section.Value);
                ParenthesisNodes(Root, From_SectionValue);
                if (Root.Parentheses.Any())
                {
                    var Base = Root.All.First();
                    From_SectionValue = From_SectionValue.Remove(Base.Start, Base.Length);
                    From_SectionValue = From_SectionValue.Insert(Base.Start, new string('■', Base.Length));
                }
                do
                {
                    ///// FromBlockPattern IS LAZY...DO IS REQUIRED AS Regex.Match SET TO LAZY
                    var FromBlockMatch = Regex.Match(From_SectionValue, FromJoinCommaPattern, RegexOptions.IgnoreCase);
                    if (FromBlockMatch.Success)
                    {
                        var InnerItems = new List<Match>(Regex.Matches(From_SectionValue, FromJoinCommaPattern, RegexOptions.IgnoreCase).OfType<Match>());
                        ///// InnerItems=EACH MATCH OF OTHER REFERENCED TABLES IN THE FROM BLOCK SUCH AS:     C085365.ACTIONS_TODAY AT
                        foreach (var InnerItem in InnerItems)
                        {
                            string InnerChunk = From_SectionValue.Substring(InnerItem.Index, From_SectionValue.Length - InnerItem.Index);
                            int InnerStart = 0;
                            foreach (var Item in InnerItem.Value)
                            {
                                if (Item == ' ')
                                    InnerStart += 1;
                                else
                                    break;
                            }

                            int NewStart = _StringData.Start + From_Section.Start + InnerItem.Index + InnerStart;
                            string InnerValue = InnerItem.Value.Substring(InnerStart, InnerItem.Length - InnerStart).Split(' ').First();
                            ScriptElement.LabelName SourceType;
                            if (WithList.ContainsKey(InnerValue))
                                ///// WITH (a,b) AS (SELECT WILL MATCH IsRoutineTable SO CHECK FIRST
                                SourceType = ScriptElement.LabelName.WithTable;
                            else if (InnerValue.ToUpperInvariant() == "TABLE" & Regex.Match(InnerChunk, "TABLE[■]{2,}", RegexOptions.IgnoreCase).Success)
                                ///// TABLE(SELECT... IS NESTED SO CONTENT OF () *IS* BLACKED OUT
                                SourceType = ScriptElement.LabelName.FloatingTable;
                            else if (Regex.Match(InnerChunk, InnerValue + "[■]{1,}", RegexOptions.IgnoreCase).Success)
                                ///// XMLTABLE( + OTHER ROUTINE TABLES ARE *NOT* NESTED WITH ANOTHER SELECT STATEMENT SO CONTENT OF () IS NOT BLACKED OUT
                                SourceType = ScriptElement.LabelName.RoutineTable;
                            else if (Regex.IsMatch(InnerValue, "([A-Z0-9!%{}^~_@#$]{1,}([.][A-Z0-9!%{}^~_@#$]{1,}))", RegexOptions.IgnoreCase)) {
                                SourceType = ScriptElement.LabelName.SystemTable;
                            }

                            else
                                SourceType = ScriptElement.LabelName.None;

                            FromElements.Add(new ScriptElement(new StringData(_StringData.Start + From_Section.Start, From_Section.Length, From_Section.Value), SourceType));

                            ///////////////// T E S T I N G ///////////////
                            //if (FromElements.Last().Source == ScriptElement.LabelName.SystemTable & FromElements.Last().Name.Contains("INITIAL"))
                            //    Debugger.Break();
                            ///////////////// T E S T I N G ///////////////

                            ///// EACH ITERATION REMOVES A FOUND ITEM AND IS NOT CONSIDERED IN NEXT EVALUATION ///
                            From_SectionValue = From_SectionValue.Remove(InnerItem.Index, InnerItem.Length);
                            From_SectionValue = From_SectionValue.Insert(InnerItem.Index, new string('©', InnerItem.Length));
                        }
                    }
                    else
                        ///// ALL MATCHES HAVE BEEN REPLACED BY <'©'>. NOTHING REMAINS
                        break;
                }
                while (true);
            }
            FromElements.Sort((x, y) => string.Compare(x.Source.ToString().ToUpperInvariant(), y.Source.ToString().ToUpperInvariant(), StringComparison.Ordinal));
            Labels.AddRange(FromElements);
            return FromElements;
        }
        private List<ScriptElement> FieldsFromBlocks(StringData fieldStringData, string Pattern)
        {
            var fieldData = StripComments(fieldStringData.Value);
            var Fields = new List<ScriptElement>();
            ScriptElement.LabelName sourceType = ScriptElement.LabelName.None;
            string FieldPattern = null;
            if (Pattern.Contains("GROUP"))
            {
                sourceType = ScriptElement.LabelName.GroupField;
                FieldPattern = @"\bGROUP[\s]{1,}BY\b[\s]{1,}";
            }
            else if (Pattern.Contains("ORDER"))
            {
                sourceType = ScriptElement.LabelName.OrderField;
                FieldPattern = @"\bORDER[\s]{1,}BY\b[\s]{1,}";
            }
            else if (Pattern.Contains("SELECT"))
            {
                sourceType = ScriptElement.LabelName.SelectField;
                FieldPattern = "SELECT(\\sdistinct){0,1}\\s{1,}";
            }
            string FieldSection = fieldData.Remove(0, Regex.Match(fieldData, FieldPattern, RegexOptions.IgnoreCase).Length);
            string FieldSectionNoParenthesis = FieldSection;
            ///// REMOVE CONTENT INSIDE () SINCE FUNCTIONS, ETC OFTEN CONTAIN COMMAS WHICH IS NEEDED AS A "§" FOR THE FIELD
            var Root = new StringData(0, 0, FieldSection);
            ParenthesisNodes(Root, FieldSection);
            foreach (var Section in Root.Parentheses)
            {
                FieldSectionNoParenthesis = FieldSectionNoParenthesis.Remove(Section.Start, Section.Length);
                FieldSectionNoParenthesis = FieldSectionNoParenthesis.Insert(Section.Start, new string('■', Section.Length));
            }
            var DelimitMatches = new List<StringData>(Regex.Matches(FieldSectionNoParenthesis, ",[ ]{0,}", RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
            foreach (var Section in DelimitMatches)
            {
                FieldSectionNoParenthesis = FieldSectionNoParenthesis.Remove(Section.Start, Section.Length);
                FieldSectionNoParenthesis = FieldSectionNoParenthesis.Insert(Section.Start, new string('½', Section.Length));
            }
            FieldSectionNoParenthesis = Regex.Replace(FieldSectionNoParenthesis, " ", "¾");
            int FieldStart = fieldData.Length - FieldSection.Length;
            var FieldMatches = new List<StringData>(Regex.Matches(FieldSectionNoParenthesis, @"[^½\s®]{1,}", RegexOptions.IgnoreCase).OfType<Match>().Select(m => new StringData(m)));
            foreach (StringData Field in FieldMatches)
            {
                string FieldValue = FieldSection.Substring(Field.Start, Field.Length);
                FieldValue = Regex.Replace(FieldValue, @"[\t\r\n]", "■");
                FieldValue = Regex.Replace(FieldValue, "■$", string.Empty);
                Fields.Add(new ScriptElement(new StringData(fieldStringData.Start + FieldStart + Field.Start, Field.Length, FieldValue), sourceType));
            }
            return Fields;
        }
        private void ParenthesisNodes(StringData StringNode, string TextIn)
        {
            ///// MUST BE DELIMITED BY A CHARACTER WHICH WILL NEVER BE FOUND IN SCRIPT
            var Group = ParenthesisCapture(TextIn);

            ///// GROUP.LENGTH=0 MEANS NO () FOUND IN TextIn
            ///// IF TextIn.LENGTH=0 THEN HAS REACHED EOL

            if (TextIn.Length == 0) { }
            else if (Group.Length == 0)
            {
                ///// NO PARENTHESIS LEFT=SIBLINGS ADDED, NOW ADD CHILDREN
                List<StringData> NewNodes = StringNode.Parentheses;
                foreach (var ChildNode in NewNodes)
                {
                    var TextValues = ChildNode.Value.Split('©');
                    ChildNode.Value = TextValues.First();
                    ParenthesisNodes(ChildNode, TextValues.Last());
                }
            }
            else
            {
                ///// FOUND PARENTHESIS...ADD SIBLINGS BY RECURSING ON TEXT. MUST SUBSTITUTE PARENTHESIS WITH {} OTHERWISE INFINITE LOOP
                string ChildText = "{" + TextIn.Substring(Group.Start + 1, Group.Length - 2) + "}";
                string SiblingText = TextIn.Remove(Group.Start, Group.Length);
                SiblingText = SiblingText.Insert(Group.Start, new string('-', Group.Length));

                string NodeText = string.Join(string.Empty, new string[] { Group.Value, '©'.ToString(), ChildText });
                if (StringNode != null)
                {
                    StringData ParentGroup = StringNode;
                    Group.Start += ParentGroup.Start;
                    StringNode.Parentheses.Add(new StringData(Group.Start, Group.Length, NodeText));
                }
                ParenthesisNodes(StringNode, SiblingText);
            }
        }
        private StringData ParenthesisCapture(string textIn)
        {
            StringData Capture = new StringData();
            var withBlock = Capture;
            textIn = textIn ?? string.Empty;
            if (textIn.Any())
            {
                List<Match> Parentheses = new List<Match>(Regex.Matches(textIn, @"\(|\)", RegexOptions.IgnoreCase).OfType<Match>());
                int LeftCount = 0;
                int RightCount = 0;
                foreach (var Parenthese in Parentheses)
                {
                    if (Parenthese.Value == "(")
                        LeftCount += 1;
                    if (Parenthese.Value == ")")
                        RightCount += 1;
                    if (LeftCount == RightCount)
                    {
                        withBlock.Start = Parentheses.First().Index;
                        withBlock.Length = 1 + Math.Abs(Parenthese.Index - Parentheses.First().Index);
                        withBlock.Value = textIn.Substring(withBlock.Start, withBlock.Length);
                        break;
                    }
                }
            }
            return Capture;
        }

        public override string ToString() => string.Join(",", Tables.Select(t => t.ToString()));
    }
    public class StringData : IEquatable<StringData>
    {
        public int Start { get; set; }
        public int Length { get; set; }
        public int End => Start + Length;
        public string Value { get; set; }
        public bool Contains(StringData sd) => sd.Start >= Start && sd.End <= End;
        public List<StringData> Parentheses { get; }
        public List<StringData> All
        {
            get
            {
                List<StringData> nodes = new List<StringData>();
                Queue<StringData> Queue = new Queue<StringData>();
                StringData topNode;
                foreach (var nd in Parentheses)
                    Queue.Enqueue(nd);
                while (Queue.Any())
                {
                    topNode = Queue.Dequeue();
                    nodes.Add(topNode);
                    foreach (var nd in topNode.Parentheses)
                        Queue.Enqueue(nd);
                }
                return nodes.OrderBy(n => nodes.IndexOf(n)).ToList();
            }
        }

        public StringData(Match match) {

            Parentheses = new List<StringData>();
            Start = match.Index;
            Length = match.Length;
            Value = match.Value;
        }
        public StringData(int start, int length, string value) {
            Parentheses = new List<StringData>();
            Start = start;
            Length = length;
            Value = value;
        }
        public StringData() { }

        public override int GetHashCode() => Start.GetHashCode() ^ Length.GetHashCode() ^ End.GetHashCode() ^ (Value ?? string.Empty).GetHashCode() ^ Parentheses.GetHashCode();
        public bool Equals(StringData other) => Value == other?.Value & Start == other?.Start & Length == other?.Length;
        public static bool operator ==(StringData value1, StringData value2) => value1.Equals(value2);
        public static bool operator !=(StringData value1, StringData value2) => !value1.Equals(value2);
        public override bool Equals(object obj)
        {
            if (obj is StringData data)
                return data == this;
            else
                return false;
        }
        public override string ToString() => $"{Value}●{Start}●{Length}";
    }
    public readonly struct ScriptElement : IEquatable<ScriptElement>
    {
        public enum LabelName { None, WithTable, WithBlock, SystemTable, FloatingTable, RoutineTable, SelectBlock, SelectField, GroupBlock, GroupField, OrderBlock, OrderField, Union, Comment, Constant, Limit }
        public StringData Block { get; }
        public LabelName Source { get; }

        public ScriptElement(StringData block, LabelName source) {
            Block = block;
            Source = source;
        }

        public override int GetHashCode() => Block.GetHashCode() ^ Source.GetHashCode();
        public bool Equals(ScriptElement other) => Block == other.Block & Source == other.Source;
        public static bool operator ==(ScriptElement value1, ScriptElement value2) => value1.Equals(value2);
        public static bool operator !=(ScriptElement value1, ScriptElement value2) => value1 != value2;
        public override bool Equals(object obj)
        {
            if (obj is ScriptElement element)
                return element == this;
            else
                return false;
        }
        public override string ToString() => $"{Source}●{Block}";
    }
    public readonly struct SchemaTable : IEquatable<SchemaTable>
    {
        public SchemaTable(string schemaTable)
        {
            // from c.openacth3 oa
            var parentChildPattern = "([A-Z0-9!%{}^~_@#$]{1,}([.][A-Z0-9!%{}^~_@#$]{1,}))";
            var pc = Regex.Match(schemaTable, parentChildPattern, RegexOptions.IgnoreCase);
            if (pc.Success)
            {
                var parentChild = pc.Value.Split('.');
                Schema = parentChild[0].ToUpperInvariant();
                Table = parentChild[1].ToUpperInvariant();
            }
            else {
                Schema = string.Empty; Table = string.Empty;
            }
        }
        public SchemaTable(string schema, string table)
        {
            Schema = schema.ToUpperInvariant();
            Table = table.ToUpperInvariant();
        }
        public string Schema { get; }
        public string Table { get; }
        public override int GetHashCode() => (Schema ?? string.Empty).GetHashCode() ^ (Table ?? string.Empty).GetHashCode();
        public bool Equals(SchemaTable other) => other == null ? this == null : Schema == other.Schema & Table == other.Table;
        public static bool operator ==(SchemaTable value1, SchemaTable value2)
        {
            if (value1 == null)
                return value2 == null;
            else if (value2 == null)
                return value1 == null;
            else
                return value1.Equals(value2);
        }
        public static bool operator !=(SchemaTable value1, SchemaTable value2) => value1 != value2;
        public override bool Equals(object obj) => obj is SchemaTable @object && @object == this;

        public override string ToString() => $"{Schema}.{Table}";
    }
}