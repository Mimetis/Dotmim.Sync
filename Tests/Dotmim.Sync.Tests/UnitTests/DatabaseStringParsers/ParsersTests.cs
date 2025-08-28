using Dotmim.Sync.DatabaseStringParsers;
using Xunit;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class ParsersTests
    {

        [Fact]
        public void TableParser_Parse_OnlyOneLetter_ShouldWork()
        {
            var tableParser = new TableParser("D", '[', ']');

            Assert.Equal("D", tableParser.TableName);
            Assert.Equal("[D]", tableParser.QuotedShortName);
            Assert.Equal("[D]", tableParser.QuotedFullName);
            Assert.Equal("D", tableParser.NormalizedShortName);
            Assert.Equal("D", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Parse_OnlyOneLetter_NoQuotes_ShouldWork()
        {
            var tableParser = new TableParser("D");

            Assert.Equal("D", tableParser.TableName);
            Assert.Equal("[D]", tableParser.QuotedShortName);
            Assert.Equal("[D]", tableParser.QuotedFullName);
            Assert.Equal("D", tableParser.NormalizedShortName);
            Assert.Equal("D", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Parse_NoLetter_NoQuotes_ShouldWork()
        {
            var tableParser = new TableParser("");

            Assert.Equal("", tableParser.TableName);
            Assert.Equal("[]", tableParser.QuotedShortName);
            Assert.Equal("[]", tableParser.QuotedFullName);
            Assert.Equal("", tableParser.NormalizedShortName);
            Assert.Equal("", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Parse_OneSpace_NoQuotes_ShouldWork()
        {
            var tableParser = new TableParser(" ");

            Assert.Equal("", tableParser.TableName);
            Assert.Equal("[]", tableParser.QuotedShortName);
            Assert.Equal("[]", tableParser.QuotedFullName);
            Assert.Equal("", tableParser.NormalizedShortName);
            Assert.Equal("", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Parse_OneDot_NoQuotes_ShouldWork()
        {
            var tableParser = new TableParser(".");

            Assert.Equal("", tableParser.TableName);
            Assert.Equal("[]", tableParser.QuotedShortName);
            Assert.Equal("[]", tableParser.QuotedFullName);
            Assert.Equal("", tableParser.NormalizedShortName);
            Assert.Equal("", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Parse_OneLetterAsQuote_ShouldWork()
        {
            var tableParser = new TableParser("[");

            Assert.Equal("", tableParser.TableName);
            Assert.Equal("[]", tableParser.QuotedShortName);
            Assert.Equal("[]", tableParser.QuotedFullName);
            Assert.Equal("", tableParser.NormalizedShortName);
            Assert.Equal("", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Parse_OneLetterAsQuote_WithQuote_ShouldWork()
        {
            var tableParser = new TableParser("|", '|', '|');

            Assert.Equal("", tableParser.TableName);
            Assert.Equal("||", tableParser.QuotedShortName);
            Assert.Equal("||", tableParser.QuotedFullName);
            Assert.Equal("", tableParser.NormalizedShortName);
            Assert.Equal("", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Parse_ShouldWork()
        {
            var tableParser = new TableParser("dbo.Customer", '[', ']');

            Assert.Equal("Customer", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("[Customer]", tableParser.QuotedShortName);
            Assert.Equal("[dbo].[Customer]", tableParser.QuotedFullName);
            Assert.Equal("Customer", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Customer", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Quoted_Parse_ShouldWork()
        {
            var tableParser = new TableParser("[dbo].[Customer]", '[', ']');

            Assert.Equal("Customer", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("[Customer]", tableParser.QuotedShortName);
            Assert.Equal("[dbo].[Customer]", tableParser.QuotedFullName);
            Assert.Equal("Customer", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Customer", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Special_Quoted_Parse_ShouldWork()
        {
            var tableParser = new TableParser("`dbo`.`Customer`", '`', '`');

            Assert.Equal("Customer", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("`Customer`", tableParser.QuotedShortName);
            Assert.Equal("`dbo`.`Customer`", tableParser.QuotedFullName);
            Assert.Equal("Customer", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Customer", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_DoubleQuoted_Parse_ShouldWork()
        {
            var tableParser = new TableParser("\"dbo\".\"Customer\"", '\"', '\"');

            Assert.Equal("Customer", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("\"Customer\"", tableParser.QuotedShortName);
            Assert.Equal("\"dbo\".\"Customer\"", tableParser.QuotedFullName);
            Assert.Equal("Customer", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Customer", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Special_MultiQuoted_Parse_ShouldWork()
        {
            var tableParser = new TableParser("[dbo].`Customer`", ['`', '['], ['`', ']']);

            Assert.Equal("Customer", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("[Customer]", tableParser.QuotedShortName);
            Assert.Equal("[dbo].[Customer]", tableParser.QuotedFullName);
            Assert.Equal("Customer", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Customer", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_Special_MultiQuoted2_Parse_ShouldWork()
        {
            var tableParser = new TableParser("`dbo`.[Customer]", ['[', '`'], ['`', ']']);

            Assert.Equal("Customer", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("`Customer`", tableParser.QuotedShortName);
            Assert.Equal("`dbo`.`Customer`", tableParser.QuotedFullName);
            Assert.Equal("Customer", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Customer", tableParser.NormalizedFullName);
        }


        [Fact]
        public void TableParser_Parse_WhiteSpaces_ShouldWork()
        {
            var tableParser = new TableParser("dbo.Orders Lines", '[', ']');

            Assert.Equal("Orders Lines", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("[Orders Lines]", tableParser.QuotedShortName);
            Assert.Equal("[dbo].[Orders Lines]", tableParser.QuotedFullName);
            Assert.Equal("Orders_Lines", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Orders_Lines", tableParser.NormalizedFullName);
        }


        [Fact]
        public void TableParser_Parse_Quoted_WhiteSpaces_ShouldWork()
        {
            var tableParser = new TableParser("dbo.[Orders Lines]", '[', ']');

            Assert.Equal("Orders Lines", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("[Orders Lines]", tableParser.QuotedShortName);
            Assert.Equal("[dbo].[Orders Lines]", tableParser.QuotedFullName);
            Assert.Equal("Orders_Lines", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Orders_Lines", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_PartialQuotes_Parse_WhiteSpaces_ShouldWork()
        {
            var tableParser = new TableParser("[dbo.Orders Lines]", '[', ']');

            Assert.Equal("Orders Lines", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("[Orders Lines]", tableParser.QuotedShortName);
            Assert.Equal("[dbo].[Orders Lines]", tableParser.QuotedFullName);
            Assert.Equal("Orders_Lines", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Orders_Lines", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_MultiQuotes_ShouldWork()
        {
            var tableParser = new TableParser("[dbo].Orders Lines`", ['[', '`'], [']', '`']);

            Assert.Equal("Orders Lines", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("[Orders Lines]", tableParser.QuotedShortName);
            Assert.Equal("[dbo].[Orders Lines]", tableParser.QuotedFullName);
            Assert.Equal("Orders_Lines", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Orders_Lines", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_MultiQuotes2_ShouldWork()
        {
            var tableParser = new TableParser("dbo.Orders Lines", ['[', '`'], [']', '`']);

            Assert.Equal("Orders Lines", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("[Orders Lines]", tableParser.QuotedShortName);
            Assert.Equal("[dbo].[Orders Lines]", tableParser.QuotedFullName);
            Assert.Equal("Orders_Lines", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Orders_Lines", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_MultiQuotes3_ShouldWork()
        {
            var tableParser = new TableParser("`dbo`.Orders Lines", ['[', '`'], [']', '`']);

            Assert.Equal("Orders Lines", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("`Orders Lines`", tableParser.QuotedShortName);
            Assert.Equal("`dbo`.`Orders Lines`", tableParser.QuotedFullName);
            Assert.Equal("Orders_Lines", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Orders_Lines", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_MultiQuotesWithDoubleQuotes_ShouldWork()
        {
            var tableParser = new TableParser("`dbo`.[Orders Lines\"`]", ['[', '`', '"'], [']', '`', '"']);

            Assert.Equal("Orders Lines", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("`Orders Lines`", tableParser.QuotedShortName);
            Assert.Equal("`dbo`.`Orders Lines`", tableParser.QuotedFullName);
            Assert.Equal("Orders_Lines", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Orders_Lines", tableParser.NormalizedFullName);
        }

        [Fact]
        public void TableParser_NoArgs_ShouldWork()
        {
            var tableParser = new TableParser("`dbo`.[Orders Lines\"`]");

            Assert.Equal("Orders Lines", tableParser.TableName);
            Assert.Equal("dbo", tableParser.SchemaName);
            Assert.Equal("`Orders Lines`", tableParser.QuotedShortName);
            Assert.Equal("`dbo`.`Orders Lines`", tableParser.QuotedFullName);
            Assert.Equal("Orders_Lines", tableParser.NormalizedShortName);
            Assert.Equal("dbo_Orders_Lines", tableParser.NormalizedFullName);
        }


        [Fact]
        public void ColumnParser_Parse_ShouldWork()
        {
            var columnParser = new ObjectParser("Customer.ID", '[', ']');

            Assert.Equal("ID", columnParser.ObjectName);
            Assert.Equal("Customer", columnParser.OwnerName);
            Assert.Equal("[ID]", columnParser.QuotedShortName);
            Assert.Equal("ID", columnParser.NormalizedShortName);
        }

        [Fact]
        public void ColumnParser_QuoteParse_ShouldWork()
        {
            var columnParser = new ObjectParser("[Customer].[ID]", '[', ']');

            Assert.Equal("ID", columnParser.ObjectName);
            Assert.Equal("Customer", columnParser.OwnerName);
            Assert.Equal("[ID]", columnParser.QuotedShortName);
            Assert.Equal("ID", columnParser.NormalizedShortName);
        }

        [Fact]
        public void ColumnParser_Parse_ColumnOnly_ShouldWork()
        {
            var columnParser = new ObjectParser("ID", '[', ']');

            Assert.Equal("ID", columnParser.ObjectName);
            Assert.Empty(columnParser.OwnerName);
        }

        [Fact]
        public void ColumnParser_Parse_WhiteSpaces_ShouldWork()
        {
            var columnParser = new ObjectParser("Attribute With Space", '[', ']');

            Assert.Equal("Attribute With Space", columnParser.ObjectName);
            Assert.Equal("", columnParser.OwnerName);
            Assert.Equal("[Attribute With Space]", columnParser.QuotedShortName);
            Assert.Equal("Attribute_With_Space", columnParser.NormalizedShortName);
        }

        [Fact]
        public void TableParser_Parse_TableName_WithADot_WithoutSchema()
        {
            var tableParser = new TableParser("Customer.", '[', ']');

            Assert.Equal("Customer", tableParser.TableName);
            Assert.Equal("", tableParser.SchemaName);
            Assert.Equal("[Customer]", tableParser.QuotedShortName);
            Assert.Equal("[Customer]", tableParser.QuotedFullName);
            Assert.Equal("Customer", tableParser.NormalizedShortName);
            Assert.Equal("Customer", tableParser.NormalizedFullName);
        }

        [Fact]
        public void ColumnParser_Parse_WhiteSpecials_Characters_ShouldWork()
        {
            var columnParser = new ObjectParser("[Date Empl. Après L'Entête]", '[', ']');

            Assert.Equal("Date Empl. Après L'Entête", columnParser.ObjectName);
            Assert.Equal("", columnParser.OwnerName);
            Assert.Equal("[Date Empl. Après L'Entête]", columnParser.QuotedShortName);
            Assert.Equal("Date_Empl__Apres_L_Entete", columnParser.NormalizedShortName);
        }



    }
}
