using Dotmim.Sync.FX.Tests.Misc;
using Dotmim.Sync.FX.Tests.SqlUtils;
using Dotmim.Sync.MySql;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.FX.Tests.MySql
{
    public class WordPressFixture : IDisposable
    {
        private HelperDB helperDb = new HelperDB();
        private string serverDbName = "mysqldatabase165";
        private string clientDbName = "mysqldatabase165client";

        public string[] Tables => new string[] { "wp_users", "wp_usermeta", "wp_terms", "wp_termmeta", "wp_term_taxonomy",
                                        "wp_term_relationships", "wp_posts", "wp_postmeta", "wp_options", "wp_links",
                                        "wp_comments", "wp_commentmeta"};


        public String ServerConnectionString => HelperDB.GetMySqlDatabaseConnectionString(serverDbName);
        public String ClientMySqlConnectionString => HelperDB.GetMySqlDatabaseConnectionString(clientDbName);

        public SyncAgent Agent { get; set; }


        public WordPressFixture()
        {
            // create client database
            helperDb.DropMySqlDatabase(clientDbName);
            helperDb.CreateMySqlDatabase(clientDbName);

            helperDb.DropMySqlDatabase(serverDbName);
            // restore server database
            var wordpressscript = Path.Combine(Directory.GetCurrentDirectory(), "Backup", "Wordpress.sql");
            var fs = File.OpenText(wordpressscript);
            var script = fs.ReadToEnd();
            helperDb.ExecuteMySqlScript("sys", script);

            var serverProvider = new MySqlSyncProvider(ServerConnectionString);
            var clientProvider = new MySqlSyncProvider(ClientMySqlConnectionString);
            var simpleConfiguration = new SyncConfiguration(Tables);

            Agent = new SyncAgent(clientProvider, serverProvider, simpleConfiguration);

        }
        public void Dispose()
        {
            helperDb.DeleteDatabase(serverDbName);
            helperDb.DropMySqlDatabase(clientDbName);

        }
    }

    [TestCaseOrderer("Dotmim.Sync.FX.Tests.Misc.PriorityOrderer", "Dotmim.Sync.FX.Tests")]
    public class WordPressTests : IClassFixture<WordPressFixture>
    {
        WordPressFixture fixture;
        SyncAgent agent;

        public WordPressTests(WordPressFixture fixture)
        {
            this.fixture = fixture;
            this.agent = fixture.Agent;
        }

        [Fact, TestPriority(1)]
        public async Task Initialize()
        {
            var session = await agent.SynchronizeAsync();

            Assert.Equal(141, session.TotalChangesDownloaded);
            Assert.Equal(0, session.TotalChangesUploaded);
        }

    }
}
