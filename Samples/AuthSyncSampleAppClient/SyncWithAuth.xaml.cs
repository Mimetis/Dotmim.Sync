using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.Web;
using Microsoft.Identity.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

// The Blank Page item template is documented at https://go.microsoft.com/fwlink/?LinkId=234238

namespace AuthSyncSampleAppClient
{
    /// <summary>
    /// An empty page that can be used on its own or navigated to within a Frame.
    /// </summary>
    public sealed partial class SyncWithAuth : Page
    {

        // Authentication sample config
        // -------------------------------------------------------------------
        // App is registered in https://apps.dev.microsoft.com
        // -------------------------------------------------------------------
        private string ClientId = "99362e01-d41a-4370-95ce-db9e1d51796f"; // AAD V2
        private PublicClientApplication publicClientApp;
        private string[] scopes = new string[] {
            "api://99362e01-d41a-4370-95ce-db9e1d51796f/access_as_user",
            "api://99362e01-d41a-4370-95ce-db9e1d51796f/access_admin"
        };
        private AuthenticationResult authenticationResult = null;

        public SyncWithAuth()
        {
            this.InitializeComponent();
            publicClientApp = new PublicClientApplication(ClientId);
        }

        private void Debug(string str)
        {
            LstResults.Items.Add(str);

        }


        private void ResetButton_Click(object sender, RoutedEventArgs e)
        {
            LstResults.Items.Clear();
        }

        private async void LoginButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var user = publicClientApp.Users.FirstOrDefault();
                authenticationResult = await publicClientApp.AcquireTokenSilentAsync(scopes, user);
                Debug("We have a silent token for " + user.Name);
                Debug(authenticationResult.AccessToken);
            }
            catch (MsalUiRequiredException)
            {
                Debug("Can't login silently, try to launch an auth popup.");
                try
                {
                    authenticationResult = await publicClientApp.AcquireTokenAsync(scopes);
                    Debug("We have a token");
                    Debug(authenticationResult.AccessToken);
                }
                catch (MsalException msalex)
                {
                    Debug($"Error Acquiring Token");
                    Debug(msalex.Message);
                }
            }
            catch (Exception ex)
            {
                Debug($"Fatal Error ");
                Debug(ex.Message);
            }
        }

        private void LogoutButton_Click(object sender, RoutedEventArgs e)
        {
            if (publicClientApp.Users.Any())
            {
                try
                {
                    publicClientApp.Remove(publicClientApp.Users.FirstOrDefault());
                    Debug("User has signed-out");
                }
                catch (MsalException ex)
                {
                    Debug($"Error signing-out user: {ex.Message}");
                }
            }
        }

        private async void SyncButton_Click(object sender, RoutedEventArgs e)
        {
            var clientProvider = new SqliteSyncProvider("employees.db");

            var proxyClientProvider = new WebProxyClientProvider(
                new Uri("http://localhost:58507/api/authsync"));

            // adding bearer auth
            if (authenticationResult != null && authenticationResult.AccessToken != null)
                proxyClientProvider.AddCustomHeader("Authorization", authenticationResult.CreateAuthorizationHeader());

            var agent = new SyncAgent(clientProvider, proxyClientProvider);

            agent.SyncProgress += (s, a) => Debug(a.Message + a.PropertiesMessage);
            try
            {
                var r = await agent.SynchronizeAsync();
                Debug("TotalChangesDownloaded: " + r.TotalChangesDownloaded);
            }
            catch (Exception ex)
            {

                Debug("Error during sync " + ex.Message);
            }


        }

    }
}
