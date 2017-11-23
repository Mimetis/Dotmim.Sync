using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UWPSyncSample.Helpers;
using Windows.UI.Popups;
using Windows.UI.Xaml;

namespace UWPSyncSample.ViewModels
{
    public class SettingsViewModel : BaseViewModel
    {
        private readonly SettingsHelper settingsHelper;

        public SettingsViewModel(SettingsHelper settingsHelper)
        {
            this.settingsHelper = settingsHelper;
        }

        public String Server_SqlServer
        {
            get
            {
                return this.settingsHelper[ConnectionType.Server_SqlServer];
            }
            set
            {
                if (value != this.settingsHelper[ConnectionType.Server_SqlServer])
                {
                    this.settingsHelper[ConnectionType.Server_SqlServer] = value;
                    RaisePropertyChanged(nameof(Server_SqlServer));
                }
            }
        }


        public String Client_SqlServer
        {
            get
            {
                return this.settingsHelper[ConnectionType.Client_SqlServer];
            }
            set
            {
                if (value != this.settingsHelper[ConnectionType.Client_SqlServer])
                {
                    this.settingsHelper[ConnectionType.Client_SqlServer] = value;
                    RaisePropertyChanged(nameof(Client_SqlServer));
                }
            }
        }

        public String Client_Sqlite
        {
            get
            {
                return this.settingsHelper[ConnectionType.Client_Sqlite];
            }
            set
            {
                if (value != this.settingsHelper[ConnectionType.Client_Sqlite])
                {
                    this.settingsHelper[ConnectionType.Client_Sqlite] = value;
                    RaisePropertyChanged(nameof(Client_Sqlite));
                }
            }
        }

        public String Client_MySql
        {
            get
            {
                return this.settingsHelper[ConnectionType.Client_MySql];
            }
            set
            {
                if (value != this.settingsHelper[ConnectionType.Client_MySql])
                {
                    this.settingsHelper[ConnectionType.Client_MySql] = value;
                    RaisePropertyChanged(nameof(Client_MySql));
                }
            }
        }

        public String Client_Http_SqlServer
        {
            get
            {
                return this.settingsHelper[ConnectionType.Client_Http_SqlServer];
            }
            set
            {
                if (value != this.settingsHelper[ConnectionType.Client_Http_SqlServer])
                {
                    this.settingsHelper[ConnectionType.Client_Http_SqlServer] = value;
                    RaisePropertyChanged(nameof(Client_Http_SqlServer));
                }
            }
        }
        public String Client_Http_Sqlite
        {
            get
            {
                return this.settingsHelper[ConnectionType.Client_Http_Sqlite];
            }
            set
            {
                if (value != this.settingsHelper[ConnectionType.Client_Http_Sqlite])
                {
                    this.settingsHelper[ConnectionType.Client_Http_Sqlite] = value;
                    RaisePropertyChanged(nameof(Client_Http_Sqlite));
                }
            }
        }

        public String Client_Http_MySql
        {
            get
            {
                return this.settingsHelper[ConnectionType.Client_Http_MySql];
            }
            set
            {
                if (value != this.settingsHelper[ConnectionType.Client_Http_MySql])
                {
                    this.settingsHelper[ConnectionType.Client_Http_MySql] = value;
                    RaisePropertyChanged(nameof(Client_Http_MySql));
                }
            }
        }
        public String WebProxy
        {
            get
            {
                return this.settingsHelper[ConnectionType.WebProxy];
            }
            set
            {
                if (value != this.settingsHelper[ConnectionType.WebProxy])
                {
                    this.settingsHelper[ConnectionType.WebProxy] = value;
                    RaisePropertyChanged(nameof(WebProxy));
                }
            }
        }


        internal async void SaveClick(object sender, RoutedEventArgs e)
        {
            settingsHelper.Save();
            MessageDialog cd = new MessageDialog("Connections strings saved to local settings.");
            await cd.ShowAsync();
        }
    }
}
