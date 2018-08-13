# install chocolatey
Set-ExecutionPolicy Bypass -Scope Process -Force; iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))

#install mysql
cinst mysql -y

#set initial root user password to be the one used by dotmim sync
mysqladmin -u root password Password12!