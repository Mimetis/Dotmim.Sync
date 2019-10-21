timestamps {
    node {
        def suffix = "tsu-${BUILD_NUMBER}"
        stage('Clone repository') {
            checkout scm
        }    
        docker.image('microsoft/dotnet:2.1.300-sdk').inside() {
            stage('Restore Dependencies') {
                sh 'dotnet restore'
            }
            stage('Build') {
                sh "dotnet build Projects/Dotmim.Sync.Core/Dotmim.Sync.Core.csproj -c Release -f netstandard2.0 -o ${WORKSPACE}/${BUILD_NUMBER}/Dotmim.Sync.Core --version-suffix ${suffix}"
                sh "dotnet build Projects/Dotmim.Sync.SqlServer/Dotmim.Sync.SqlServer.csproj -c Release -f netstandard2.0 -o ${WORKSPACE}/${BUILD_NUMBER}/Dotmim.Sync.SqlServer --version-suffix ${suffix}"
                sh "dotnet build Projects/Dotmim.Sync.Sqlite/Dotmim.Sync.Sqlite.csproj -c Release -f netstandard2.0 -o ${WORKSPACE}/${BUILD_NUMBER}/Dotmim.Sync.Sqlite --version-suffix ${suffix}"
                sh "dotnet build Projects/Dotmim.Sync.MySql/Dotmim.Sync.MySql.csproj -c Release -f netstandard2.0 -o ${WORKSPACE}/${BUILD_NUMBER}/Dotmim.Sync.MySql --version-suffix ${suffix}"
                sh "dotnet build Projects/Dotmim.Sync.Web.Client/Dotmim.Sync.Web.Client.csproj -c Release -f netstandard2.0 -o ${WORKSPACE}/${BUILD_NUMBER}/Dotmim.Sync.Web.Client --version-suffix ${suffix}"
                sh "dotnet build Projects/Dotmim.Sync.Web.Server/Dotmim.Sync.Web.Server.csproj -c Release -f netstandard2.0 -o ${WORKSPACE}/${BUILD_NUMBER}/Dotmim.Sync.Web.Server --version-suffix ${suffix}"
                sh "dotnet build Projects/dotnet-sync/dotnet-sync.csproj -c Release -o ${WORKSPACE}/${BUILD_TAG}/dotnet-sync --version-suffix ${suffix}"
            }
            stage('Push to Baget') {
                pushPackage("${WORKSPACE}/${BUILD_NUMBER}", "Dotmim.Sync.Core")
                pushPackage("${WORKSPACE}/${BUILD_NUMBER}", "Dotmim.Sync.SqlServer")
                pushPackage("${WORKSPACE}/${BUILD_NUMBER}", "Dotmim.Sync.Sqlite")
                pushPackage("${WORKSPACE}/${BUILD_NUMBER}", "Dotmim.Sync.MySql")
                pushPackage("${WORKSPACE}/${BUILD_NUMBER}", "Dotmim.Sync.Web.Client")
                pushPackage("${WORKSPACE}/${BUILD_NUMBER}", "Dotmim.Sync.Web.Server")
            }
        }
    }
}

def String pushPackage(String path, String packageName) {
    def file = getPackageFile(path, packageName)
    sh "dotnet nuget push ${file} -s http://10.19.11.17:5500/v3/index.json -k NUGET-SERVER-API-KEY"
} 

def String getPackageFile(String path, String packageName) {
    def file = sh returnStdout: true, script: "find ${path}/${packageName} -name ${packageName}*.nupkg -not -name *.symbols.nupkg -printf '%p'"
    return file
}
