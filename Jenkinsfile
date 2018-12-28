timestamps {
	node {
	    def ver = "0.3.0"
		stage('Clone repository') {
			checkout scm
		}    
		docker.image('microsoft/dotnet:2.1.300-sdk').inside() {
			stage('Restore Dependencies') {
				sh 'dotnet restore'
			}
			stage('Build') {
				sh 'dotnet build Projects/Dotmim.Sync.Core/Dotmim.Sync.Core.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.Core --version-suffix $(BUILD_TAG) /p:Version=${ver}'
				//sh 'dotnet build Projects/Dotmim.Sync.SqlServer/Dotmim.Sync.SqlServer.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.SqlServer --version-suffix $(BUILD_TAG)'
				//sh 'dotnet build Projects/Dotmim.Sync.Sqlite/Dotmim.Sync.Sqlite.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.Sqlite --version-suffix $(BUILD_TAG)'
				//sh 'dotnet build Projects/Dotmim.Sync.MySql/Dotmim.Sync.MySql.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.MySql --version-suffix $(BUILD_TAG)'
				//sh 'dotnet build Projects/Dotmim.Sync.Web.Client/Dotmim.Sync.Web.Client.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.Web.Client --version-suffix $(BUILD_TAG)'
				//sh 'dotnet build Projects/Dotmim.Sync.Web.Server/Dotmim.Sync.Web.Server.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.Web.Server --version-suffix $(BUILD_TAG)'
				//sh 'dotnet build Projects/dotnet-sync/dotnet-sync.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/dotnet-sync --version-suffix $(BUILD_TAG)'
			}
			stage('Publish') {
				sh 'dotnet publish Projects/Dotmim.Sync.Core/Dotmim.Sync.Core.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.Core --version-suffix $(BUILD_TAG) /p:Version=${ver}'
				//sh 'dotnet publish Projects/Dotmim.Sync.SqlServer/Dotmim.Sync.SqlServer.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.SqlServer --version-suffix $(BUILD_TAG)'
				//sh 'dotnet publish Projects/Dotmim.Sync.Sqlite/Dotmim.Sync.Sqlite.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.Sqlite --version-suffix $(BUILD_TAG)'
				//sh 'dotnet publish Projects/Dotmim.Sync.MySql/Dotmim.Sync.MySql.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.MySql --version-suffix $(BUILD_TAG)'
				//sh 'dotnet publish Projects/Dotmim.Sync.Web.Client/Dotmim.Sync.Web.Client.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.Web.Client --version-suffix $(BUILD_TAG)'
				//sh 'dotnet publish Projects/Dotmim.Sync.Web.Server/Dotmim.Sync.Web.Server.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.Web.Server --version-suffix $(BUILD_TAG)'
				//sh 'dotnet publish Projects/dotnet-sync/dotnet-sync.csproj -c Release -f netstandard2.0 out -o ${WORKSPACE}/${BUILD_TAG}/dotnet-sync --version-suffix $(BUILD_TAG)'
			}
			stage('Push to Baget') {
				//sh 'dotnet nuget push ${WORKSPACE}/${BUILD_TAG}/Dotmim.Sync.Core/Dotmim.Sync.Core.${ver}{BUILD_TAG}.nupkg -s http://mgtoverlord01:5500/v3/index.json -k NUGET-SERVER-API-KEY'
			}
		}
	}
}