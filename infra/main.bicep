@description('Primary location for all resources')
param location string = resourceGroup().location

@allowed([
  'dev'
  'test'
  'prod'
])
param environment string = 'dev'

@maxLength(8)
param appPrefix string = 'fintech'

param synapseSqlAdminLogin string = 'sqladminuser'

@secure()
param synapseSqlAdminPassword string

@allowed([
  'DW100c'
  'DW200c'
  'DW300c'
])
param sqlPoolSku string = 'DW100c'

param resourceTags object = {
  Environment: environment
  Project: 'FinTech-Medallion-Pipeline'
  ManagedBy: 'Bicep'
}

// ---------------- Variables ----------------
var uniqueSuffix = uniqueString(resourceGroup().id)
var storageAccountName = 'fintechlakedevtj3oe5' // CLI se bana hua storage
var keyVaultName = '${appPrefix}-kv-${environment}-${take(uniqueSuffix, 5)}'
var dataFactoryName = '${appPrefix}-adf-${environment}-${uniqueSuffix}'
var databricksWorkspaceName = '${appPrefix}-dbx-${environment}-${uniqueSuffix}'
var synapseWorkspaceName = '${appPrefix}-syn-${environment}-${uniqueSuffix}'
var synapseSqlPoolName = 'gold_dw'
var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

// ---------------- 1. Storage (already created via CLI) ----------------
resource dataLake 'Microsoft.Storage/storageAccounts@2023-01-01' existing = {
  name: storageAccountName
}

// ---------------- 2. Key Vault ----------------
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: resourceTags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enabledForDeployment: true
    enabledForTemplateDeployment: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
  }
}

resource piiSaltSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  parent: keyVault
  name: 'PII-HASH-SALT'
  properties: {
    value: 'SaltKey-${uniqueSuffix}-2026'
  }
}

// ---------------- 3. Data Factory ----------------
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  tags: resourceTags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    publicNetworkAccess: 'Enabled'
  }
}

// ---------------- 4. Databricks ----------------
resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: databricksWorkspaceName
  location: location
  tags: resourceTags
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', '${databricksWorkspaceName}-managed-rg')
  }
}

// ---------------- 5. Synapse + Dedicated SQL Pool ----------------
resource synapseWorkspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: synapseWorkspaceName
  location: location
  tags: resourceTags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    defaultDataLakeStorage: {
      accountUrl: dataLake.properties.primaryEndpoints.dfs
      filesystem: 'synapse-workspace'
    }
    sqlAdministratorLogin: synapseSqlAdminLogin
    sqlAdministratorLoginPassword: synapseSqlAdminPassword
  }
}

resource synapseFirewallRule 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = {
  parent: synapseWorkspace
  name: 'AllowAllWindowsAzureIps'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

resource dedicatedSqlPool 'Microsoft.Synapse/workspaces/sqlPools@2021-06-01' = {
  parent: synapseWorkspace
  name: synapseSqlPoolName
  location: location
  tags: resourceTags
  sku: {
    name: sqlPoolSku
  }
  properties: {
    collation: 'SQL_Latin1_General_CP1_CI_AS'
    createMode: 'Default'
  }
}

// ---------------- 6. RBAC ----------------
resource adfStorageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(dataLake.id, dataFactory.id, storageBlobDataContributorRoleId)
  scope: dataLake
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource synapseStorageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(dataLake.id, synapseWorkspace.id, storageBlobDataContributorRoleId)
  scope: dataLake
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalId: synapseWorkspace.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// ---------------- Outputs ----------------
output storageAccountName string = dataLake.name
output storageDfsEndpoint string = dataLake.properties.primaryEndpoints.dfs
output keyVaultName string = keyVault.name
output dataFactoryName string = dataFactory.name
output databricksWorkspaceUrl string = databricksWorkspace.properties.workspaceUrl
output synapseWorkspaceName string = synapseWorkspace.name
output synapseDedicatedSqlPoolName string = dedicatedSqlPool.name
output synapseSqlEndpoint string = '${synapseWorkspace.name}.sql.azuresynapse.net'
