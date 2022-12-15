@description('The name of you Virtual Machine.')
param linuxVmName string = 'az-cslabs-ph2-linux-vm1'

@description('Name of the virtual machine.')
param windowsVmName string = 'azcslabsph2win1'

@description('Name of the virual network.')
param virtualNetworkName string = 'az-cslabs-ph2-vnet'

@description('Name of the Network Security Group')
param linuxVmNsgName string = 'az-cslabs-ph2-linux-secgrp'

@description('Name of the Network Security Group')
param windowsVmNsgName string = 'az-cslabs-ph2-win-secgrp'

@description('Username for the Virtual Machine.')
param linuxVmAdminUsername string = 'linuxAdminUser'

@description('Username for the Virtual Machine.')
param windowsVmAdminUsername string = 'windowsVmAdminUsername'

@description('Type of authentication to use on the Virtual Machine. SSH key is recommended.')
@allowed([
  'sshPublicKey'
  'password'
])
param authenticationType string = 'password'

@description('SSH Key or password for the Virtual Machine. SSH key is recommended.')
@secure()
param linuxVmAdminPasswordOrKey string

@description('Unique DNS Name for the Public IP used to access the Virtual Machine.')
param linuxAdminDnsLabelPrefix string = toLower('${linuxVmName}-${uniqueString(resourceGroup().id)}')

@description('The Ubuntu version for the VM. This will pick a fully patched image of this given Ubuntu version.')
param ubuntuOSVersion string = '20_04-lts-gen2'

@description('Location for all resources.')
param location string = resourceGroup().location

@description('The size of the VM')
param linuxVmSize string = 'Standard_B2s'


@description('Password for the Virtual Machine.')
@minLength(12)
@secure()
param windowsVmAdminPassword string

@description('Unique DNS Name for the Public IP used to access the Virtual Machine.')
param windowsVmDnsLabelPrefix string = toLower('${windowsVmName}-${uniqueString(resourceGroup().id, windowsVmName)}')

@description('Allocation method for the Public IP used to access the Virtual Machine.')
@allowed([
  'Dynamic'
  'Static'
])
param windowsVmPublicIPAllocationMethod string = 'Dynamic'

@description('SKU for the Public IP used to access the Virtual Machine.')
@allowed([
  'Basic'
  'Standard'
])
param windowsVmPublicIpSku string = 'Basic'

@description('The Windows version for the VM. This will pick a fully patched image of this given Windows version.')
@allowed([
'2022-datacenter-azure-edition'
])
param windowsVmOSVersion string = '2022-datacenter-azure-edition'

@description('Size of the virtual machine.')
param windowsVmSize string = 'Standard_D2s_v5'

@description('Blob URI')
param blobUri string = ''

var vnetAddressPrefix = '10.1.0.0/16'
var linuxVmSubnetAddressPrefix = '10.1.0.0/24'
var windowsVmSubnetAddressPrefix = '10.1.1.0/24'

var linuxVmOsDiskType = 'Standard_LRS'
var linuxVmPublicIPAddressName = '${linuxVmName}PublicIP'
var linuxVmNetworkInterfaceName = '${linuxVmName}NetInt'
var linuxVmImageName = '${linuxVmName}Image'
var linuxVmSubnetName = 'linuxVmSubnet'

var windowVmOsDiskType = 'StandardSSD_LRS'
var windowsVmPublicIPAddressName = '${windowsVmName}PublicIP'
var windowsVmNicName = '${windowsVmName}NetInt'
var windowsVmSubnetName = 'windowsVmSubnet'
var windowsVmStorageAccountName = 'bootdiags${uniqueString(resourceGroup().id)}'


var linuxVmConfiguration = {
  disablePasswordAuthentication: true
  ssh: {
    publicKeys: [
      {
        path: '/home/${linuxVmAdminUsername}/.ssh/authorized_keys'
        keyData: linuxVmAdminPasswordOrKey
      }
    ]
  }
}

resource linuxVmNsg 'Microsoft.Network/networkSecurityGroups@2021-05-01' = {
  name: linuxVmNsgName
  location: location
  properties: {
    securityRules: [      
      {
        name: 'AllowSSH'
        properties: {
          priority: 100
          access: 'Allow'
          direction: 'Inbound'
          protocol: 'Tcp'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '22'
        }
      }            
    ]
  }
}

resource windowsVmNsg 'Microsoft.Network/networkSecurityGroups@2021-05-01' = {
  name: windowsVmNsgName
  location: location
  properties: {
    securityRules: [
      {
        name: 'AllowRDP'
        properties: {
          priority: 100
          access: 'Allow'
          direction: 'Inbound'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '3389'
        }
      }                  
    ]
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2021-05-01' = {
  name: virtualNetworkName
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetAddressPrefix
      ]
    }
  }
}

resource linuxVmSubnet 'Microsoft.Network/virtualNetworks/subnets@2021-05-01' = {
  parent: vnet
  name: linuxVmSubnetName
  properties: {
    addressPrefix: linuxVmSubnetAddressPrefix
    privateEndpointNetworkPolicies: 'Enabled'
    privateLinkServiceNetworkPolicies: 'Enabled'
    networkSecurityGroup: {
      id: linuxVmNsg.id
    }
  }
}

resource windowsVmSubnet 'Microsoft.Network/virtualNetworks/subnets@2021-05-01' = {
  parent: vnet
  name: windowsVmSubnetName
  properties: {
    addressPrefix: windowsVmSubnetAddressPrefix
    privateEndpointNetworkPolicies: 'Enabled'
    privateLinkServiceNetworkPolicies: 'Enabled'
    networkSecurityGroup: {
      id: windowsVmNsg.id
    }    
  }
  dependsOn: [
    linuxVmSubnet
  ]
}

resource linuxVmNic 'Microsoft.Network/networkInterfaces@2021-05-01' = {
  name: linuxVmNetworkInterfaceName
  location: location
  properties: {
    ipConfigurations: [
      {
        name: 'ipconfig1'
        properties: {
          subnet: {
            id: linuxVmSubnet.id
          }
          privateIPAllocationMethod: 'Dynamic'
          publicIPAddress: {
            id: linuxVmPublicIP.id
          }
        }
      }
    ]    
  }
}

resource linuxVmPublicIP 'Microsoft.Network/publicIPAddresses@2021-05-01' = {
  name: linuxVmPublicIPAddressName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    publicIPAllocationMethod: 'Dynamic'
    publicIPAddressVersion: 'IPv4'
    dnsSettings: {
      domainNameLabel: linuxAdminDnsLabelPrefix
    }
    idleTimeoutInMinutes: 4
  }
}

resource linuxVmImage 'Microsoft.Compute/images@2022-08-01' = {
  name: linuxVmImageName
  location: location
  
  properties: {
    hyperVGeneration: 'V2'    
    storageProfile: {      
      osDisk: {
        blobUri: blobUri
        caching: 'ReadWrite'                        
        osState: 'Generalized'
        osType: 'Linux'        
        storageAccountType: 'Standard_LRS'
      }      
    }
  }
}


resource linuxVm 'Microsoft.Compute/virtualMachines@2021-11-01' = {
  name: linuxVmName
  location: location
  properties: {
    hardwareProfile: {
      vmSize: linuxVmSize
    }
    storageProfile: {
      osDisk: {
        createOption: 'FromImage'
        managedDisk: {
          storageAccountType: linuxVmOsDiskType
        }
      }
      imageReference: {
        id: linuxVmImage.id
      }
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: linuxVmNic.id
        }
      ]
    }
    osProfile: {
      computerName: linuxVmName
      adminUsername: linuxVmAdminUsername
      adminPassword: linuxVmAdminPasswordOrKey
      linuxConfiguration: ((authenticationType == 'password') ? null : linuxVmConfiguration)
    }
  }
}


resource windowsVmStg 'Microsoft.Storage/storageAccounts@2021-04-01' = {
  name: windowsVmStorageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'Storage'
}

resource windowsVmPublicIP 'Microsoft.Network/publicIPAddresses@2021-02-01' = {
  name: windowsVmPublicIPAddressName        
  location: location
  sku: {
    name: windowsVmPublicIpSku
  }
  properties: {
    publicIPAllocationMethod: windowsVmPublicIPAllocationMethod
    dnsSettings: {
      domainNameLabel: windowsVmDnsLabelPrefix
    }
  }
}

resource windowsVmNic 'Microsoft.Network/networkInterfaces@2021-02-01' = {
  name: windowsVmNicName
  location: location
  properties: {
    ipConfigurations: [
      {
        name: 'ipconfig1'
        properties: {
          privateIPAllocationMethod: 'Dynamic'
          subnet: {
            id: windowsVmSubnet.id
          }
          publicIPAddress: {
            id: windowsVmPublicIP.id
          }          
        }
      }
    ]
  }
}

resource windowsVm 'Microsoft.Compute/virtualMachines@2021-03-01' = {
  name: windowsVmName
  location: location
  properties: {
    hardwareProfile: {
      vmSize: windowsVmSize
    }
    osProfile: {
      computerName: windowsVmName
      adminUsername: windowsVmAdminUsername
      adminPassword: windowsVmAdminPassword
    }
    storageProfile: {
      imageReference: {
        publisher: 'MicrosoftWindowsServer'
        offer: 'WindowsServer'
        sku: windowsVmOSVersion
        version: 'latest'
      }
      osDisk: {
        createOption: 'FromImage'
        managedDisk: {
          storageAccountType: windowVmOsDiskType
        }
      }
      dataDisks: [
        {
          diskSizeGB: 1023
          lun: 0
          createOption: 'Empty'
        }
      ]
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: windowsVmNic.id
        }
      ]
    }
    diagnosticsProfile: {
      bootDiagnostics: {
        enabled: true
        storageUri: windowsVmStg.properties.primaryEndpoints.blob
      }
    }
  }
}

output linuxVmAdminUsername string = linuxVmAdminUsername
output linuxVmHostname string = linuxVmPublicIP.properties.dnsSettings.fqdn
output linuxVmSshCommand string = 'ssh ${linuxVmAdminUsername}@${linuxVmPublicIP.properties.dnsSettings.fqdn}'
output windowsVmHostname string = windowsVmPublicIP.properties.dnsSettings.fqdn
