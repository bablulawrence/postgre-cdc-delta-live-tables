@description('The name of you Virtual Machine.')
param linuxVmName string = 'az-cslabs-ph2-vm1'

@description('Username for the Virtual Machine.')
param linuxAdminUsername string

@description('Type of authentication to use on the Virtual Machine. SSH key is recommended.')
@allowed([
  'sshPublicKey'
  'password'
])
param authenticationType string = 'password'

@description('SSH Key or password for the Virtual Machine. SSH key is recommended.')
@secure()
param linuxAdminPasswordOrKey string

@description('Unique DNS Name for the Public IP used to access the Virtual Machine.')
param linuxAdminDnsLabelPrefix string = toLower('${linuxVmName}-${uniqueString(resourceGroup().id)}')

@description('The Ubuntu version for the VM. This will pick a fully patched image of this given Ubuntu version.')
param ubuntuOSVersion string = '20_04-lts-gen2'

@description('Location for all resources.')
param location string = resourceGroup().location

@description('The size of the VM')
param linuxVmSize string = 'Standard_B2s'

@description('Name of the VNET')
param virtualNetworkName string = 'vNet'

@description('Name of the subnet in the virtual network')
param linuxSubnetName string = 'linuxSubnet'

@description('Name of the Network Security Group')
param networkSecurityGroupName string = 'SecGroupNet'

var publicIPAddressName = '${linuxVmName}PublicIP'
var networkInterfaceName = '${linuxVmName}NetInt'
var osDiskType = 'Standard_LRS'
var linuxVmSubnetAddressPrefix = '10.1.0.0/24'
var addressPrefix = '10.1.0.0/16'
var linuxConfiguration = {
  disablePasswordAuthentication: true
  ssh: {
    publicKeys: [
      {
        path: '/home/${linuxAdminUsername}/.ssh/authorized_keys'
        keyData: linuxAdminPasswordOrKey
      }
    ]
  }
}

resource nsg 'Microsoft.Network/networkSecurityGroups@2021-05-01' = {
  name: networkSecurityGroupName
  location: location
  properties: {
    securityRules: [
      {
        name: 'SSH'
        properties: {
          priority: 1000
          protocol: 'Tcp'
          access: 'Allow'
          direction: 'Inbound'
          sourceAddressPrefix: '*'
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '22'
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
        addressPrefix
      ]
    }
  }
}

resource subnet 'Microsoft.Network/virtualNetworks/subnets@2021-05-01' = {
  parent: vnet
  name: linuxSubnetName
  properties: {
    addressPrefix: linuxVmSubnetAddressPrefix
    privateEndpointNetworkPolicies: 'Enabled'
    privateLinkServiceNetworkPolicies: 'Enabled'
  }
}

resource nic 'Microsoft.Network/networkInterfaces@2021-05-01' = {
  name: networkInterfaceName
  location: location
  properties: {
    ipConfigurations: [
      {
        name: 'ipconfig1'
        properties: {
          subnet: {
            id: subnet.id
          }
          privateIPAllocationMethod: 'Dynamic'
          publicIPAddress: {
            id: publicIP.id
          }
        }
      }
    ]
    networkSecurityGroup: {
      id: nsg.id
    }
  }
}

resource publicIP 'Microsoft.Network/publicIPAddresses@2021-05-01' = {
  name: publicIPAddressName
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

resource vm 'Microsoft.Compute/virtualMachines@2021-11-01' = {
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
          storageAccountType: osDiskType
        }
      }
      imageReference: {
        publisher: 'Canonical'
        offer: '0001-com-ubuntu-server-focal'
        sku: ubuntuOSVersion
        version: 'latest'
      }
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: nic.id
        }
      ]
    }
    osProfile: {
      computerName: linuxVmName
      adminUsername: linuxAdminUsername
      adminPassword: linuxAdminPasswordOrKey
      linuxConfiguration: ((authenticationType == 'password') ? null : linuxConfiguration)
    }
  }
}

output linuxAdminUsername string = linuxAdminUsername
output hostname string = publicIP.properties.dnsSettings.fqdn
output sshCommand string = 'ssh ${linuxAdminUsername}@${publicIP.properties.dnsSettings.fqdn}'

@description('Username for the Virtual Machine.')
param windowsVmAdminUsername string

@description('Password for the Virtual Machine.')
@minLength(12)
@secure()
param windowsVmAdminPassword string

@description('Unique DNS Name for the Public IP used to access the Virtual Machine.')
param windowsVmDnsLabelPrefix string = toLower('${windowsVmName}-${uniqueString(resourceGroup().id, windowsVmName)}')

@description('Name for the Public IP used to access the Virtual Machine.')
param windowsVmPublicIpName string = 'myPublicIP'

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

@description('Location for all resources.')
param location string = resourceGroup().location

@description('Name of the virtual machine.')
param windowsVmName string = 'simple-vm'

var windowsVmStorageAccountName = 'bootdiags${uniqueString(resourceGroup().id)}'
var nicName = 'myVMNic'
var addressPrefix = '10.0.0.0/16'
var windowsVmSubnetName = 'Subnet'
var windowsSubnetPrefix = '10.0.0.0/24'
var virtualNetworkName = 'MyVNET'
var networkSecurityGroupName = 'default-NSG'

resource stg 'Microsoft.Storage/storageAccounts@2021-04-01' = {
  name: windowsVmStorageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'Storage'
}

resource pip 'Microsoft.Network/publicIPAddresses@2021-02-01' = {
  name: windowsVmPublicIpName
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

resource securityGroup 'Microsoft.Network/networkSecurityGroups@2021-02-01' = {
  name: networkSecurityGroupName
  location: location
  properties: {
    securityRules: [
      {
        name: 'default-allow-3389'
        properties: {
          priority: 1000
          access: 'Allow'
          direction: 'Inbound'
          destinationPortRange: '3389'
          protocol: 'Tcp'
          sourcePortRange: '*'
          sourceAddressPrefix: '*'
          destinationAddressPrefix: '*'
        }
      }
    ]
  }
}

resource vn 'Microsoft.Network/virtualNetworks@2021-02-01' = {
  name: virtualNetworkName
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        addressPrefix
      ]
    }
    subnets: [
      {
        name: windowsVmSubnetName
        properties: {
          addressPrefix: windowsSubnetPrefix
          networkSecurityGroup: {
            id: securityGroup.id
          }
        }
      }
    ]
  }
}

resource nic 'Microsoft.Network/networkInterfaces@2021-02-01' = {
  name: nicName
  location: location
  properties: {
    ipConfigurations: [
      {
        name: 'ipconfig1'
        properties: {
          privateIPAllocationMethod: 'Dynamic'
          publicIPAddress: {
            id: pip.id
          }
          subnet: {
            id: resourceId('Microsoft.Network/virtualNetworks/subnets', vn.name, windowsVmSubnetName)
          }
        }
      }
    ]
  }
}

resource vm 'Microsoft.Compute/virtualMachines@2021-03-01' = {
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
          storageAccountType: 'StandardSSD_LRS'
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
          id: nic.id
        }
      ]
    }
    diagnosticsProfile: {
      bootDiagnostics: {
        enabled: true
        storageUri: stg.properties.primaryEndpoints.blob
      }
    }
  }
}

output hostname string = pip.properties.dnsSettings.fqdn
