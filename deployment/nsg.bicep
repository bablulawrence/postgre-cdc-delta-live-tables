@description('Name of the Network Security Group')
param networkSecurityGroupName string = 'az-cslabs-ph2-secgrp'

@description('Location for all resources.')
param location string = resourceGroup().location


resource nsg 'Microsoft.Network/networkSecurityGroups@2021-05-01' = {
  name: networkSecurityGroupName
  location: location
  properties: {
    securityRules: [
      {
        name: 'default-allow-3389'
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
      {
        name: 'AllowTagSSHInbound'
        properties: {
          priority: 120
          access: 'Allow'
          protocol: 'TCP'
          direction: 'Inbound'          
          sourceAddressPrefix: 'VirtualNetwork'
          sourcePortRange: '*'
          destinationPortRange: '22'
          destinationAddressPrefix: '*'
        }
      }            
    ]
  }
}
