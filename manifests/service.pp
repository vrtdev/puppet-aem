# == Defines: aem::service
#
# Used to create service definitions for AEM instances.
#
# Based on Elastic Search service management.
#
define aem::service (
  $ensure                  = 'present',
  $group                   = 'aem',
  Stdlib::Absolutepath $home = undef,
  $status                  = 'enabled',
  $user                    = 'aem',
  $systemd_service_options = undef,
) {

  if $home == undef {
    fail('Home directory must be specified.')
  }

  case $facts['os']['name'] {
    'CentOS', 'Fedora', 'RedHat' : {

      if versioncmp($facts['os']['release']['major'], '7') >= 0 {
        $provider      = 'systemd'
      } else {
        $provider      = 'init'
      }
    }
    'Amazon' : {
      $provider = 'init'
    }
    'Debian': {

      if versioncmp($facts['os']['release']['major'], '8') >= 0 {
        $provider      = 'systemd'
      } else {
        $provider      = 'init'
      }
    }
    'Ubuntu': {

      if versioncmp($facts['os']['release']['major'], '15') >= 0 {
        $provider      = 'systemd'
      } else {
        $provider      = 'init'
      }
    }
    default: {
      fail("'${module_name}' provides no service parameters for '${facts['os']['name']}'")
    }
  }

  case $provider {
    'init' : {
      aem::service::init { $name :
        ensure => $ensure,
        status => $status,
        group  => $group,
        home   => $home,
        user   => $user,
      }
    }
    'systemd' : {
      aem::service::systemd { $name :
        ensure                  => $ensure,
        status                  => $status,
        group                   => $group,
        home                    => $home,
        user                    => $user,
        systemd_service_options => $systemd_service_options,
      }
    }
    default : {
      fail("Unknown service provider: ${provider}")
    }
  }

}
