# == Define: aem::instance
#
# This define manages AEM instances.
#
#
define aem::instance (
  $ensure                  = 'present',
  $context_root            = undef,
  Variant[Undef, Integer] $debug_port              = undef,
  $group                   = 'aem',
  Stdlib::Absolutepath $home                    = undef,
  $jvm_mem_opts            = '-Xmx1024m',
  $jvm_opts                = undef,
  Boolean $manage_group            = true,
  Boolean $manage_home             = true,
  Boolean $manage_user             = true,
  $osgi_configs            = undef,
  Variant[Undef, Array] $crx_packages            = undef,
  Integer $port                    = 4502,
  Array $runmodes                = [],
  Boolean $sample_content          = true,
  Integer $snooze                  = 10,
  Stdlib::Absolutepath $source                  = undef,
  $status                  = 'enabled',
  Integer $timeout                 = 600,
  $type                    = author,
  $user                    = 'aem',
  $version                 = undef,
  $systemd_service_options = undef,
) {

  anchor { "aem::${name}::begin": }

  if $ensure =~ /^((?!((^|, )(present|absent))+$).)*$/ {
    fail("${ensure} is not supported for ensure. Allowed values are 'present' and 'absent'.")
  }

  if !$home {
    case $::kernel {
      'Linux' : { $_home = '/opt/aem' }
      default : { fail("'${module_name}' has no default 'home' value for '${::kernel}'") }
    }
  } else {
    $_home = $home
  }

  if $manage_group {
    group { $group: ensure => $ensure, }
  }

  if $manage_user {
    user { $user:
      ensure => $ensure,
      gid    => $group,
    }
  }

  if $osgi_configs {
    unless $osgi_configs =~ Hash and !(is_array($osgi_configs) and $osgi_configs[0] =~ Hash) {
      fail("Aem::Instance[${name}]: 'osgi_configs' must be either a Hash or an Array of Hashes")
    }
  }

  if $status =~ /^((?!((^|, )(enabled|disabled|running|unmanaged))+$).)*$/ {
    fail("${status} is not supported for status. Allowed values are 'enabled', 'disabled', 'running' and 'unmanaged'.")
  }

  if $type =~ /^((?!((^|, )(author|publish|standby))+$).)*$/ {
    fail("${type} is not supported for type. Allowed values are 'author', 'publish' and 'standby'.")
  }

  if $version {
    if $version =~ /^((?!((^|, )(^\d+\.\d+(\.\d+)?$))+$).)*$/ {
      fail("${version} is not a valid version.")
    }
  }

  # ### Manage actions

  # package(s)
  aem::package { $name :
    ensure      => $ensure,
    group       => $group,
    home        => $_home,
    manage_home => $manage_home,
    source      => $source,
    user        => $user,
  }

  if $status != 'unmanaged' {
    aem::service { $name :
      ensure                  => $ensure,
      status                  => $status,
      home                    => $_home,
      user                    => $user,
      group                   => $group,
      systemd_service_options => $systemd_service_options,
    }
  }

  if ($ensure == 'present') {
    # configuration
    aem::config { $name:
      context_root   => $context_root,
      debug_port     => $debug_port,
      group          => $group,
      home           => $_home,
      jvm_mem_opts   => $jvm_mem_opts,
      jvm_opts       => $jvm_opts,
      osgi_configs   => $osgi_configs,
      crx_packages   => $crx_packages,
      port           => $port,
      runmodes       => $runmodes,
      sample_content => $sample_content,
      type           => $type,
      user           => $user,
    }

    aem_installer { $name:
      ensure  => $ensure,
      home    => $_home,
      snooze  => $snooze,
      timeout => $timeout,
    }

    # Is there no way to do this better?
    if $manage_group {
      Anchor["aem::${name}::begin"]
      -> Group[$group]
      -> Aem::Package[$name]
    }

    if $manage_user {
      Anchor["aem::${name}::begin"]
      -> User[$user]
      -> Aem::Package[$name]

      if $manage_group {
        Anchor["aem::${name}::begin"]
        -> Group[$group]
        -> User[$user]
      }
    }

    Anchor["aem::${name}::begin"]
    -> Aem::Package[$name]
    -> Aem::Config[$name]
    -> Aem_Installer[$name]

    if $status != 'unmanaged' {
      Aem_Installer[$name]
      ~> Aem::Service[$name]

      Aem::Config[$name]
      ~> Aem::Service[$name]
    }

  } else {
    Anchor["aem::${name}::begin"]
    -> Aem::Service[$name]
    -> Aem::Package[$name]

    # I mean seriously.
    if $manage_user {
      Anchor["aem::${name}::begin"]
      -> User[$user]

    }

    if $manage_group {
      Anchor["aem::${name}::begin"]
      -> Group[$group]

      if $manage_user {
        Anchor["aem::${name}::begin"]
        -> User[$user]
        -> Group[$group]
      }
    }

  }
}
