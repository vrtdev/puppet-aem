# == Define: aem::instance
#
# This define manages AEM instances.
#
#
define aem::instance (
  Enum['present', 'absent'] $ensure                  = 'present',
  Optional[String] $context_root            = undef,
  Optional[Integer] $debug_port              = undef,
  String $group                   = 'aem',
  Stdlib::Absolutepath $home                    = undef,
  String $jvm_mem_opts            = '-Xmx1024m',
  String $jvm_opts                = undef,
  Boolean $manage_group            = true,
  Boolean $manage_home             = true,
  Boolean $manage_user             = true,
  Variant[Array[Hash], Hash] $osgi_configs            = undef,
  Optional[Array] $crx_packages            = undef,
  Integer $port                    = 4502,
  Array $runmodes                = [],
  Boolean $sample_content          = true,
  Integer $snooze                  = 10,
  Stdlib::Absolutepath $source                  = undef,
  Enum['enabled', 'disabled', 'running', 'unmanaged'] $status = 'enabled',
  Integer $timeout                 = 600,
  Enum['author', 'publish', 'standby'] $type = author,
  String $user                    = 'aem',
  Optional[Pattern[/^\d+\.\d+(\.\d+)?$/]] $version = undef,
  Hash $systemd_service_options = undef,
) {

  anchor { "aem::${name}::begin": }

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
    unless $osgi_configs =~ Hash and !($osgi_configs =~ Array and $osgi_configs[0] =~ Hash) {
      fail("Aem::Instance[${name}]: 'osgi_configs' must be either a Hash or an Array of Hashes")
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
