# This class exists soley to ensure that the module is properly defined.

class aem::dispatcher (
  Enum['present', 'absent'] $ensure             = 'present',
  Enum['on', 'off', 1, 0] $decline_root       = $::aem::dispatcher::params::decline_root,
  $dispatcher_name    = undef,
  $group              = $::aem::dispatcher::params::group,
  $log_file           = $::aem::dispatcher::params::log_file,
  Enum['error', 'warn', 'info', 'debug', 'trace', 4, 0] $log_level          = $::aem::dispatcher::params::log_level,
  Stdlib::Absolutepath $module_file = undef,
  $pass_error         = $::aem::dispatcher::params::pass_error,
  Enum['on', 'off', 1, 0] $use_processed_url  = $::aem::dispatcher::params::use_processed_url,
  $user               = $::aem::dispatcher::params::user
) inherits ::aem::dispatcher::params {

  # Check for Apache because it is used by parameter defaults
  if ! defined(Class['apache']) {
    fail('You must include the apache base class before using any dispatcher class or defined resources')
  }

  anchor { 'aem::dispatcher::begin': }

  $_mod_filename = basename($module_file)

  $config_file = $::aem::dispatcher::params::config_file

  anchor { 'aem::dispatcher::end': }

  # Manage actions

  if ($ensure == 'present') {
    apache::mod { 'dispatcher' :
      lib => 'mod_dispatcher.so',
    }

    file { "${::aem::dispatcher::params::mod_path}/${_mod_filename}" :
      ensure  => file,
      group   => $group,
      owner   => $user,
      replace => true,
      source  => $module_file,
    }

    file { "${::aem::dispatcher::params::mod_path}/mod_dispatcher.so" :
      ensure  => link,
      group   => $group,
      owner   => $user,
      replace => true,
      target  => "${::aem::dispatcher::params::mod_path}/${_mod_filename}",
    }

    file { "${::aem::dispatcher::params::farm_path}/dispatcher.conf" :
      ensure  => file,
      group   => $group,
      owner   => $user,
      replace => true,
      content => template("${module_name}/dispatcher/dispatcher.conf.erb")
    }

    file {  "${::aem::dispatcher::params::farm_path}/${config_file}":
      ensure  => file,
      group   => $group,
      owner   => $user,
      replace => true,
      content => template("${module_name}/dispatcher/dispatcher.farms.erb")
    }

    Anchor['aem::dispatcher::begin']
    -> File["${::aem::dispatcher::params::mod_path}/${_mod_filename}"]
    -> File["${::aem::dispatcher::params::mod_path}/mod_dispatcher.so"]
    -> Apache::Mod['dispatcher']
    -> File["${::aem::dispatcher::params::farm_path}/${config_file}"]
    -> File["${::aem::dispatcher::params::farm_path}/dispatcher.conf"]
    -> Anchor['aem::dispatcher::end']

    if defined(Service[$::apache::service_name]) {
      Anchor['aem::dispatcher::begin']
      -> File["${::aem::dispatcher::params::farm_path}/${config_file}"]
      ~> Service[$::apache::service_name]
      -> Anchor['aem::dispatcher::end']

      Anchor['aem::dispatcher::begin']
      -> File["${::aem::dispatcher::params::farm_path}/dispatcher.conf"]
      ~> Service[$::apache::service_name]
      -> Anchor['aem::dispatcher::end']
    }

  } else {

    file { "${::aem::dispatcher::params::mod_path}/${_mod_filename}" :
      ensure => $ensure,
    }

    file { "${::aem::dispatcher::params::mod_path}/mod_dispatcher.so" :
      ensure => $ensure,
    }

    file { "${::aem::dispatcher::params::farm_path}/dispatcher.conf" :
      ensure => $ensure,
    }

    file { "${::aem::dispatcher::params::farm_path}/${config_file}" :
      ensure => $ensure,
    }

    Anchor['aem::dispatcher::begin']
    -> File["${::aem::dispatcher::params::farm_path}/dispatcher.conf"]
    -> File["${::aem::dispatcher::params::farm_path}/${config_file}"]
    -> File["${::aem::dispatcher::params::mod_path}/${_mod_filename}"]
    -> File["${::aem::dispatcher::params::mod_path}/mod_dispatcher.so"]
    -> Anchor['aem::dispatcher::end']

    if defined(Service[$::apache::service_name]) {
      Anchor['aem::dispatcher::begin']
      -> File["${::aem::dispatcher::params::farm_path}/${config_file}"]
      ~> Service[$::apache::service_name]
      -> Anchor['aem::dispatcher::end']

      Anchor['aem::dispatcher::begin']
      -> File["${::aem::dispatcher::params::farm_path}/dispatcher.conf"]
      ~> Service[$::apache::service_name]
      -> Anchor['aem::dispatcher::end']
    }
  }

}
