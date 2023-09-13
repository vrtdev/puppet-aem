# == Define: aem::dispatcher::farm
#
# Configure a Dispatcher instance.
define aem::dispatcher::farm(
  Enump['present', 'absent'] $ensure              = 'present',
  Enum[1, 0] $allow_authorized    = undef,
  Array $allowed_clients     = $::aem::dispatcher::params::allowed_clients,
  $cache_headers       = undef,
  $cache_rules         = $::aem::dispatcher::params::cache_rules,
  Enum[1, 0] $cache_ttl           = undef,
  $client_headers      = $::aem::dispatcher::params::client_headers,
  Stdlib::Absolutepath $docroot = undef,
  Enum[1, 0] $failover            = undef,
  $filters             = $::aem::dispatcher::params::filters,
  Enum[undef, 1] $grace_period        = undef,
  String $health_check_url    = undef,
  $ignore_parameters   = undef,
  $invalidate          = undef,
  Stdlib::Absolutepath $invalidate_handler = undef,
  Integer $priority            = $::aem::dispatcher::params::priority,
  Enum[1, 0] $propagate_synd_post = undef,
  $renders             = $::aem::dispatcher::params::renders,
  Enum[undef, 1] $retries             = undef,
  Enum[undef, 1] $retry_delay         = undef,
  Enum[1, 0] $serve_stale         = undef,
  Hash[String, String, String, Integer] $session_management  = undef,
  Stdlib::Absolutepath $stat_file = undef,
  Enum[undef, 0] $stat_files_level    = undef,
  Array $statistics          = undef,
  Array $sticky_connections  = undef,
  Enum[undef, 1] $unavailable_penalty = undef,
  Hash[String, Integer] $vanity_urls         = undef,
  Array $virtualhosts        = $::aem::dispatcher::params::virtualhosts
) {

  # Required dispatcher class because it is used by parameter defaults
  if ! defined(Class['aem::dispatcher']) {
    fail('You must include the aem::dispatcher base class before using any dispatcher class or defined resources')
  }

  if $allowed_clients =~ Array {
    unless $allowed_clients[0] =~ Hash {
      fail('Allowed clients should be a hash or an array of hashes.')
    }
    $_allowed_clients = $allowed_clients
  } else {
    unless $allowed_clients =~ Hash {
      fail('Allowed clients should be a hash or an array of hashes.')
    }
    $_allowed_clients = [$allowed_clients]
  }

  if $cache_headers {
    if is_array($cache_headers) {
      $_cache_headers = $cache_headers
    } else {
      $_cache_headers = [$cache_headers]
    }
  }

  if is_array($cache_rules) {
    unless $cache_rules[0] =~ Hash {
      fail('Cache rules should be a hash or an array of hashes.')
    }
    $_cache_rules = $cache_rules
  } else {
    unless $cache_rules =~ Hash {
      fail('Cache rules should be a hash or an array of hashes.')
    }
    $_cache_rules = [$cache_rules]
  }

  if $client_headers =~ Array {
    $_client_headers = $client_headers
  } else {
    $_client_headers = [$client_headers]
  }

  if $filters =~ Array {
    unless $filters[0] =~ Hash {
      fail('Filters should be a hash or an array of hashes.')
    }
    $_filters = $filters
  } else {
    unless $filters =~ Hash {
      fail('Filters should be a hash or an array of hashes.')
    }
    $_filters = [$filters]
  }

  if $ignore_parameters {
    if $ignore_parameters =~ Array {
      unless $ignore_parameters[0] =~ Hash {
        fail('Ignore parameters should be a hash or an array of hashes.')
      }
      $_ignore_parameters = $ignore_parameters
    } else {
      unless $ignore_parameters =~ Hash {
        fail('Ignore parameters should be a hash or an array of hashes.')
      }
      $_ignore_parameters = [$ignore_parameters]
    }
  }

  if $invalidate and $invalidate_handler {
    fail('Both invalidate and invalidate_handler can not be set.')
  }

  if $invalidate == undef {
    $_invalidate = $::aem::dispatcher::params::invalidate
  } else {

    if $invalidate =~ Array {
      unless $invalidate[0] =~ Hash {
        fail('Invalidate should be a hash or an array of hashes.')
      }
      $_invalidate = $invalidate
    } else {
      unless $invalidate =~ Hash {
        fail('Invalidate should be a hash or an array of hashes.')
      }
      $_invalidate = [$invalidate]
    }
  }

  if $priority {
    if $priority < 10 {
      $priority_string = "0${priority}"
    }
    else {
      $priority_string = $priority
    }

  }
  else {
    $priority_string = '00'
  }

  if $renders =~ Array {
    unless $renders[0] =~ Hash {
      fail('Renders should be a hash or an array of hashes.')
    }
    $_renders = $renders
  } else {
    unless $renders =~ Hash {
      fail('Renders should be a hash or an array of hashes.')
    }
    $_renders = [$renders]
  }

  if $session_management {
    if $allow_authorized == 1 {
      fail('Allow authorized and session management are mutually exclusive.')
    }
    if !('directory' in $session_management) {
      fail('Session management directory is not specified.')
    } else {
      unless $session_management['directory'] =~ Stdlib::Absolutepath {
        fail("${session_management['directory']} is not an absolute path.")
      }
    }
    if 'encode' in $session_management {
      if $session_management['encode'] =~ /^((?!((^|, )(md5|hex))+$).)*$/ {
        fail("${session_management['encode']} is not supported for session_management['encode']. Allowed values are 'md5' and 'hex'.")
      }
    }
  }
  
  if $statistics {
    unless $statistics[0] =~ Hash {
      fail('Statistics should be a hash or an array of hashes.')
    }
    $_statistics = $statistics
  } else {
    unless $statistics =~ Hash {
      fail('Statistics should be a hash or an array of hashes.')
    }
    $_statistics = [$statistics]
  }

  if $sticky_connections {
    unless $sticky_connections[0] =~ String {
      fail('Sticky connections should be a string or an array of strings.')
    }
  } else {
    unless $sticky_connections =~ String {
      fail('Sticky connections should be a string or an array of strings.')
    }
  }

  if $vanity_urls {
    if !('file' in $vanity_urls) {
      fail('Vanity Urls cache file is not specified.')
    } else {
      unless $vanity_urls['file'] =~ Stdlib::Absolutepath {
        fail("${vanity_urls['file']} is not an absolute path.")
      }
    }
  }

  if $virtualhosts =~ Array {
    $_virtualhosts = $virtualhosts
  } else {
    $_virtualhosts = [$virtualhosts]
  }

  if $ensure == 'present' {
    file { "${::aem::dispatcher::params::farm_path}/dispatcher.${priority_string}-${name}.inc.any" :
      ensure  => $ensure,
      content => template("${module_name}/dispatcher/dispatcher.any.erb"),
      notify  => Service[$::apache::service_name],
    }
  } else {
    file { "${::aem::dispatcher::params::farm_path}/dispatcher.${priority_string}-${name}.inc.any" :
      ensure => $ensure,
      notify => Service[$::apache::service_name],
    }
  }

}
