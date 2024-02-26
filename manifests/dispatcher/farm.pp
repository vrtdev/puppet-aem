# == Define: aem::dispatcher::farm
#
# Configure a Dispatcher instance.
define aem::dispatcher::farm(
  Enum['present', 'absent'] $ensure              = 'present',
  Optional[Integer[0, 1]] $allow_authorized    = undef,
  Variant[Optional[Array[Hash]], Optional[Hash]] $allowed_clients     = $::aem::dispatcher::params::allowed_clients,
  Optional[Array[String]] $cache_headers       = undef,
  Variant[Optional[Array[Hash]], Optional[Hash]] $cache_rules         = $::aem::dispatcher::params::cache_rules,
  Optional[Integer[0, 1]] $cache_ttl           = undef,
  Variant[Optional[Array[String]], Optional[String]] $client_headers      = $::aem::dispatcher::params::client_headers,
  Stdlib::Absolutepath $docroot = undef,
  Optional[Integer[0, 1]] $failover            = undef,
  Variant[Optional[Array[Hash]], Optional[Hash]] $filters             = $::aem::dispatcher::params::filters,
  Optional[Integer] $grace_period        = undef,
  Optional[String] $health_check_url    = undef,
  Variant[Optional[Array[Hash]], Optional[Hash]] $ignore_parameters   = undef,
  Variant[Optional[Array[Hash]], Optional[Hash]] $invalidate          = undef,
  Optional[Stdlib::Absolutepath] $invalidate_handler = undef,
  Optional[Integer[0, 99]] $priority            = $::aem::dispatcher::params::priority,
  Optional[Integer[0, 1]] $propagate_synd_post = undef,
  Variant[Optional[Array[Hash]], Optional[Hash]] $renders             = $::aem::dispatcher::params::renders,
  Optional[Integer] $retries             = undef,
  Optional[Integer] $retry_delay         = undef,
  Optional[Integer[0, 1]] $serve_stale         = undef,
  Optional[Hash] $session_management  = undef,
  Optional[Stdlib::Absolutepath] $stat_file = undef,
  Optional[Integer] $stat_files_level    = undef,
  Variant[Optional[Array[Hash]], Optional[Hash]]  $statistics          = undef,
  Variant[Optional[Array[String]], Optional[String]] $sticky_connections  = undef,
  Optional[Integer] $unavailable_penalty = undef,
  Optional[Hash[String, Variant[String, Integer]]] $vanity_urls         = undef,
  Variant[Optional[Array[String]], Optional[String]] $virtualhosts        = $::aem::dispatcher::params::virtualhosts
) {

  # Required dispatcher class because it is used by parameter defaults
  if ! defined(Class['aem::dispatcher']) {
    fail('You must include the aem::dispatcher base class before using any dispatcher class or defined resources')
  }

  if $allowed_clients =~ Array[Hash] {
    $_allowed_clients = $allowed_clients
  } else {
    $_allowed_clients = [$allowed_clients]
  }

  if $cache_headers {
    $_cache_headers = $cache_headers
  }

  if $cache_rules =~ Array[Hash] {
    $_cache_rules = $cache_rules
  } else {
    $_cache_rules = [$cache_rules]
  }

  if $client_headers =~ Array[String] {
    $_client_headers = $client_headers
  } else {
    $_client_headers = [$client_headers]
  }

  if $filters =~ Array[Hash] {
    $_filters = $filters
  } else {
    $_filters = [$filters]
  }

  if $ignore_parameters {
    if $ignore_parameters =~ Array[Hash] {
      $_ignore_parameters = $ignore_parameters
    } else {
      $_ignore_parameters = [$ignore_parameters]
    }
  }

  if $invalidate and $invalidate_handler {
    fail('Both invalidate and invalidate_handler can not be set.')
  }

  if $invalidate == undef {
    $_invalidate = $::aem::dispatcher::params::invalidate
  } else {
    if $invalidate =~ Array[Hash] {
      $_invalidate = $invalidate
    } else {
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

  if $renders =~ Array[Hash] {
    $_renders = $renders
  } else {
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
    if $statistics =~ Array[Hash] {
      $_statistics = $statistics
    } else {
      $_statistics = [$statistics]
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

  if $virtualhosts {
    $_virtualhosts = $virtualhosts
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
