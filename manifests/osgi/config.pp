# == Define: aem::osgi::config
#
# Configure osgi resource based on the specified type.
#
define aem::osgi::config(
  Enum['present', 'absent'] $ensure         = 'present',
  $group          = 'aem',
  Optional[Enum['merge', 'remove']] $handle_missing = undef,
  Stdlib::Absolutepath $home = undef,
  $password       = undef,
  $pid            = undef,
  $properties     = undef,
  Enum['console', 'file'] $type = undef,
  $user           = 'aem',
  $username       = undef,
){
  if $ensure == 'present' {
    if $properties == undef {
      fail('Properties must contain at least one entry.')
    }

    unless $properties =~ Hash {
      fail("Aem::Osgi::Config[${name}]: 'properties' must be a Hash of values")
    }
  }

  if $type == 'console' {

    if $username == undef {
      fail("Username must be specified if type == 'console'")
    }
    if $password == undef {
      fail("Password must be specified if type == 'console'")
    }
  }

  case $type {
    'console' : {
      aem_osgi_config { $name :
        ensure         => $ensure,
        configuration  => $properties,
        handle_missing => $handle_missing,
        home           => $home,
        pid            => $pid,
        password       => $password,
        username       => $username,
      }
    }
    'file' : {
      aem::osgi::config::file { $name :
        ensure     => $ensure,
        group      => $group,
        home       => $home,
        pid        => $pid,
        properties => $properties,
        user       => $user,
      }
    }
    default : {
      fail("${type} is not supported for type. Allowed values are 'console' and 'file'.")
    }
  }
}
