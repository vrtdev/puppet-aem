# == Define: aem::license
#
# Used to manage AEM license files.
#
#
define aem::license (
  $ensure      = 'present',
  $customer    = undef,
  $group       = 'aem',
  Stdlib::Absolutepath $home = undef,
  $license_key = undef,
  $user        = 'aem',
  $version     = undef) {

  if $ensure =~ /^((?!((^|, )(present|absent))+$).)*$/ {
    fail("${ensure} is not supported for ensure. Allowed values are 'present' and 'absent'.")
  }

  if $home == undef {
    fail('Home directory must be specified.')
  }

  if $ensure == 'present' and $license_key == undef {
    fail('License key must be specified.')
  }

  # Create the env script
  file { "${home}/license.properties":
    ensure  => $ensure,
    content => template("${module_name}/license.properties.erb"),
    group   => $group,
    mode    => '0664',
    owner   => $user,
  }

  if defined(File[$home]) {
    File[$home]
    -> File["${home}/license.properties"]
  }

}
