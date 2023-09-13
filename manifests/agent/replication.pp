# == Define: aem::agent::replication
#
# Configure a Replication Agent
define aem::agent::replication(

  $agent_user            = undef,
  Optional[Boolean] $batch_enabled         = undef,
  Optional[Integer] $batch_max_wait        = undef,
  Optional[Integer] $batch_trigger_size    = undef,
  $description           = undef,
  Boolean $enabled               = true,
  Enum['present', 'absent'] $ensure                = 'present',
  $force_passwords       = undef,
  Stdlib::Absolutepath $home = undef,
  Enum['debug', 'info', 'error'] $log_level             = 'info',
  Optional[Array] $mixin_types           = undef,
  $password              = undef,
  Optional[Boolean] $protocol_close_conn   = undef,
  Optional[Integer] $protocol_conn_timeout = undef,
  Optional[Array] $protocol_http_headers = undef,
  $protocol_http_method  = undef,
  $protocol_interface    = undef,
  Optional[Integer] $protocol_sock_timeout = undef,
  $protocol_version      = undef,
  $proxy_host            = undef,
  $proxy_ntlm_domain     = undef,
  $proxy_ntlm_host       = undef,
  $proxy_password        = undef,
  Optional[Integer] $proxy_port            = undef,
  $proxy_user            = undef,
  $resource_type         = undef,
  Optional[Integer] $retry_delay           = undef,
  Optional[Boolean] $reverse               = undef,
  $runmode               = undef,
  $serialize_type        = undef,
  Optional[Stdlib::Absolutepath] $static_directory = undef,
  $static_definition     = undef,
  $template              = undef,
  Optional[Integer] $timeout               = undef,
  Optional[Boolean] $trans_allow_exp_cert  = undef,
  $trans_ntlm_domain     = undef,
  $trans_ntlm_host       = undef,
  $trans_password        = undef,
  Optional[Enum['default', 'relaxed', 'clientauth']] $trans_ssl             = undef,
  $trans_uri             = undef,
  $trans_user            = undef,
  Optional[Boolean] $trigger_ignore_def    = undef,
  Optional[Boolean] $trigger_no_status     = undef,
  Optional[Boolean] $trigger_no_version    = undef,
  Optional[Boolean] $trigger_on_dist       = undef,
  Optional[Boolean] $trigger_on_mod        = undef,
  Optional[Boolean] $trigger_on_receive    = undef,
  Optional[Boolean] $trigger_onoff_time    = undef,
  $username              = undef
) {

  if $name =~ /^((?!((^|, )(^[A-Za-z0-9\-_]+$))+$).)*$/ {
    fail("Name [${name}] must contain only letters, numbers, underscores, or hyphens.")
  }

  if $runmode == undef {
    fail("Parameter 'runmode' must be specified.")
  }

  if $password == undef {
    fail("Parameter 'password' must be specified.")
  }
  if $username == undef {
    fail("Parameter 'username' must be specified.")
  }

  if $ensure == 'present' {
    if !$resource_type {
      fail("Parameter 'resource_type' must be specified.")
    }

    if !$serialize_type {
      fail("Parameter 'serialize_type' must be specified.")
    }

    if !$template {
      fail("Parameter 'template' must be specified.")
    }

    $_description = "**Managed by Puppet. Any changes made will be overwritten** ${description}"

  } else {
    $_description = undef
  }

  $password_properties = ['transportPassword', 'proxyPassword']

  $resource_props = {
    'jcr:primaryType'             => 'nt:unstructured',
    'userId'                      => $agent_user,
    'queueBatchMode'              => $batch_enabled,
    'queueBatchWaitTime'          => $batch_max_wait,
    'queueBatchMaxSize'           => $batch_trigger_size,
    'jcr:description'             => $_description,
    'enabled'                     => $enabled,
    'logLevel'                    => $log_level,
    'jcr:mixinTypes'              => $mixin_types,
    'protocolHTTPConnectionClose' => $protocol_close_conn,
    'protocolConnectTimeout'      => $protocol_conn_timeout,
    'protocolHTTPHeaders'         => $protocol_http_headers,
    'protocolHTTPMethod'          => $protocol_http_method,
    'protocolInterface'           => $protocol_interface,
    'protocolSocketTimeout'       => $protocol_sock_timeout,
    'protocolVersion'             => $protocol_version,
    'proxyHost'                   => $proxy_host,
    'proxyNTLMDomain'             => $proxy_ntlm_domain,
    'proxyNTLMHost'               => $proxy_ntlm_host,
    'proxyPassword'               => $proxy_password,
    'proxyPort'                   => $proxy_port,
    'proxyUser'                   => $proxy_user,
    'sling:resourceType'          => $resource_type,
    'retryDelay'                  => $retry_delay,
    'reverseReplication'          => $reverse,
    'serializationType'           => $serialize_type,
    'directory'                   => $static_directory,
    'definition'                  => $static_definition,
    'cq:template'                 => $template,
    'jcr:title'                   => $title,
    'protocolHTTPExpired'         => $trans_allow_exp_cert,
    'transportNTLMDomain'         => $trans_ntlm_domain,
    'transportNTLMHost'           => $trans_ntlm_host,
    'transportPassword'           => $trans_password,
    'ssl'                         => $trans_ssl,
    'transportUri'                => $trans_uri,
    'transportUser'               => $trans_user,
    'triggerSpecific'             => $trigger_ignore_def,
    'noStatusUpdate'              => $trigger_no_status,
    'noVersioning'                => $trigger_no_version,
    'triggerDistribute'           => $trigger_on_dist,
    'triggerModified'             => $trigger_on_mod,
    'triggerReceive'              => $trigger_on_receive,
    'triggerOnOffTime'            => $trigger_onoff_time,
  }

  $_resource_props = delete_undef_values($resource_props)

  $path = "/etc/replication/agents.${runmode}/${name}"

  aem_sling_resource { $title :
    ensure              => $ensure,
    force_passwords     => $force_passwords,
    handle_missing      => remove,
    home                => $home,
    password            => $password,
    password_properties => $password_properties,
    path                => $path,
    properties          => {
      'jcr:primaryType' => 'cq:Page',
      'jcr:content'     => $_resource_props,
    },
    timeout             => $timeout,
    username            => $username,
  }

}
