
Puppet::Type.type(:aem_crx_package).provide :ruby, parent: Puppet::Provider do

  mk_resource_methods

  confine feature: :xmlsimple
  confine feature: :crx_packmgr_api_client

  def self.require_libs
    require 'crx_packmgr_api_client'
    require 'xmlsimple'
  end

  def initialize(resource = nil)
    super(resource)
    @property_flush = {}
    @stabilization_time = resource[:stabilization_time]
  end

  def upload
    @property_flush[:ensure] = :present
    Puppet.debug('aem_crx_package::ruby - Upload requested.')
  end

  def install
    @property_flush[:ensure] = :installed
    Puppet.debug('aem_crx_package::ruby - Install requested.')
  end

  def remove
    @property_flush[:ensure] = :absent
    Puppet.debug('aem_crx_package::ruby - Remove requested.')
  end

  def purge
    @property_flush[:ensure] = :purged
    Puppet.debug('aem_crx_package::ruby - Purge requested.')
  end

  def retrieve
    self.class.require_libs
    find_package
    Puppet.debug("aem_crx_package::ruby - Retrieve - Property Hash: #{@property_hash}")
    @property_hash[:ensure]
  end

  def flush
    return unless @property_flush[:ensure]
    need_sleep = false
    Puppet.debug('aem_crx_package::ruby - Flushing out to AEM.')
    self.class.require_libs
    case @property_flush[:ensure]
    when :purged
      if @property_hash[:ensure] == :installed
        need_sleep = true
        result = uninstall_package
        raise_on_failure(result)
      end
      result = remove_package
    when :absent
      result = remove_package
    when :present
      need_sleep = true
      result = @property_hash[:ensure] == :absent ? upload_package : uninstall_package
    when :installed
      need_sleep = true
      result = @property_hash[:ensure] == :absent ? upload_package(true) : install_package
    else
      raise(Puppet::ResourceError, "Unknown property flush value: #{@property_flush[:ensure]}")
    end
    raise_on_failure(result)
    if need_sleep
      Puppet.debug("Sleeping 10 seconds to wait for installation to kick in")
      sleep(10)
    end
    find_package
    @property_flush.clear
  end

  private

  def build_cfg(port = nil, context_root = nil)
    config = CrxPackageManager::Configuration.new
    config.configure do |c|
      c.username = @resource[:username]
      c.password = @resource[:password]
      c.timeout = @resource[:timeout]
      c.host = "localhost:#{port}" if port
      c.base_path = "#{context_root}#{c.base_path}" if context_root
    end
    config
  end

  def build_client

    return @client if @client

    port = nil
    context_root = nil

    File.foreach(File.join(@resource[:home], 'crx-quickstart', 'bin', 'start-env')) do |line|
      match = line.match(/^PORT=(\S+)/) || nil
      port = match.captures[0] if match

      match = line.match(/^CONTEXT_ROOT='(\S+)'/) || nil
      context_root = match.captures[0] if match
    end

    config = build_cfg(port, context_root)

    @client = CrxPackageManager::DefaultApi.new(CrxPackageManager::ApiClient.new(config))
    @client
  end

  def wait_for_osgi_installer_finished
    require 'uri'
    require 'json'
    require 'net/http'
    retries ||= @resource[:retries]
    retry_timeout = @resource[:retry_timeout]
    host = 'http://localhost:8778'
    path = '/jolokia/read/org.apache.sling.installer:type=Installer,name=*'
    uri = URI.parse(host + path)
    request = Net::HTTP::Get.new(uri)
    begin
      Puppet.debug("Starting wait_for_osgi_installer_finished for Aem_crx_package[#{@resource[:pkg]}]")
      response = Net::HTTP.start(uri.hostname, uri.port) do |http|
        http.request(request)
      end
      raise "wait_for_installer_finished Response '#{response.code}' is not a Net::HTTPSuccess" unless response.is_a?(Net::HTTPSuccess)
      data = JSON.parse(response.body)
      check_install_status data
    rescue Errno::EADDRNOTAVAIL, JSON::ParserError, RuntimeError => e
      Puppet.info("wait_for_installer_finished exception for Aem_crx_package[#{@resource[:pkg]}]: #{e.class} : #{e.message} :")
      will_retry = (retries -= 1) >= 0
      if will_retry
        Puppet.debug("Waiting #{retry_timeout} seconds before retrying installer state query")
        sleep retry_timeout
        Puppet.debug("Retrying installer state query; remaining retries: #{retries}")
        retry
      end
      raise
    end
  end

  def check_install_status(data)
    Puppet.debug("jolokia data: #{data}")
    unless data.nil? || data['value'].nil?
      realdata = data['value']['org.apache.sling.installer:name=Sling OSGi Installer,type=Installer']
    end
    Puppet.debug("install status: #{realdata}")
    if realdata.nil?
      @stabilization_time = @resource[:stabilization_time]
      raise "Failed to fetch OSGi installer status"
    elsif realdata['Active'] == true || realdata['ActiveResourceCount'] != 0
      @stabilization_time = @resource[:stabilization_time]
      raise "OSGi installer still active, ActiveResourceCount: #{realdata['ActiveResourceCount']}"
    elsif @stabilization_time > 0
      @stabilization_time -= @resource[:retry_timeout]
      raise "Subtracting retry timeout (#{@resource[:retry_timeout]}) from stabilization time: #{@stabilization_time} seconds remaining"
    end
  end

  def wait_for_bundles_active
    require 'uri'
    require 'json'
    require 'net/http'
    retries ||= @resource[:retries]
    retry_timeout = @resource[:retry_timeout]
    host = 'http://localhost:4502'
    path = '/system/console/bundles.json'
    uri = URI.parse(host + path)
    request = Net::HTTP::Get.new(uri)
    request.basic_auth(@resource[:username], @resource[:password])
    begin
      Puppet.debug("Starting wait_for_bundles_active for Aem_crx_package[#{@resource[:pkg]}]")
      response = Net::HTTP.start(uri.hostname, uri.port) do |http|
        http.request(request)
      end
      raise "wait_for_bundles_active Response '#{response.code}' is not a Net::HTTPSuccess" unless response.is_a?(Net::HTTPSuccess)
      data = JSON.parse(response.body)
      check_bundles_status data
    rescue Errno::EADDRNOTAVAIL, JSON::ParserError, RuntimeError => e
      Puppet.info("wait_for_bundles_active exception for Aem_crx_package[#{@resource[:pkg]}]: #{e.class} : #{e.message} :")
      will_retry = (retries -= 1) >= 0
      if will_retry
        Puppet.debug("Waiting #{retry_timeout} seconds before retrying bundles state query")
        sleep retry_timeout
        Puppet.debug("Retrying bundles state query; remaining retries: #{retries}")
        retry
      end
      raise
    end
  end

  def check_bundles_status(data)
    realdata = data['s']
    Puppet.debug("bundle status: #{realdata}")
    if realdata[3] != 0 || realdata[4] != 0
      raise "OSGi bundles not all active #{realdata[3]} (req: 0) #{realdata[4]} (req: 0)"
    else
      Puppet.debug("All OSGi bundles active")
    end
  end

  def find_package
    Puppet.debug("Starting find_package for Aem_crx_package[#{@resource[:pkg]}]")

    wait_for_osgi_installer_finished
    wait_for_bundles_active
    client = build_client

    path = "/etc/packages/#{@resource[:group]}/#{@resource[:pkg]}-.zip"
    begin
      retries ||= @resource[:retries]
      retry_timeout = @resource[:retry_timeout]
      data = client.list(path: path, include_versions: true)
    rescue CrxPackageManager::ApiError => e
      Puppet.info("Unable to find package for Aem_crx_package[#{@resource[:pkg]}]: #{e}")
      will_retry = (retries -= 1) >= 0
      if will_retry
        Puppet.debug("Waiting #{retry_timeout} seconds before retrying package lookup")
        sleep retry_timeout
        Puppet.debug("Retrying package lookup; remaining retries: #{retries}")
        retry
      end
      raise
    end

    found_pkg = find_version(data.results)
    Puppet.debug("aem_crx_package::ruby - Found package: #{found_pkg}")
    if found_pkg
      @property_hash[:pkg] = found_pkg.name
      @property_hash[:group] = found_pkg.group
      @property_hash[:version] = found_pkg.version
      @property_hash[:ensure] = found_pkg.last_unpacked ? :installed : :present
    else
      @property_hash[:ensure] = :absent
    end
  end

  def find_version(ary)
    found_pkg = nil
    ary && ary.each do |p|
      found_pkg = p if p.version == @resource[:version]
      break if found_pkg
    end
    found_pkg
  end

  def upload_package(install = false)
    client = build_client
    file = File.new(@resource[:source])
    Puppet.debug("Starting upload_package for Aem_crx_package[#{@resource[:pkg]}] install: #{install}")
    client.service_post(file, install: install)
  end

  def install_package
    client = build_client
    Puppet.debug("Starting install_package for Aem_crx_package[#{@resource[:pkg]}]")
    client.service_exec('install', @resource[:pkg], @resource[:group], @resource[:version])
  end

  def uninstall_package
    client = build_client
    client.service_exec('uninstall', @resource[:pkg], @resource[:group], @resource[:version])
  end

  def remove_package
    client = build_client
    client.service_exec('delete', @resource[:pkg], @resource[:group], @resource[:version])
  end

  def raise_on_failure(api_response)
    if api_response.is_a?(CrxPackageManager::ServiceExecResponse)
      raise(api_response.msg) unless api_response.success
    else
      hash = XmlSimple.xml_in(api_response, ForceArray: false, KeyToSymbol: true, AttrToSymbol: true)
      response = CrxPackageManager::ServiceResponse.new
      response.build_from_hash(hash)
      raise(response.response.status[:content]) unless response.response.status[:code].to_i == 200
    end
  end
end
