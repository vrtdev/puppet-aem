source ENV['GEM_SOURCE'] || 'https://rubygems.org'

facterversion = ENV.fetch('FACTER_GEM_VERSION', nil)
puppetversion = ENV.fetch('PUPPET_VERSION', nil)

group :development, :tests do
  gem 'benchmark',                  require: false
  gem 'codeclimate-test-reporter',  require: false
  gem 'ostruct',                    require: false
  gem 'metadata-json-lint',         require: false
  gem 'puppetlabs_spec_helper',     require: false
  gem 'rake',                       require: false
  gem 'rspec',                      require: false
  gem 'rspec-puppet',               require: false
  gem 'simplecov',                  require: false
  gem 'webmock',                    require: false
  gem 'syslog',                     require: false
  gem 'rainbow',                    require: false
end

group :linting do
  gem 'puppet-lint',                require: false
  gem 'rubocop',                    require: false
  gem 'rubocop-performance',        require: false
  gem 'rubocop-rspec',              require: false
  gem 'rubocop-rake',               require: false
end

group :system_tests do
  gem 'beaker', '<3.14.0',            require: false
  gem 'beaker-puppet_install_helper', require: false
  gem 'beaker-rspec',                 require: false
  gem 'serverspec',                   require: false
end

if facterversion
  gem 'facter', facterversion, require: false
else
  gem 'facter', require: false
end

if puppetversion
  gem 'puppet', puppetversion, require: false
else
  gem 'puppet', require: false
end

gem 'crx_packmgr_api_client', '~>1.2', require: false
gem 'xml-simple',             '~>1.1.5', require: false
