# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.authors       = ['Mikael Wikman']
  gem.email         = ['mikael@wikman.me']
  gem.description   = %q{A data storage supporting facetting}
  gem.summary       = %q{ }
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|features)/})
  gem.name          = "sc-store"
  gem.require_paths = ["lib"]
  gem.version       = '0.2.0'
  gem.add_dependency 'em-synchrony'
  gem.add_dependency 'em-mongo', '0.4.3'
end
