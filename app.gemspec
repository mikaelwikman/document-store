# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.authors       = ['Mikael Wikman']
  gem.email         = ['mikael@wikman.me']
  gem.description   = %q{This wrapper provides a minimalistic interfaced to document-based databases. It includes a in-memory store that can be easily used for writing tests, as well as a in-memory cached version of each implementation.}
  gem.summary       = %q{A wrapper around document-based databases to provide a minimalistic interface that can be easily changed}
  gem.homepage      = "https://github.com/mikaelwikman/document-store"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|features)/})
  gem.name          = "document-store"
  gem.require_paths = ["lib"]
  gem.version       = '2.2.6'
  gem.add_dependency 'em-synchrony'
  gem.add_dependency 'mongo', '1.9.2'
end
