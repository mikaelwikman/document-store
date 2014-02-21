require 'bundler/setup'
require 'em-synchrony'
require 'test/unit'
require 'em-minitest'
require 'turn/autorun'
require 'shoulda'
require 'mocha'

#Turn.config.format = :dot

$LOAD_PATH << 'lib'

class TestCase < Test::Unit::TestCase
end

