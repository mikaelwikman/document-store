require 'bundler/setup'
require 'test/unit'
require 'turn/autorun'
require 'shoulda'
require 'mocha'

Turn.config.format = :dot

$LOAD_PATH << 'lib'

class TestCase < Test::Unit::TestCase
end


# Monkey patch MiniTest to use EM::Synchrony
if defined? MiniTest::Unit
  class MiniTest::Unit
    alias_method :run_alias, :run

    def run(args = [])
      result = nil
      EM.synchrony do
        result = run_alias args
        EM.stop
      end

      result
    end
  end
end

