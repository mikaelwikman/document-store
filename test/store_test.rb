require 'test_helper'
require 'store'
require 'memory_store'

class StoreTest < TestCase

  context 'store' do

    setup do
      time_values = ["time1", "time2"]
      mock_time = ->() {time_values.shift}

      @it = Store.new(mock_time)
      @it.backend = MemoryStore.new
    end

    should 'create/save record should update created_at/updated_at' do
      id = @it.save({ 'lal' => 'lol' })
      assert_equal({ 'lal' => 'lol', 'created_at' => "time1" }, @it.load(id))

      id2 = @it.save({ 'lal' => 'lol', 'id' => id})
      assert_equal id, id2

      data = @it.load(id)
      assert_equal "time2", data['updated_at']
    end
  end
end
