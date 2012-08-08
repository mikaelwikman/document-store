require 'test_helper'
require 'store'

class StoreTest < TestCase
  should 'storing a record should update created_at, updated_at' do
    time = "theTime"
    mock_time = ->() {time}

    s = Store.new(mock_time)

    id = s.save({ 'lal' => 'lol' })
    assert_equal({ 'lal' => 'lol', 'created_at' => time }, s.load(id))

    id2 = s.save({ 'lal' => 'lol', 'id' => id})
    assert_equal id, id2

    data = s.load(id)
    assert_equal time, data['updated_at']
  end
end
