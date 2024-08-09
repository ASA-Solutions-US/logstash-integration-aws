# timestream_spec.rb

require 'rspec'
require 'logstash/outputs/timestream'
require 'logstash/event'
require 'aws-sdk-timestreamwrite'

describe LogStash::Outputs::Timestream do
  let(:event) { LogStash::Event.new }
  let(:timestream_client) { double('Aws::TimestreamWrite::Client') }
  let(:plugin) { described_class.new("database_name" => "my-database", "table_name" => "my-table") }

  before do
    allow(Aws::TimestreamWrite::Client).to receive(:new).and_return(timestream_client)
    allow(plugin).to receive(:@timestream).and_return(timestream_client)
    plugin.register
  end

  describe '#register' do
    it 'creates a Timestream client' do
      expect(Aws::TimestreamWrite::Client).to have_received(:new)
    end
  end

  describe '#receive' do
    context 'when event has required fields' do
      it 'sends the event to Timestream with correct parameters' do
        event.set('measure_name', 'cpu_usage')
        event.set('measure_value', '75.5')
        event.set('measure_value_type', 'DOUBLE')
        event.set('timestamp', '1625247600000')

        allow(timestream_client).to receive(:write_records)
        
        plugin.receive(event)
        
        expect(timestream_client).to have_received(:write_records).with(
          database_name: 'my-database',
          table_name: 'my-table',
          records: [
            {
              dimensions: [],
              measure_name: 'cpu_usage',
              measure_value: '75.5',
              measure_value_type: 'DOUBLE',
              time: '1625247600000',
              time_unit: 'MILLISECONDS'
            }
          ]
        )
      end
    end

    context 'when required fields are missing' do
      it 'raises an error when database_name or table_name is missing' do
        invalid_plugin = described_class.new({})

        expect { invalid_plugin.register }.to raise_error(LogStash::ConfigurationError)
      end
    end
  end

  describe '#build_records' do
    it 'builds the correct records for Timestream' do
      event.set('measure_name', 'cpu_usage')
      event.set('measure_value', '75.5')
      event.set('measure_value_type', 'DOUBLE')
      event.set('timestamp', '1625247600000')

      records = plugin.send(:build_records, event)

      expect(records).to eq([
        {
          dimensions: [],
          measure_name: 'cpu_usage',
          measure_value: '75.5',
          measure_value_type: 'DOUBLE',
          time: '1625247600000',
          time_unit: 'MILLISECONDS'
        }
      ])
    end
  end
end
