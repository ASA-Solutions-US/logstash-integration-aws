# eventbridge_spec.rb

require 'rspec'
require 'logstash/outputs/eventbridge'
require 'logstash/event'
require 'aws-sdk-eventbridge'

describe LogStash::Outputs::EventBridge do
  let(:event) { LogStash::Event.new }
  let(:eventbridge_client) { double('Aws::EventBridge::Client') }
  let(:plugin) { described_class.new }

  before do
    allow(Aws::EventBridge::Client).to receive(:new).and_return(eventbridge_client)
    allow(plugin).to receive(:@eventbridge).and_return(eventbridge_client)
    plugin.register
  end

  describe '#register' do
    it 'creates an EventBridge client' do
      expect(Aws::EventBridge::Client).to have_received(:new)
    end
  end

  describe '#receive' do
    context 'when eventbridge_detail is present' do
      it 'sends event to EventBridge with correct parameters' do
        event.set('eventbridge', 'my-event-bus')
        event.set('eventbridge_source', 'my-source')
        event.set('eventbridge_detail_type', 'my-detail-type')
        event.set('eventbridge_detail', '{"key": "value"}')

        expect(plugin).to receive(:send_eventbridge_event).with('my-event-bus', 'my-source', 'my-detail-type', '{"key": "value"}')
        plugin.receive(event)
      end
    end

    context 'when eventbridge_detail is absent' do
      it 'encodes and sends the whole event' do
        allow(plugin.codec).to receive(:encode).and_yield(event.to_json)

        expect(plugin).to receive(:send_eventbridge_event).with(nil, 'logstash', 'logstashEvent', event.to_json)
        plugin.receive(event)
      end
    end
  end

  describe '#send_eventbridge_event' do
    it 'raises an error if event_bus_name is nil' do
      expect { plugin.send(:send_eventbridge_event, nil, 'source', 'type', 'detail') }.to raise_error(ArgumentError)
    end

    it 'sends the event to EventBridge with the correct parameters' do
      allow(eventbridge_client).to receive(:put_events)
      plugin.send(:send_eventbridge_event, 'my-event-bus', 'my-source', 'my-detail-type', 'my-detail')

      expect(eventbridge_client).to have_received(:put_events).with(entries: [{
        event_bus_name: 'my-event-bus',
        source: 'my-source',
        detail_type: 'my-detail-type',
        detail: 'my-detail'
      }])
    end
  end
end
